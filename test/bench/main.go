package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type latencyStats struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (s *latencyStats) Add(d time.Duration) {
	s.mu.Lock()
	s.samples = append(s.samples, d)
	s.mu.Unlock()
}

func (s *latencyStats) Snapshot() (time.Duration, time.Duration, time.Duration, bool) {
	s.mu.Lock()
	if len(s.samples) == 0 {
		s.mu.Unlock()
		return 0, 0, 0, false
	}
	samples := append([]time.Duration(nil), s.samples...)
	s.mu.Unlock()

	slices.Sort(samples)
	return percentile(samples, 50), percentile(samples, 95), percentile(samples, 99), true
}

func percentile(samples []time.Duration, p int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	if p <= 0 {
		return samples[0]
	}
	if p >= 100 {
		return samples[len(samples)-1]
	}
	idx := (len(samples)*p + 99) / 100
	if idx <= 0 {
		idx = 1
	}
	return samples[idx-1]
}

func main() {
	var (
		target   string
		proxyRaw string
		workers  int
		timeout  time.Duration
	)

	flag.StringVar(&target, "target", "https://ifconfig.me", "request target URL")
	flag.StringVar(&proxyRaw, "proxy", "http://test:test@127.0.0.1:3128", "proxy URL")
	flag.IntVar(&workers, "workers", 10, "number of concurrent workers")
	flag.DurationVar(&timeout, "timeout", 15*time.Second, "per-request timeout")
	flag.Parse()

	if workers <= 0 {
		log.Fatal("workers must be > 0")
	}

	proxyURL, err := url.Parse(proxyRaw)
	if err != nil {
		log.Fatalf("parse proxy URL: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var total atomic.Uint64
	var failures atomic.Uint64
	stats := &latencyStats{}

	var wg sync.WaitGroup
	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
				if err != nil {
					log.Printf("build request: %v", err)
					failures.Add(1)
					return
				}

				client := newBenchHTTPClient(proxyURL, timeout)
				startedAt := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					failures.Add(1)
					log.Printf("request failed: %v", err)
					continue
				}

				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()

				if resp.StatusCode < 200 || resp.StatusCode >= 400 {
					failures.Add(1)
					log.Printf("unexpected status: %d", resp.StatusCode)
					continue
				}

				total.Add(1)
				stats.Add(time.Since(startedAt))
			}
		}()
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	log.Printf("starting bench helper: workers=%d proxy=%s target=%s", workers, proxyURL.Redacted(), target)

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			logBenchStats("stopped", total.Load(), failures.Load(), stats)
			return
		case <-ticker.C:
			logBenchStats("progress", total.Load(), failures.Load(), stats)
		}
	}
}

func logBenchStats(prefix string, okCount uint64, failCount uint64, stats *latencyStats) {
	median, p95, p99, hasSamples := stats.Snapshot()
	if !hasSamples {
		log.Printf("%s: ok=%d failed=%d median=n/a p95=n/a p99=n/a", prefix, okCount, failCount)
		return
	}
	log.Printf("%s: ok=%d failed=%d median=%s p95=%s p99=%s", prefix, okCount, failCount, median, p95, p99)
}

func newBenchHTTPClient(proxyURL *url.URL, timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy:             http.ProxyURL(proxyURL),
			ForceAttemptHTTP2: true,
		},
		Timeout: timeout,
	}
}
