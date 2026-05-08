package proxy

import (
	"context"
	"encoding/base64"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type countingListener struct {
	net.Listener
	accepted atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.accepted.Add(1)
	return c, nil
}

func newCountingProxyServer(b testing.TB) (*http.Server, *countingListener, string) {
	b.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}

	cl := &countingListener{Listener: ln}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "ok")
	})}

	go func() {
		_ = srv.Serve(cl)
	}()

	return srv, cl, cl.Addr().String()
}

func benchmarkGETRequestURL(b testing.TB) *url.URL {
	b.Helper()
	u, err := url.Parse("http://example.test/resource?id=1")
	if err != nil {
		b.Fatalf("parse url: %v", err)
	}
	return u
}

func benchmarkUpstreamConfig(b testing.TB, addr string) UpstreamConfig {
	b.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		b.Fatalf("split host/port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		b.Fatalf("parse port: %v", err)
	}

	return UpstreamConfig{
		Host:        host,
		Port:        port,
		User:        "u1",
		Pass:        "p1",
		DialTimeout: 3 * time.Second,
	}
}

func BenchmarkDoGETViaUpstreamProxy_RawConnection(b *testing.B) {
	resetUpstreamConnPool()
	srv, cl, addr := newCountingProxyServer(b)
	b.Cleanup(func() {
		_ = srv.Close()
	})

	urlValue := benchmarkGETRequestURL(b)
	cfg := benchmarkUpstreamConfig(b, addr)
	pool := NewUpstreamGETPool(cfg.Host, cfg.DialTimeout, []Credential{{User: cfg.User, Pass: cfg.Pass}}, cfg.Port, cfg.Port, time.Second, time.Minute)
	waitForWarmPool(b, pool)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := &http.Request{Header: make(http.Header), URL: urlValue}
		resp, _, _, err := pool.Do(context.Background(), req)
		if err != nil {
			b.Fatalf("request failed: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	accepted := cl.accepted.Load()
	b.ReportMetric(float64(accepted)/float64(b.N), "accepted_conn/op")
}

func BenchmarkHandlerServeHTTP_GET(b *testing.B) {
	resetUpstreamConnPool()
	srv, cl, addr := newCountingProxyServer(b)
	b.Cleanup(func() {
		_ = srv.Close()
	})

	prevLogWriter := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() {
		if prevLogWriter == nil {
			log.SetOutput(os.Stderr)
			return
		}
		log.SetOutput(prevLogWriter)
	})

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		b.Fatalf("split host/port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		b.Fatalf("parse port: %v", err)
	}

	getPool := NewUpstreamGETPool(host, 3*time.Second, []Credential{{User: "u1", Pass: "p1"}}, port, port, time.Second, time.Minute)
	connectPool := NewUpstreamCONNECTPool(host, 3*time.Second, []Credential{{User: "u1", Pass: "p1"}}, port, port, 45*time.Second, time.Second, time.Minute)
	h := NewHandler(HandlerConfig{
		MaxRetries403: 0,
		Timeout:       5 * time.Second,
		StickyPortTTL: 45 * time.Second,
		ServiceUser:   "svc",
		ServicePass:   "svc-pass",
		GETPool:       getPool,
		CONNECTPool:   connectPool,
		Creds:         NewCredentialProvider([]Credential{{User: "u1", Pass: "p1"}}),
	})
	waitForWarmHandler(b, h)

	auth := "Basic " + base64.StdEncoding.EncodeToString([]byte("svc:svc-pass"))
	targetURL := "http://example.test/resource?id=1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, targetURL, nil)
		req.Header.Set("Proxy-Authorization", auth)
		rr := httptest.NewRecorder()

		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			b.Fatalf("unexpected status: got=%d body=%q", rr.Code, rr.Body.String())
		}
	}

	accepted := cl.accepted.Load()
	b.ReportMetric(float64(accepted)/float64(b.N), "accepted_conn/op")
}
