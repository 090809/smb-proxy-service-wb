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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fixedPortPicker struct {
	port int
}

func (p fixedPortPicker) Pick(_ map[int]bool) int {
	return p.port
}

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
		Host:                    host,
		Port:                    port,
		User:                    "u1",
		Pass:                    "p1",
		DialTimeout:             3 * time.Second,
		KeepAliveIdleTimeout:    60 * time.Second,
		KeepAliveMaxIdleConns:   1024,
		KeepAliveMaxIdlePerHost: 128,
	}
}

func resetUpstreamClientPool() {
	upstreamHTTPClientPool = sync.Map{}
}

func BenchmarkDoGETViaUpstreamProxy_KeepAlivePool(b *testing.B) {
	resetUpstreamClientPool()
	srv, cl, addr := newCountingProxyServer(b)
	b.Cleanup(func() {
		_ = srv.Close()
	})

	urlValue := benchmarkGETRequestURL(b)
	cfg := benchmarkUpstreamConfig(b, addr)

	// Warm up one request to build transport/client and establish the first connection.
	warmReq := &http.Request{Header: make(http.Header), URL: urlValue}
	warmResp, err := DoGETViaUpstreamProxy(context.Background(), cfg, warmReq)
	if err != nil {
		b.Fatalf("warmup request failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, warmResp.Body)
	_ = warmResp.Body.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := &http.Request{Header: make(http.Header), URL: urlValue}
		resp, err := DoGETViaUpstreamProxy(context.Background(), cfg, req)
		if err != nil {
			b.Fatalf("request failed: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	accepted := cl.accepted.Load()
	b.ReportMetric(float64(accepted)/float64(b.N), "accepted_conn/op")
}

func BenchmarkDoGETViaUpstreamProxy_NoPoolBaseline(b *testing.B) {
	resetUpstreamClientPool()
	srv, cl, addr := newCountingProxyServer(b)
	b.Cleanup(func() {
		_ = srv.Close()
	})

	urlValue := benchmarkGETRequestURL(b)
	cfg := benchmarkUpstreamConfig(b, addr)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Emulates old behavior (new transport/client per request) by clearing the global pool.
		resetUpstreamClientPool()
		req := &http.Request{Header: make(http.Header), URL: urlValue}
		resp, err := DoGETViaUpstreamProxy(context.Background(), cfg, req)
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
	resetUpstreamClientPool()
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

	h := NewHandler(HandlerConfig{
		UpstreamHost:             host,
		Picker:                   fixedPortPicker{port: port},
		MaxRetries403:            0,
		Timeout:                  5 * time.Second,
		DialTimeout:              3 * time.Second,
		KeepAliveIdleTimeout:     60 * time.Second,
		KeepAliveMaxIdleConns:    1024,
		KeepAliveMaxIdlePerHost:  128,
		StickyPortTTL:            45 * time.Second,
		ServiceUser:              "svc",
		ServicePass:              "svc-pass",
		Creds:                    NewCredentialProvider([]Credential{{User: "u1", Pass: "p1"}}),
	})

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
