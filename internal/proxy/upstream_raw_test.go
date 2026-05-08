package proxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func proxyServerConfig(t *testing.T, serverURL string) UpstreamConfig {
	t.Helper()

	host, portStr, err := net.SplitHostPort(serverURL[len("http://"):])
	if err != nil {
		t.Fatalf("split host/port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}

	return UpstreamConfig{
		Host:        host,
		Port:        port,
		User:        "upstream-user",
		Pass:        "upstream-pass",
		DialTimeout: 3 * time.Second,
	}
}

func waitForWarmPool(tb testing.TB, pool *UpstreamGETPool) {
	waitForWarmPoolCount(tb, pool, 1)
}

func waitForWarmPoolCount(tb testing.TB, pool *UpstreamGETPool, want int) {
	tb.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ready := 0
		for _, channel := range pool.channels {
			ready += len(channel.ready)
		}
		if ready >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Fatal("timed out waiting for warmed upstream pool")
}

func waitForWarmHandler(tb testing.TB, h *Handler) {
	tb.Helper()
	waitForWarmPool(tb, h.getPool)
}

func waitForWarmHandlerCount(tb testing.TB, h *Handler, want int) {
	tb.Helper()
	waitForWarmPoolCount(tb, h.getPool, want)
}

func newSingleChannelPool(cfg UpstreamConfig) *UpstreamGETPool {
	return NewUpstreamGETPool(cfg.Host, cfg.DialTimeout, []Credential{{User: cfg.User, Pass: cfg.Pass}}, cfg.Port, cfg.Port, time.Second, time.Minute)
}

func newRawGETKeepAliveUpstream(tb testing.TB, onRequest func(remoteAddr string)) UpstreamConfig {
	tb.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	tb.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()

				br := bufio.NewReader(conn)
				for {
					req, err := http.ReadRequest(br)
					if err != nil {
						return
					}
					_ = req.Body.Close()
					onRequest(conn.RemoteAddr().String())
					if _, err := io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok"); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	host, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		tb.Fatalf("split host/port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		tb.Fatalf("parse port: %v", err)
	}
	return UpstreamConfig{
		Host:        host,
		Port:        port,
		User:        "upstream-user",
		Pass:        "upstream-pass",
		DialTimeout: 3 * time.Second,
	}
}

func TestUpstreamGETPool_RawConnection(t *testing.T) {
	resetUpstreamConnPool()

	var gotRequestURI string
	var gotXTest string
	var gotProxyAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRequestURI = r.RequestURI
		gotXTest = r.Header.Get("X-Test")
		gotProxyAuth = r.Header.Get("Proxy-Authorization")

		w.Header().Set("X-Upstream", "yes")
		w.Header().Set("Proxy-Authenticate", `Basic realm="upstream"`)
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()

	req := httptest.NewRequest(http.MethodGet, "http://example.test/resource?id=1", nil)
	req.Header.Set("X-Test", "1")
	req.Header.Set("Proxy-Authorization", basicProxyAuthHeader("client-user", "client-pass"))

	pool := newSingleChannelPool(proxyServerConfig(t, srv.URL))
	waitForWarmPool(t, pool)

	resp, _, _, err := pool.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("pool.Do: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if gotRequestURI != "http://example.test/resource?id=1" {
		t.Fatalf("unexpected request URI: %q", gotRequestURI)
	}
	if gotXTest != "1" {
		t.Fatalf("unexpected X-Test header: %q", gotXTest)
	}
	if gotProxyAuth != basicProxyAuthHeader("upstream-user", "upstream-pass") {
		t.Fatalf("unexpected Proxy-Authorization header: %q", gotProxyAuth)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Upstream") != "yes" {
		t.Fatalf("unexpected upstream header: %q", resp.Header.Get("X-Upstream"))
	}
	if string(body) != "ok" {
		t.Fatalf("unexpected body: %q", string(body))
	}
}

func TestHandlerServeHTTP_GET_RawResponseHeaders(t *testing.T) {
	resetUpstreamConnPool()

	var gotProxyAuth string
	var gotXTest string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotProxyAuth = r.Header.Get("Proxy-Authorization")
		gotXTest = r.Header.Get("X-Test")

		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Proxy-Authenticate", `Basic realm="upstream"`)
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "payload")
	}))
	defer srv.Close()

	cfg := proxyServerConfig(t, srv.URL)
	getPool := NewUpstreamGETPool(cfg.Host, cfg.DialTimeout, []Credential{{User: cfg.User, Pass: cfg.Pass}}, cfg.Port, cfg.Port, time.Second, time.Minute)
	connectPool := NewUpstreamCONNECTPool(cfg.Host, cfg.DialTimeout, []Credential{{User: cfg.User, Pass: cfg.Pass}}, cfg.Port, cfg.Port, 45*time.Second, time.Second, time.Minute)
	h := NewHandler(HandlerConfig{
		MaxRetries403: 0,
		Timeout:       5 * time.Second,
		ServiceUser:   "svc",
		ServicePass:   "svc-pass",
		GETPool:       getPool,
		CONNECTPool:   connectPool,
		Creds:         NewCredentialProvider([]Credential{{User: cfg.User, Pass: cfg.Pass}}),
	})
	waitForWarmHandler(t, h)

	req := httptest.NewRequest(http.MethodGet, "http://example.test/resource?id=1", nil)
	req.Header.Set("Proxy-Authorization", basicProxyAuthHeader("svc", "svc-pass"))
	req.Header.Set("X-Test", "1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("unexpected status: %d", rr.Code)
	}
	if gotProxyAuth != basicProxyAuthHeader("upstream-user", "upstream-pass") {
		t.Fatalf("unexpected upstream proxy auth: %q", gotProxyAuth)
	}
	if gotXTest != "1" {
		t.Fatalf("unexpected forwarded X-Test header: %q", gotXTest)
	}
	if rr.Header().Get("X-Upstream") != "yes" {
		t.Fatalf("unexpected X-Upstream header: %q", rr.Header().Get("X-Upstream"))
	}
	if rr.Header().Get("Proxy-Authenticate") != "" {
		t.Fatalf("Proxy-Authenticate header must be stripped, got %q", rr.Header().Get("Proxy-Authenticate"))
	}
	if rr.Header().Get("Connection") != "" {
		t.Fatalf("Connection header must be stripped, got %q", rr.Header().Get("Connection"))
	}
	if rr.Header().Get("X-Proxy-Channel") != "1" {
		t.Fatalf("unexpected X-Proxy-Channel header: %q", rr.Header().Get("X-Proxy-Channel"))
	}
	if rr.Body.String() != "payload" {
		t.Fatalf("unexpected body: %q", rr.Body.String())
	}
}

func TestDoGETViaUpstreamConn_ReusesConnectionAfterTenRequests(t *testing.T) {
	resetUpstreamConnPool()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	go func() {
		defer serverConn.Close()

		br := bufio.NewReader(serverConn)
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		_ = req.Body.Close()
		_, _ = io.WriteString(serverConn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok")
	}()

	pool := &UpstreamGETPool{
		badPorts:    newBadPortState(time.Second, time.Minute),
		readySignal: make(chan struct{}, 1),
	}
	channel := &upstreamGETChannel{
		pool:        pool,
		ready:       make(chan *pooledUpstreamConn, 1),
		refill:      make(chan struct{}, 1),
		activePorts: map[int]int{10001: 1},
	}
	pc := &pooledUpstreamConn{
		cfg:             UpstreamConfig{Host: "example.test", Port: 10001},
		proxyAuthHeader: basicProxyAuthHeader("upstream-user", "upstream-pass"),
		conn:            clientConn,
		reader:          bufio.NewReader(clientConn),
		channel:         channel,
		requests:        10,
	}

	req := httptest.NewRequest(http.MethodGet, "http://example.test/reuse", nil)
	resp, err := doGETViaUpstreamConn(context.Background(), pc, req)
	if err != nil {
		t.Fatalf("doGETViaUpstreamConn: %v", err)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("unexpected body: %q", string(body))
	}
	if pc.requests != 11 {
		t.Fatalf("unexpected request count: %d", pc.requests)
	}

	select {
	case reused := <-channel.ready:
		if reused != pc {
			t.Fatalf("expected same pooled connection to be returned to ready queue")
		}
	default:
		t.Fatal("expected connection to be returned to ready queue after 11th request")
	}
}

func TestUpstreamGETPool_CoolsDownBucketOn503(t *testing.T) {
	resetUpstreamConnPool()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "busy")
	}))
	defer srv.Close()

	cfg := proxyServerConfig(t, srv.URL)
	req := httptest.NewRequest(http.MethodGet, "http://example.test/busy", nil)
	pool := newSingleChannelPool(cfg)
	waitForWarmPool(t, pool)

	resp, _, _, err := pool.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	waitCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, _, _, err = pool.Do(waitCtx, req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout while waiting for warmed connection during cooldown, got %v", err)
	}
}

func TestHandlerServeHTTP_GET_RetriesOn503(t *testing.T) {
	resetUpstreamConnPool()

	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "busy")
	}))
	defer badSrv.Close()

	goodSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "retry-ok")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	defer goodSrv.Close()

	badCfg := proxyServerConfig(t, badSrv.URL)
	goodCfg := proxyServerConfig(t, goodSrv.URL)
	if badCfg.Host != goodCfg.Host {
		t.Fatalf("expected same host for retry test, got %q and %q", badCfg.Host, goodCfg.Host)
	}

	getPool := NewUpstreamGETPool(badCfg.Host, 3*time.Second, []Credential{{User: badCfg.User, Pass: badCfg.Pass}}, min(badCfg.Port, goodCfg.Port), max(badCfg.Port, goodCfg.Port), time.Second, time.Minute)
	connectPool := NewUpstreamCONNECTPool(badCfg.Host, 3*time.Second, []Credential{{User: badCfg.User, Pass: badCfg.Pass}}, min(badCfg.Port, goodCfg.Port), max(badCfg.Port, goodCfg.Port), 45*time.Second, time.Second, time.Minute)
	h := NewHandler(HandlerConfig{
		MaxRetries403: 1,
		Timeout:       5 * time.Second,
		ServiceUser:   "svc",
		ServicePass:   "svc-pass",
		GETPool:       getPool,
		CONNECTPool:   connectPool,
		Creds:         NewCredentialProvider([]Credential{{User: badCfg.User, Pass: badCfg.Pass}}),
	})
	h.getPool.selector.next.Store(uint64(badCfg.Port - h.getPool.selector.min))
	waitForWarmHandlerCount(t, h, 2)

	req := httptest.NewRequest(http.MethodGet, "http://example.test/retry", nil)
	req.Header.Set("Proxy-Authorization", basicProxyAuthHeader("svc", "svc-pass"))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%q", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("X-Upstream") != "retry-ok" {
		t.Fatalf("unexpected X-Upstream header: %q", rr.Header().Get("X-Upstream"))
	}
	if rr.Body.String() != "ok" {
		t.Fatalf("unexpected body: %q", rr.Body.String())
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
