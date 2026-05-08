package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type rawConnectUpstream struct {
	tb    testing.TB
	ln    net.Listener
	delay time.Duration

	mu           sync.Mutex
	connectCount int
	lastTarget   string
	lastAuth     string
}

func newRawConnectUpstream(tb testing.TB) *rawConnectUpstream {
	return newRawConnectUpstreamWithDelay(tb, 0)
}

func newRawConnectUpstreamWithDelay(tb testing.TB, delay time.Duration) *rawConnectUpstream {
	tb.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}

	s := &rawConnectUpstream{tb: tb, ln: ln, delay: delay}
	go s.serve()
	tb.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *rawConnectUpstream) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *rawConnectUpstream) handleConn(conn net.Conn) {
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	br := bufio.NewReader(conn)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	defer req.Body.Close()

	if req.Method != http.MethodConnect {
		return
	}

	s.mu.Lock()
	s.connectCount++
	s.lastTarget = req.Host
	s.lastAuth = req.Header.Get("Proxy-Authorization")
	s.mu.Unlock()

	if s.delay > 0 {
		time.Sleep(s.delay)
	}

	if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	_, _ = io.Copy(conn, io.MultiReader(br, conn))
}

func (s *rawConnectUpstream) config() UpstreamConfig {
	host, portStr, err := net.SplitHostPort(s.ln.Addr().String())
	if err != nil {
		s.tb.Fatalf("split host/port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		s.tb.Fatalf("parse port: %v", err)
	}
	return UpstreamConfig{
		Host:        host,
		Port:        port,
		User:        "upstream-user",
		Pass:        "upstream-pass",
		DialTimeout: 3 * time.Second,
	}
}

func (s *rawConnectUpstream) snapshot() (int, string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connectCount, s.lastTarget, s.lastAuth
}

func waitForConnectCount(tb testing.TB, srv *rawConnectUpstream, want int) {
	tb.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		count, _, _ := srv.snapshot()
		if count >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	count, _, _ := srv.snapshot()
	tb.Fatalf("timed out waiting for connect count %d, got %d", want, count)
}

func TestUpstreamCONNECTPool_PrewarmsTargetTunnel(t *testing.T) {
	upstream := newRawConnectUpstream(t)
	cfg := upstream.config()

	pool := NewUpstreamCONNECTPool(cfg.Host, cfg.DialTimeout, []Credential{{User: cfg.User, Pass: cfg.Pass}}, cfg.Port, cfg.Port, 45*time.Second, time.Second, time.Minute)

	warmCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.WaitUntilReady(warmCtx, "example.com:443", 1); err != nil {
		t.Fatalf("wait for ready tunnel: %v", err)
	}

	count, target, auth := upstream.snapshot()
	if count != 1 {
		t.Fatalf("expected 1 prewarmed CONNECT, got %d", count)
	}
	if target != "example.com:443" {
		t.Fatalf("unexpected CONNECT target: %q", target)
	}
	if auth != basicProxyAuthHeader(cfg.User, cfg.Pass) {
		t.Fatalf("unexpected upstream auth: %q", auth)
	}

	pt, err := pool.Acquire(context.Background(), "example.com:443")
	if err != nil {
		t.Fatalf("acquire tunnel: %v", err)
	}

	if _, err := io.WriteString(pt.conn, "ping"); err != nil {
		t.Fatalf("write tunneled bytes: %v", err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(pt.conn, reply); err != nil {
		t.Fatalf("read tunneled bytes: %v", err)
	}
	if string(reply) != "ping" {
		t.Fatalf("unexpected tunneled reply: %q", string(reply))
	}

	pt.channel.releaseConsumed(pt)
	waitForConnectCount(t, upstream, 2)
}

func TestUpstreamCONNECTPool_PrewarmTargets(t *testing.T) {
	upstream := newRawConnectUpstream(t)
	cfg := upstream.config()

	pool := NewUpstreamCONNECTPool(cfg.Host, cfg.DialTimeout, []Credential{{User: cfg.User, Pass: cfg.Pass}}, cfg.Port, cfg.Port, 45*time.Second, time.Second, time.Minute)

	warmCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.PrewarmTargets(warmCtx, []string{"example.com", "example.com:443", "api.example.com:8443"}); err != nil {
		t.Fatalf("prewarm targets: %v", err)
	}

	count, _, _ := upstream.snapshot()
	if count != 2 {
		t.Fatalf("expected 2 distinct prewarmed CONNECT targets, got %d", count)
	}

	targets := []string{"example.com:443", "api.example.com:8443"}
	for _, target := range targets {
		acquireCtx, acquireCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		pt, err := pool.Acquire(acquireCtx, target)
		acquireCancel()
		if err != nil {
			t.Fatalf("acquire prewarmed target %s: %v", target, err)
		}
		pt.channel.releaseConsumed(pt)
	}
}

func TestHandlerServeHTTP_CONNECT_UsesPrewarmedTunnel(t *testing.T) {
	upstream := newRawConnectUpstream(t)
	cfg := upstream.config()

	getUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer getUpstream.Close()

	getCfg := proxyServerConfig(t, getUpstream.URL)
	getPool := NewUpstreamGETPool(getCfg.Host, getCfg.DialTimeout, []Credential{{User: getCfg.User, Pass: getCfg.Pass}}, getCfg.Port, getCfg.Port, time.Second, time.Minute)
	connectPool := NewUpstreamCONNECTPool(cfg.Host, cfg.DialTimeout, []Credential{{User: cfg.User, Pass: cfg.Pass}}, cfg.Port, cfg.Port, 45*time.Second, time.Second, time.Minute)

	warmCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := connectPool.WaitUntilReady(warmCtx, "example.com:443", 1); err != nil {
		t.Fatalf("wait for ready tunnel: %v", err)
	}

	h := NewHandler(HandlerConfig{
		MaxRetries403: 0,
		Timeout:       5 * time.Second,
		StickyPortTTL: 45 * time.Second,
		ServiceUser:   "svc",
		ServicePass:   "svc-pass",
		GETPool:       getPool,
		CONNECTPool:   connectPool,
		Creds:         NewCredentialProvider([]Credential{{User: cfg.User, Pass: cfg.Pass}}),
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen local proxy: %v", err)
	}
	defer ln.Close()

	srv := &http.Server{Handler: h}
	defer srv.Close()
	go func() { _ = srv.Serve(ln) }()

	beforeCount, _, _ := upstream.snapshot()
	if beforeCount != 1 {
		t.Fatalf("expected tunnel to be prewarmed before client connect, got %d", beforeCount)
	}

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial local proxy: %v", err)
	}
	defer clientConn.Close()

	auth := basicProxyAuthHeader("svc", "svc-pass")
	if _, err := fmt.Fprintf(clientConn, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Authorization: %s\r\n\r\n", auth); err != nil {
		t.Fatalf("write CONNECT request: %v", err)
	}

	br := bufio.NewReader(clientConn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatalf("read CONNECT response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected CONNECT status: %d body=%q", resp.StatusCode, string(body))
	}
	if strings.TrimSpace(resp.Header.Get("X-Proxy-Channel")) != "1" {
		t.Fatalf("unexpected X-Proxy-Channel: %q", resp.Header.Get("X-Proxy-Channel"))
	}

	if _, err := io.WriteString(clientConn, "ping"); err != nil {
		t.Fatalf("write tunneled bytes: %v", err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(br, reply); err != nil {
		t.Fatalf("read tunneled bytes: %v", err)
	}
	if string(reply) != "ping" {
		t.Fatalf("unexpected tunneled reply: %q", string(reply))
	}

	waitForConnectCount(t, upstream, 2)
}

func TestUpstreamCONNECTPool_PenalizesSlowWarmupPort(t *testing.T) {
	slowUpstream := newRawConnectUpstreamWithDelay(t, upstreamSlowConnectThreshold+150*time.Millisecond)
	fastUpstream := newRawConnectUpstream(t)

	slowCfg := slowUpstream.config()
	fastCfg := fastUpstream.config()
	if slowCfg.Host != fastCfg.Host {
		t.Fatalf("expected same host for retry test, got %q and %q", slowCfg.Host, fastCfg.Host)
	}

	pool := NewUpstreamCONNECTPool(slowCfg.Host, 3*time.Second, []Credential{{User: slowCfg.User, Pass: slowCfg.Pass}}, min(slowCfg.Port, fastCfg.Port), max(slowCfg.Port, fastCfg.Port), 45*time.Second, time.Second, time.Minute)
	pool.selector.next.Store(uint64(slowCfg.Port - pool.selector.min))

	warmCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := pool.WaitUntilReady(warmCtx, "example.com:443", 1); err != nil {
		t.Fatalf("wait for ready tunnel: %v", err)
	}

	if !pool.badPorts.IsPenalized(slowCfg.Port) {
		t.Fatalf("expected slow port %d to be penalized", slowCfg.Port)
	}

	pt, err := pool.Acquire(context.Background(), "example.com:443")
	if err != nil {
		t.Fatalf("acquire tunnel: %v", err)
	}
	defer pt.channel.releaseConsumed(pt)

	if pt.cfg.Port != fastCfg.Port {
		t.Fatalf("expected fast port %d, got %d", fastCfg.Port, pt.cfg.Port)
	}

	slowCount, _, _ := slowUpstream.snapshot()
	fastCount, _, _ := fastUpstream.snapshot()
	if slowCount == 0 {
		t.Fatalf("expected slow upstream to be attempted at least once")
	}
	if fastCount == 0 {
		t.Fatalf("expected fast upstream to be used after slow penalty")
	}
}

func TestUpstreamCONNECTPool_PrefersFastKnownPort(t *testing.T) {
	slowUpstream := newRawConnectUpstreamWithDelay(t, 200*time.Millisecond)
	fastUpstream := newRawConnectUpstream(t)

	slowCfg := slowUpstream.config()
	fastCfg := fastUpstream.config()
	if slowCfg.Host != fastCfg.Host {
		t.Fatalf("expected same host for preference test, got %q and %q", slowCfg.Host, fastCfg.Host)
	}

	creds := []Credential{
		{User: slowCfg.User, Pass: slowCfg.Pass},
		{User: slowCfg.User, Pass: slowCfg.Pass},
	}
	pool := NewUpstreamCONNECTPool(slowCfg.Host, 3*time.Second, creds, min(slowCfg.Port, fastCfg.Port), max(slowCfg.Port, fastCfg.Port), 45*time.Second, time.Second, time.Minute)
	pool.selector.next.Store(uint64(slowCfg.Port - pool.selector.min))

	warmCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := pool.WaitUntilReady(warmCtx, "example.com:443", 2); err != nil {
		t.Fatalf("wait for ready tunnels: %v", err)
	}

	first, err := pool.Acquire(context.Background(), "example.com:443")
	if err != nil {
		t.Fatalf("acquire first tunnel: %v", err)
	}
	if first.cfg.Port != fastCfg.Port {
		t.Fatalf("expected first acquire to prefer fast port %d, got %d", fastCfg.Port, first.cfg.Port)
	}
	first.channel.releaseConsumed(first)

	refillCtx, refillCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer refillCancel()
	if err := pool.WaitUntilReady(refillCtx, "example.com:443", 2); err != nil {
		t.Fatalf("wait for refilled tunnels: %v", err)
	}

	second, err := pool.Acquire(context.Background(), "example.com:443")
	if err != nil {
		t.Fatalf("acquire second tunnel: %v", err)
	}
	defer second.channel.releaseConsumed(second)
	if second.cfg.Port != fastCfg.Port {
		t.Fatalf("expected second acquire to keep preferring fast port %d, got %d", fastCfg.Port, second.cfg.Port)
	}
}
