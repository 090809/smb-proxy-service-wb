package proxy

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// UpstreamConfig describes a single upstream proxy endpoint.
type UpstreamConfig struct {
	Host        string
	Port        int
	User        string
	Pass        string
	DialTimeout time.Duration
	KeepAliveIdleTimeout    time.Duration
	KeepAliveMaxIdleConns   int
	KeepAliveMaxIdlePerHost int
}

type pooledClient struct {
	client *http.Client
}

var upstreamHTTPClientPool sync.Map

func defaultKeepAliveIdleTimeout(v time.Duration) time.Duration {
	if v <= 0 {
		return 90 * time.Second
	}
	return v
}

func defaultKeepAliveMaxIdleConns(v int) int {
	if v <= 0 {
		return 1000
	}
	return v
}

func defaultKeepAliveMaxIdlePerHost(v int) int {
	if v <= 0 {
		return 100
	}
	return v
}

func upstreamClientPoolKey(cfg UpstreamConfig) string {
	return fmt.Sprintf("%s:%d|%s|%s|dial=%s|idle=%s|maxIdle=%d|maxIdleHost=%d",
		cfg.Host,
		cfg.Port,
		cfg.User,
		cfg.Pass,
		cfg.DialTimeout,
		defaultKeepAliveIdleTimeout(cfg.KeepAliveIdleTimeout),
		defaultKeepAliveMaxIdleConns(cfg.KeepAliveMaxIdleConns),
		defaultKeepAliveMaxIdlePerHost(cfg.KeepAliveMaxIdlePerHost),
	)
}

func getOrCreateUpstreamHTTPClient(cfg UpstreamConfig, proxyURL *url.URL) *http.Client {
	key := upstreamClientPoolKey(cfg)
	if v, ok := upstreamHTTPClientPool.Load(key); ok {
		return v.(*pooledClient).client
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: (&net.Dialer{
			Timeout: cfg.DialTimeout,
		}).DialContext,
		ForceAttemptHTTP2:     false,
		DisableKeepAlives:     false,
		IdleConnTimeout:       defaultKeepAliveIdleTimeout(cfg.KeepAliveIdleTimeout),
		MaxIdleConns:          defaultKeepAliveMaxIdleConns(cfg.KeepAliveMaxIdleConns),
		MaxIdleConnsPerHost:   defaultKeepAliveMaxIdlePerHost(cfg.KeepAliveMaxIdlePerHost),
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := &http.Client{Transport: transport}

	actual, _ := upstreamHTTPClientPool.LoadOrStore(key, &pooledClient{client: client})
	return actual.(*pooledClient).client
}

// DoGETViaUpstreamProxy forwards r as a GET request through the upstream HTTP proxy.
func DoGETViaUpstreamProxy(ctx context.Context, cfg UpstreamConfig, r *http.Request) (*http.Response, error) {
	proxyURL := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
	}
	if cfg.User != "" {
		proxyURL.User = url.UserPassword(cfg.User, cfg.Pass)
	}

	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 5 * time.Second
	}
	cfg.DialTimeout = dialTimeout
	client := getOrCreateUpstreamHTTPClient(cfg, proxyURL)

	outReq, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL.String(), nil)
	if err != nil {
		return nil, err
	}
	// Copy safe headers from the original request, excluding hop-by-hop and proxy auth.
	skipHeaders := map[string]bool{
		"Proxy-Authorization": true,
		"Proxy-Connection":    true,
		"Connection":          true,
		"Keep-Alive":          true,
		"Te":                  true,
		"Trailers":            true,
		"Transfer-Encoding":   true,
		"Upgrade":             true,
	}
	for key, vals := range r.Header {
		if skipHeaders[key] {
			continue
		}
		outReq.Header[key] = vals
	}

	return client.Do(outReq)
}

// OpenCONNECTTunnelViaUpstreamProxy opens a CONNECT tunnel to target via the upstream proxy.
// On successful tunnel establishment it returns status 200 and an open upstream connection.
// For non-200 proxy responses it returns the response status and no connection.
func OpenCONNECTTunnelViaUpstreamProxy(ctx context.Context, cfg UpstreamConfig, target string) (net.Conn, *bufio.Reader, int, error) {
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 5 * time.Second
	}
	dialCtx, dialCancel := context.WithTimeout(ctx, dialTimeout)
	defer dialCancel()
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(dialCtx, "tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.Port))
	if err != nil {
		return nil, nil, 0, err
	}

	req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Connection: Keep-Alive\r\n", target, target)
	if cfg.User != "" {
		token := base64.StdEncoding.EncodeToString([]byte(cfg.User + ":" + cfg.Pass))
		req += fmt.Sprintf("Proxy-Authorization: Basic %s\r\n", token)
	}
	req += "\r\n"

	if _, err = conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, nil, 0, err
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		conn.Close()
		return nil, nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		conn.Close()
		return nil, nil, resp.StatusCode, nil
	}

	return conn, br, resp.StatusCode, nil
}

// CopyResponseHeaders copies headers from src to dst, skipping hop-by-hop headers.
func CopyResponseHeaders(dst, src http.Header) {
	hopByHop := map[string]bool{
		"Connection":          true,
		"Keep-Alive":          true,
		"Proxy-Authenticate":  true,
		"Proxy-Authorization": true,
		"Te":                  true,
		"Trailers":            true,
		"Transfer-Encoding":   true,
		"Upgrade":             true,
	}
	for key, vals := range src {
		if hopByHop[key] {
			continue
		}
		dst[key] = vals
	}
}
