package proxy

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

func shouldCooldownEndpoint(statusCode int) bool {
	switch statusCode {
	case http.StatusForbidden, http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func shouldDiscardConnection(statusCode int) bool {
	return statusCode == http.StatusProxyAuthRequired || shouldCooldownEndpoint(statusCode)
}

// UpstreamConfig describes a single upstream proxy endpoint.
type UpstreamConfig struct {
	Host        string
	Port        int
	User        string
	Pass        string
	DialTimeout time.Duration
}

type upstreamResponseBody struct {
	io.ReadCloser
	pc          *pooledUpstreamConn
	reusable    bool
	cooldown    time.Duration
	markSuccess bool
	sawEOF      bool
	closed      bool
}

func (b *upstreamResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.sawEOF = true
	}
	return n, err
}

func (b *upstreamResponseBody) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true

	bodyErr := b.ReadCloser.Close()
	b.pc.channel.release(b.pc, bodyErr == nil && b.sawEOF && b.reusable, b.cooldown, b.markSuccess)
	if bodyErr != nil {
		return bodyErr
	}
	return nil
}

func defaultDialTimeout(v time.Duration) time.Duration {
	if v <= 0 {
		return 5 * time.Second
	}
	return v
}

func dialUpstreamProxy(ctx context.Context, cfg UpstreamConfig) (net.Conn, error) {
	dialCtx, dialCancel := context.WithTimeout(ctx, defaultDialTimeout(cfg.DialTimeout))
	defer dialCancel()

	dialer := net.Dialer{}
	return dialer.DialContext(dialCtx, "tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.Port))
}

func copyHeaderValues(dst, src http.Header, skip map[string]struct{}) {
	for key, vals := range src {
		if _, shouldSkip := skip[http.CanonicalHeaderKey(key)]; shouldSkip {
			continue
		}
		dst[key] = append([]string(nil), vals...)
	}
}

func basicProxyAuthHeader(user, pass string) string {
	token := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	return "Basic " + token
}

func doGETViaUpstreamConn(ctx context.Context, pc *pooledUpstreamConn, r *http.Request) (*http.Response, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := pc.conn.SetDeadline(deadline); err != nil {
			return nil, err
		}
	}

	targetURL := *r.URL
	outReq := &http.Request{
		Method:     http.MethodGet,
		URL:        &targetURL,
		Host:       r.Host,
		Header:     make(http.Header, len(r.Header)+1),
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Close:      false,
	}
	if outReq.Host == "" {
		outReq.Host = r.URL.Host
	}
	copyHeaderValues(outReq.Header, r.Header, map[string]struct{}{
		"Connection":          {},
		"Keep-Alive":          {},
		"Proxy-Authorization": {},
		"Proxy-Connection":    {},
		"Te":                  {},
		"Trailers":            {},
		"Transfer-Encoding":   {},
		"Upgrade":             {},
	})
	outReq.Header.Set("Proxy-Connection", "Keep-Alive")
	if pc.proxyAuthHeader != "" {
		outReq.Header.Set("Proxy-Authorization", pc.proxyAuthHeader)
	}

	if err := outReq.WriteProxy(pc.conn); err != nil {
		return nil, err
	}

	resp, err := http.ReadResponse(pc.reader, outReq)
	if err != nil {
		return nil, err
	}
	if err := pc.conn.SetDeadline(time.Time{}); err != nil {
		resp.Body.Close()
		return nil, err
	}

	pc.requests++
	cooldown := time.Duration(0)
	if shouldCooldownEndpoint(resp.StatusCode) {
		cooldown = upstreamBadStatusCooldown
	}

	resp.Body = &upstreamResponseBody{
		ReadCloser:  resp.Body,
		pc:          pc,
		reusable:    !resp.Close && !shouldDiscardConnection(resp.StatusCode),
		cooldown:    cooldown,
		markSuccess: resp.StatusCode != http.StatusProxyAuthRequired && !shouldCooldownEndpoint(resp.StatusCode),
	}
	return resp, nil
}

// OpenCONNECTTunnelViaUpstreamProxy opens a CONNECT tunnel to target via the upstream proxy.
// On successful tunnel establishment it returns status 200 and an open upstream connection.
// For non-200 proxy responses it returns the response status and no connection.
func OpenCONNECTTunnelViaUpstreamProxy(ctx context.Context, cfg UpstreamConfig, target string) (net.Conn, *bufio.Reader, int, error) {
	conn, err := dialUpstreamProxy(ctx, cfg)
	if err != nil {
		return nil, nil, 0, err
	}

	req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Connection: Keep-Alive\r\n", target, target)
	if cfg.User != "" {
		req += fmt.Sprintf("Proxy-Authorization: %s\r\n", basicProxyAuthHeader(cfg.User, cfg.Pass))
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
	copyHeaderValues(dst, src, map[string]struct{}{
		"Connection":          {},
		"Keep-Alive":          {},
		"Proxy-Authenticate":  {},
		"Proxy-Authorization": {},
		"Te":                  {},
		"Trailers":            {},
		"Transfer-Encoding":   {},
		"Upgrade":             {},
	})
}
