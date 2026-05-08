package proxy

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
)

// UpstreamConfig describes a single upstream proxy endpoint.
type UpstreamConfig struct {
	Host string
	Port int
	User string
	Pass string
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

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
	}
	client := &http.Client{Transport: transport}

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
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.Port))
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
