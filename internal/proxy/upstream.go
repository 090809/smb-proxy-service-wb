package proxy

import (
	"context"
	"fmt"
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
