package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type PortPicker interface {
	Pick(used map[int]bool) int
}

type HandlerConfig struct {
	UpstreamHost  string
	Picker        PortPicker
	MaxRetries403 int
	Timeout       time.Duration

	Creds *CredentialProvider
}

type Handler struct {
	cfg HandlerConfig
}

func NewHandler(cfg HandlerConfig) *Handler {
	if cfg.Picker == nil {
		panic("Picker is required")
	}
	if cfg.Creds == nil {
		panic("Creds provider is required")
	}
	return &Handler{cfg: cfg}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// only GET, only explicit proxy requests, only http://
	if r.Method != http.MethodGet {
		http.Error(w, "only GET is supported", http.StatusMethodNotAllowed)
		return
	}
	if r.URL == nil || r.URL.Scheme == "" || r.URL.Host == "" {
		http.Error(w, "expected absolute-form URL (e.g. GET http://host/path)", http.StatusBadRequest)
		return
	}
	if !strings.EqualFold(r.URL.Scheme, "http") {
		http.Error(w, "only http scheme is supported (no CONNECT/https)", http.StatusBadRequest)
		return
	}

	// Drain body if any (for GET should be empty).
	if r.Body != nil {
		defer r.Body.Close()
		io.Copy(io.Discard, io.LimitReader(r.Body, 1<<20))
	}

	// Select one of 6 channels (credentials) per request.
	chanIdx, cred := h.cfg.Creds.Next()

	attempts := 1 + h.cfg.MaxRetries403
	usedPorts := make(map[int]bool, attempts)

	var lastStatus int
	var lastErr error

	for i := 0; i < attempts; i++ {
		port := h.cfg.Picker.Pick(usedPorts)
		usedPorts[port] = true

		ctx, cancel := context.WithTimeout(r.Context(), h.cfg.Timeout)
		resp, err := DoGETViaUpstreamProxy(ctx, UpstreamConfig{
			Host: h.cfg.UpstreamHost,
			Port: port,
			User: cred.User,
			Pass: cred.Pass,
		}, r)
		cancel()

		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		lastStatus = resp.StatusCode

		// Retry policy: 403 -> retry on another port, same credential channel.
		if resp.StatusCode == http.StatusForbidden && i < attempts-1 {
			io.Copy(io.Discard, resp.Body)
			continue
		}

		// Optional debug header (can remove if you don't want to leak internals to 1C):
		w.Header().Set("X-Proxy-Channel", fmt.Sprintf("%d", chanIdx+1))

		CopyResponseHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	if lastErr != nil {
		http.Error(w, fmt.Sprintf("upstream connect/request failed (channel=%d): %v", chanIdx+1, lastErr), http.StatusBadGateway)
		return
	}
	http.Error(w, fmt.Sprintf("upstream returned status %d on all attempts (channel=%d)", lastStatus, chanIdx+1), http.StatusBadGateway)
}
