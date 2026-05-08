package proxy

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net"
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
	ServiceUser   string
	ServicePass   string

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
	if cfg.ServiceUser == "" || cfg.ServicePass == "" {
		panic("Service auth is required")
	}
	return &Handler{cfg: cfg}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.isAuthorized(r) {
		log.Printf("auth failed: Proxy-Authorization=%q", r.Header.Get("Proxy-Authorization"))
		w.Header().Set("Proxy-Authenticate", `Basic realm="smb-proxy-service-wb"`)
		http.Error(w, "proxy authentication required", http.StatusProxyAuthRequired)
		return
	}

	if r.Method == http.MethodConnect {
		h.handleConnect(w, r)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "only GET and CONNECT are supported", http.StatusMethodNotAllowed)
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

		// Retry policy: 403/407 from upstream -> retry on another port, same credential channel.
		// 407 from upstream must NOT be forwarded to client (client would think it's our auth).
		if (resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusProxyAuthRequired) && i < attempts-1 {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			continue
		}
		// Convert upstream 407 to 502 so client doesn't confuse it with our proxy auth.
		if resp.StatusCode == http.StatusProxyAuthRequired {
			io.Copy(io.Discard, resp.Body)
			http.Error(w, fmt.Sprintf("upstream auth failed on all attempts (channel=%d): check upstream credentials", chanIdx+1), http.StatusBadGateway)
			return
		}

		// Optional debug header (can remove if you don't want to leak internals to 1C):
		w.Header().Set("X-Proxy-Channel", fmt.Sprintf("%d", chanIdx+1))

		CopyResponseHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		log.Printf("access: %s %s -> status=%d channel=%d port=%d",
			r.Method, r.URL, resp.StatusCode, chanIdx+1, port)
		return
	}

	if lastErr != nil {
		log.Printf("access: %s %s -> error channel=%d: %v", r.Method, r.URL, chanIdx+1, lastErr)
		http.Error(w, fmt.Sprintf("upstream connect/request failed (channel=%d): %v", chanIdx+1, lastErr), http.StatusBadGateway)
		return
	}
	log.Printf("access: %s %s -> all attempts failed status=%d channel=%d", r.Method, r.URL, lastStatus, chanIdx+1)
	http.Error(w, fmt.Sprintf("upstream returned status %d on all attempts (channel=%d)", lastStatus, chanIdx+1), http.StatusBadGateway)
}

func (h *Handler) handleConnect(w http.ResponseWriter, r *http.Request) {
	target := strings.TrimSpace(r.Host)
	if target == "" && r.URL != nil {
		target = strings.TrimSpace(r.URL.Host)
	}
	if target == "" {
		http.Error(w, "expected CONNECT host:port", http.StatusBadRequest)
		return
	}
	if _, _, err := net.SplitHostPort(target); err != nil {
		if addrErr, ok := err.(*net.AddrError); ok && addrErr.Err == "missing port in address" {
			target = net.JoinHostPort(target, "443")
		} else {
			http.Error(w, "invalid CONNECT target", http.StatusBadRequest)
			return
		}
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}

	chanIdx, cred := h.cfg.Creds.Next()
	attempts := 1 + h.cfg.MaxRetries403
	usedPorts := make(map[int]bool, attempts)

	var lastStatus int
	var lastErr error

	for i := 0; i < attempts; i++ {
		port := h.cfg.Picker.Pick(usedPorts)
		usedPorts[port] = true

		ctx, cancel := context.WithTimeout(r.Context(), h.cfg.Timeout)
		upstreamConn, upstreamReader, status, err := OpenCONNECTTunnelViaUpstreamProxy(ctx, UpstreamConfig{
			Host: h.cfg.UpstreamHost,
			Port: port,
			User: cred.User,
			Pass: cred.Pass,
		}, target)
		cancel()

		if err != nil {
			lastErr = err
			continue
		}

		lastStatus = status
		if (status == http.StatusForbidden || status == http.StatusProxyAuthRequired) && i < attempts-1 {
			continue
		}
		if status == http.StatusProxyAuthRequired {
			http.Error(w, fmt.Sprintf("upstream auth failed on all attempts (channel=%d): check upstream credentials", chanIdx+1), http.StatusBadGateway)
			return
		}
		if status != http.StatusOK {
			http.Error(w, fmt.Sprintf("upstream CONNECT failed with status %d (channel=%d)", status, chanIdx+1), http.StatusBadGateway)
			return
		}

		clientConn, _, err := hj.Hijack()
		if err != nil {
			upstreamConn.Close()
			http.Error(w, "failed to hijack client connection", http.StatusInternalServerError)
			return
		}

		if _, err = fmt.Fprintf(clientConn, "HTTP/1.1 200 Connection Established\r\nX-Proxy-Channel: %d\r\n\r\n", chanIdx+1); err != nil {
			clientConn.Close()
			upstreamConn.Close()
			return
		}

		errCh := make(chan error, 2)
		go func() {
			_, copyErr := io.Copy(upstreamConn, clientConn)
			errCh <- copyErr
		}()
		go func() {
			upstreamSrc := io.Reader(upstreamConn)
			if upstreamReader.Buffered() > 0 {
				upstreamSrc = io.MultiReader(upstreamReader, upstreamConn)
			}
			_, copyErr := io.Copy(clientConn, upstreamSrc)
			errCh <- copyErr
		}()

		<-errCh
		clientConn.Close()
		upstreamConn.Close()
		log.Printf("access: CONNECT %s -> status=200 channel=%d port=%d", target, chanIdx+1, port)
		return
	}

	if lastErr != nil {
		log.Printf("access: CONNECT %s -> error channel=%d: %v", target, chanIdx+1, lastErr)
		http.Error(w, fmt.Sprintf("upstream connect/request failed (channel=%d): %v", chanIdx+1, lastErr), http.StatusBadGateway)
		return
	}
	log.Printf("access: CONNECT %s -> all attempts failed status=%d channel=%d", target, lastStatus, chanIdx+1)
	http.Error(w, fmt.Sprintf("upstream returned status %d on all attempts (channel=%d)", lastStatus, chanIdx+1), http.StatusBadGateway)
}

func (h *Handler) isAuthorized(r *http.Request) bool {
	proxyAuth := r.Header.Get("Proxy-Authorization")
	if proxyAuth == "" {
		return false
	}

	// Trim any surrounding whitespace from the whole header value.
	proxyAuth = strings.TrimSpace(proxyAuth)

	const prefix = "Basic "
	if !strings.HasPrefix(proxyAuth, prefix) {
		return false
	}

	// Use RawStdEncoding to also accept base64 without padding.
	b64 := strings.TrimSpace(strings.TrimPrefix(proxyAuth, prefix))
	decoded, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(b64)
	}
	if err != nil {
		return false
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return false
	}

	userOK := subtle.ConstantTimeCompare([]byte(parts[0]), []byte(h.cfg.ServiceUser)) == 1
	passOK := subtle.ConstantTimeCompare([]byte(parts[1]), []byte(h.cfg.ServicePass)) == 1
	return userOK && passOK
}
