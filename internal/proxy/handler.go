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
	"sync"
	"time"
)

type PortPicker interface {
	Pick(used map[int]bool) int
}

type HandlerConfig struct {
	UpstreamHost            string
	Picker                  PortPicker
	MaxRetries403           int
	Timeout                 time.Duration
	DialTimeout             time.Duration
	KeepAliveIdleTimeout    time.Duration
	KeepAliveMaxIdleConns   int
	KeepAliveMaxIdlePerHost int
	StickyPortTTL           time.Duration
	BadProxyPenaltyBase     time.Duration
	BadProxyPenaltyMax      time.Duration
	BadProxyPickSamples     int
	ServiceUser             string
	ServicePass             string

	Creds *CredentialProvider
}

type Handler struct {
	cfg      HandlerConfig
	sticky   *stickyPortState
	badPorts *badPortState
}

type badPortState struct {
	base    time.Duration
	max     time.Duration
	mu      sync.Mutex
	penalty map[int]badPortPenalty
}

type badPortPenalty struct {
	failures int
	until    time.Time
}

type stickyPortState struct {
	ttl      time.Duration
	mu       sync.Mutex
	byChanID map[int]stickyPortEntry
}

type stickyPortEntry struct {
	port      int
	expiresAt time.Time
}

func newStickyPortState(ttl time.Duration) *stickyPortState {
	if ttl <= 0 {
		return nil
	}
	return &stickyPortState{
		ttl:      ttl,
		byChanID: make(map[int]stickyPortEntry),
	}
}

func (s *stickyPortState) Get(chanID int) (int, bool) {
	if s == nil {
		return 0, false
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.byChanID[chanID]
	if !ok {
		return 0, false
	}
	if !entry.expiresAt.After(now) {
		delete(s.byChanID, chanID)
		return 0, false
	}
	return entry.port, true
}

func (s *stickyPortState) Set(chanID int, port int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.byChanID[chanID] = stickyPortEntry{port: port, expiresAt: time.Now().Add(s.ttl)}
	s.mu.Unlock()
}

func (s *stickyPortState) Delete(chanID int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.byChanID, chanID)
	s.mu.Unlock()
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
	if cfg.KeepAliveIdleTimeout <= 0 {
		cfg.KeepAliveIdleTimeout = 90 * time.Second
	}
	if cfg.KeepAliveMaxIdleConns <= 0 {
		cfg.KeepAliveMaxIdleConns = 1000
	}
	if cfg.KeepAliveMaxIdlePerHost <= 0 {
		cfg.KeepAliveMaxIdlePerHost = 100
	}
	if cfg.BadProxyPenaltyBase <= 0 {
		cfg.BadProxyPenaltyBase = 15 * time.Second
	}
	if cfg.BadProxyPenaltyMax <= 0 {
		cfg.BadProxyPenaltyMax = 5 * time.Minute
	}
	if cfg.BadProxyPenaltyMax < cfg.BadProxyPenaltyBase {
		cfg.BadProxyPenaltyMax = cfg.BadProxyPenaltyBase
	}
	if cfg.BadProxyPickSamples <= 0 {
		cfg.BadProxyPickSamples = 8
	}

	return &Handler{
		cfg:      cfg,
		sticky:   newStickyPortState(cfg.StickyPortTTL),
		badPorts: newBadPortState(cfg.BadProxyPenaltyBase, cfg.BadProxyPenaltyMax),
	}
}

func newBadPortState(base, max time.Duration) *badPortState {
	return &badPortState{
		base:    base,
		max:     max,
		penalty: make(map[int]badPortPenalty),
	}
}

func (s *badPortState) MarkFailure(port int) time.Duration {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.penalty[port]
	if entry.until.Before(now) {
		entry.failures = 0
	}
	entry.failures++

	dur := s.base
	for i := 1; i < entry.failures; i++ {
		if dur >= s.max {
			dur = s.max
			break
		}
		dur *= 2
		if dur > s.max {
			dur = s.max
			break
		}
	}

	entry.until = now.Add(dur)
	s.penalty[port] = entry
	return dur
}

func (s *badPortState) MarkSuccess(port int) {
	s.mu.Lock()
	delete(s.penalty, port)
	s.mu.Unlock()
}

func (s *badPortState) IsPenalized(port int) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.penalty[port]
	if !ok {
		return false
	}
	if !entry.until.After(now) {
		delete(s.penalty, port)
		return false
	}
	return true
}

func (s *badPortState) RemainingPenalty(port int) time.Duration {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.penalty[port]
	if !ok {
		return 0
	}
	if !entry.until.After(now) {
		delete(s.penalty, port)
		return 0
	}
	return entry.until.Sub(now)
}

func (h *Handler) pickPort(used map[int]bool, stickyPort int, hasSticky bool) int {
	if hasSticky && !used[stickyPort] && !h.badPorts.IsPenalized(stickyPort) {
		return stickyPort
	}

	candidateUsed := make(map[int]bool, len(used)+h.cfg.BadProxyPickSamples)
	for p := range used {
		candidateUsed[p] = true
	}

	bestPort := 0
	bestPenalty := time.Duration(1<<63 - 1)
	haveBest := false

	for i := 0; i < h.cfg.BadProxyPickSamples; i++ {
		port := h.cfg.Picker.Pick(candidateUsed)
		candidateUsed[port] = true
		penalty := h.badPorts.RemainingPenalty(port)
		if penalty == 0 {
			return port
		}
		if !haveBest || penalty < bestPenalty {
			haveBest = true
			bestPort = port
			bestPenalty = penalty
		}
	}

	if haveBest {
		return bestPort
	}
	return h.cfg.Picker.Pick(used)
}

func (h *Handler) markBadPort(port int, chanIdx int, reason string) {
	penalty := h.badPorts.MarkFailure(port)
	if stickyPort, ok := h.sticky.Get(chanIdx); ok && stickyPort == port {
		h.sticky.Delete(chanIdx)
	}
	log.Printf("proxy-port-penalty: port=%d channel=%d reason=%s cooldown=%s", port, chanIdx+1, reason, penalty)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now()

	if !h.isAuthorized(r) {
		log.Printf("auth failed: method=%s target=%s duration=%s Proxy-Authorization=%q",
			r.Method, requestTarget(r), time.Since(startedAt), r.Header.Get("Proxy-Authorization"))
		w.Header().Set("Proxy-Authenticate", `Basic realm="smb-proxy-service-wb"`)
		http.Error(w, "proxy authentication required", http.StatusProxyAuthRequired)
		return
	}

	if r.Method == http.MethodConnect {
		h.handleConnect(w, r, startedAt)
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
	stickyPort, hasStickyPort := h.sticky.Get(chanIdx)

	attempts := 1 + h.cfg.MaxRetries403
	usedPorts := make(map[int]bool, attempts)

	var lastStatus int
	var lastErr error

	for i := 0; i < attempts; i++ {
		port := h.pickPort(usedPorts, stickyPort, i == 0 && hasStickyPort)
		usedPorts[port] = true

		ctx, cancel := context.WithTimeout(r.Context(), h.cfg.Timeout)
		resp, err := DoGETViaUpstreamProxy(ctx, UpstreamConfig{
			Host:                    h.cfg.UpstreamHost,
			Port:                    port,
			User:                    cred.User,
			Pass:                    cred.Pass,
			DialTimeout:             h.cfg.DialTimeout,
			KeepAliveIdleTimeout:    h.cfg.KeepAliveIdleTimeout,
			KeepAliveMaxIdleConns:   h.cfg.KeepAliveMaxIdleConns,
			KeepAliveMaxIdlePerHost: h.cfg.KeepAliveMaxIdlePerHost,
		}, r)
		cancel()

		if err != nil {
			h.markBadPort(port, chanIdx, "dial_or_request_error")
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		lastStatus = resp.StatusCode

		// Retry policy: 403/407 from upstream -> retry on another port, same credential channel.
		// 407 from upstream must NOT be forwarded to client (client would think it's our auth).
		if (resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusProxyAuthRequired) && i < attempts-1 {
			h.markBadPort(port, chanIdx, fmt.Sprintf("status_%d", resp.StatusCode))
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			continue
		}
		// Convert upstream 407 to 502 so client doesn't confuse it with our proxy auth.
		if resp.StatusCode == http.StatusProxyAuthRequired {
			h.markBadPort(port, chanIdx, "status_407")
			io.Copy(io.Discard, resp.Body)
			http.Error(w, fmt.Sprintf("upstream auth failed on all attempts (channel=%d): check upstream credentials", chanIdx+1), http.StatusBadGateway)
			return
		}

		h.badPorts.MarkSuccess(port)
		h.sticky.Set(chanIdx, port)

		// Optional debug header (can remove if you don't want to leak internals to 1C):
		w.Header().Set("X-Proxy-Channel", fmt.Sprintf("%d", chanIdx+1))

		CopyResponseHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		log.Printf("access: %s %s -> status=%d channel=%d port=%d duration=%s",
			r.Method, r.URL, resp.StatusCode, chanIdx+1, port, time.Since(startedAt))
		return
	}

	if lastErr != nil {
		log.Printf("access: %s %s -> error channel=%d duration=%s: %v",
			r.Method, r.URL, chanIdx+1, time.Since(startedAt), lastErr)
		http.Error(w, fmt.Sprintf("upstream connect/request failed (channel=%d): %v", chanIdx+1, lastErr), http.StatusBadGateway)
		return
	}
	log.Printf("access: %s %s -> all attempts failed status=%d channel=%d duration=%s",
		r.Method, r.URL, lastStatus, chanIdx+1, time.Since(startedAt))
	http.Error(w, fmt.Sprintf("upstream returned status %d on all attempts (channel=%d)", lastStatus, chanIdx+1), http.StatusBadGateway)
}

func (h *Handler) handleConnect(w http.ResponseWriter, r *http.Request, startedAt time.Time) {
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
	stickyPort, hasStickyPort := h.sticky.Get(chanIdx)
	attempts := 1 + h.cfg.MaxRetries403
	usedPorts := make(map[int]bool, attempts)

	var lastStatus int
	var lastErr error

	for i := 0; i < attempts; i++ {
		port := h.pickPort(usedPorts, stickyPort, i == 0 && hasStickyPort)
		usedPorts[port] = true

		ctx, cancel := context.WithTimeout(r.Context(), h.cfg.Timeout)
		upstreamConn, upstreamReader, status, err := OpenCONNECTTunnelViaUpstreamProxy(ctx, UpstreamConfig{
			Host:        h.cfg.UpstreamHost,
			Port:        port,
			User:        cred.User,
			Pass:        cred.Pass,
			DialTimeout: h.cfg.DialTimeout,
		}, target)
		cancel()

		if err != nil {
			h.markBadPort(port, chanIdx, "connect_dial_error")
			lastErr = err
			continue
		}

		lastStatus = status
		if (status == http.StatusForbidden || status == http.StatusProxyAuthRequired) && i < attempts-1 {
			h.markBadPort(port, chanIdx, fmt.Sprintf("connect_status_%d", status))
			continue
		}
		if status == http.StatusProxyAuthRequired {
			h.markBadPort(port, chanIdx, "connect_status_407")
			http.Error(w, fmt.Sprintf("upstream auth failed on all attempts (channel=%d): check upstream credentials", chanIdx+1), http.StatusBadGateway)
			return
		}
		if status != http.StatusOK {
			h.markBadPort(port, chanIdx, fmt.Sprintf("connect_status_%d", status))
			http.Error(w, fmt.Sprintf("upstream CONNECT failed with status %d (channel=%d)", status, chanIdx+1), http.StatusBadGateway)
			return
		}

		h.badPorts.MarkSuccess(port)
		h.sticky.Set(chanIdx, port)

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

		setupDuration := time.Since(startedAt)
		log.Printf("connect-established: %s -> status=200 channel=%d port=%d setup_duration=%s",
			target, chanIdx+1, port, setupDuration)
		tunnelStartedAt := time.Now()

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

		copyErr := <-errCh
		clientConn.Close()
		upstreamConn.Close()
		if copyErr != nil && copyErr != io.EOF {
			log.Printf("connect-closed: %s -> channel=%d port=%d tunnel_duration=%s copy_err=%v",
				target, chanIdx+1, port, time.Since(tunnelStartedAt), copyErr)
		} else {
			log.Printf("connect-closed: %s -> channel=%d port=%d tunnel_duration=%s",
				target, chanIdx+1, port, time.Since(tunnelStartedAt))
		}
		return
	}

	if lastErr != nil {
		log.Printf("access: CONNECT %s -> error channel=%d duration=%s: %v",
			target, chanIdx+1, time.Since(startedAt), lastErr)
		http.Error(w, fmt.Sprintf("upstream connect/request failed (channel=%d): %v", chanIdx+1, lastErr), http.StatusBadGateway)
		return
	}
	log.Printf("access: CONNECT %s -> all attempts failed status=%d channel=%d duration=%s",
		target, lastStatus, chanIdx+1, time.Since(startedAt))
	http.Error(w, fmt.Sprintf("upstream returned status %d on all attempts (channel=%d)", lastStatus, chanIdx+1), http.StatusBadGateway)
}

func requestTarget(r *http.Request) string {
	if r.Method == http.MethodConnect {
		if r.Host != "" {
			return r.Host
		}
		if r.URL != nil {
			return r.URL.Host
		}
		return ""
	}
	if r.URL != nil {
		return r.URL.String()
	}
	return ""
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
