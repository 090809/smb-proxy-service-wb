package proxy

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type HandlerConfig struct {
	MaxRetries403 int
	Timeout       time.Duration
	StickyPortTTL time.Duration
	ServiceUser   string
	ServicePass   string
	GETPool       *UpstreamGETPool
	CONNECTPool   *UpstreamCONNECTPool

	Creds *CredentialProvider
}

type Handler struct {
	cfg          HandlerConfig
	sticky       *stickyPortState
	badPorts     *badPortState
	portSelector *portSelector
	getPool      *UpstreamGETPool
	connectPool  *UpstreamCONNECTPool
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
	if cfg.Creds == nil {
		panic("Creds provider is required")
	}
	if cfg.GETPool == nil {
		panic("GET pool is required")
	}
	if cfg.CONNECTPool == nil {
		panic("CONNECT pool is required")
	}
	if cfg.ServiceUser == "" || cfg.ServicePass == "" {
		panic("Service auth is required")
	}

	return &Handler{
		cfg:          cfg,
		sticky:       newStickyPortState(cfg.StickyPortTTL),
		badPorts:     cfg.GETPool.badPorts,
		portSelector: cfg.GETPool.selector,
		getPool:      cfg.GETPool,
		connectPool:  cfg.CONNECTPool,
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
	return s.MarkFailureFor(port, 0)
}

func (s *badPortState) MarkFailureFor(port int, forced time.Duration) time.Duration {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.penalty[port]
	if entry.until.Before(now) {
		entry.failures = 0
	}
	entry.failures++

	dur := forced
	if dur <= 0 {
		dur = s.base
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

func (h *Handler) selectPort(stickyPort int, hasSticky bool) int {
	if hasSticky && !h.badPorts.IsPenalized(stickyPort) {
		return stickyPort
	}
	port, ok := h.portSelector.pick(h.badPorts.IsPenalized)
	if ok {
		return port
	}
	port, _ = h.portSelector.pick(nil)
	return port
}

func (h *Handler) markBadPort(port int, chanIdx int, reason string) {
	h.markBadPortFor(port, chanIdx, reason, 0)
}

func (h *Handler) markBadPortFor(port int, chanIdx int, reason string, penaltyOverride time.Duration) {
	var penalty time.Duration
	if penaltyOverride > 0 {
		penalty = h.badPorts.MarkFailureFor(port, penaltyOverride)
	} else {
		penalty = h.badPorts.MarkFailure(port)
	}
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

	attempts := 1 + h.cfg.MaxRetries403

	var lastStatus int
	var lastErr error
	lastChanIdx := -1

	for i := 0; i < attempts; i++ {
		ctx, cancel := context.WithTimeout(r.Context(), h.cfg.Timeout)
		resp, chanIdx, port, err := h.getPool.Do(ctx, r)

		if err != nil {
			cancel()
			if port != 0 && chanIdx >= 0 {
				h.markBadPort(port, chanIdx, "dial_or_request_error")
			}
			if errors.Is(err, errUpstreamPoolEmpty) {
				lastErr = fmt.Errorf("no prewarmed upstream connection available: %w", err)
			} else {
				lastErr = err
			}
			continue
		}

		lastStatus = resp.StatusCode
		lastChanIdx = chanIdx

		// Retry policy: retry on another port for statuses that invalidate the current upstream path.
		if shouldCooldownEndpoint(resp.StatusCode) && i < attempts-1 {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			cancel()
			continue
		}
		// 407 from upstream must NOT be forwarded to client (client would think it's our auth).
		if resp.StatusCode == http.StatusProxyAuthRequired && i < attempts-1 {
			h.markBadPort(port, chanIdx, fmt.Sprintf("status_%d", resp.StatusCode))
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			cancel()
			continue
		}
		// Convert upstream 407 to 502 so client doesn't confuse it with our proxy auth.
		if resp.StatusCode == http.StatusProxyAuthRequired {
			h.markBadPort(port, chanIdx, "status_407")
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			cancel()
			http.Error(w, fmt.Sprintf("upstream auth failed on all attempts (channel=%d): check upstream credentials", chanIdx+1), http.StatusBadGateway)
			return
		}

		// Optional debug header (can remove if you don't want to leak internals to 1C):
		w.Header().Set("X-Proxy-Channel", fmt.Sprintf("%d", chanIdx+1))

		CopyResponseHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		resp.Body.Close()
		cancel()
		log.Printf("access: %s %s -> status=%d channel=%d port=%d duration=%s",
			r.Method, r.URL, resp.StatusCode, chanIdx+1, port, time.Since(startedAt))
		return
	}

	if lastErr != nil {
		channelLabel := "unknown"
		if lastChanIdx >= 0 {
			channelLabel = fmt.Sprintf("%d", lastChanIdx+1)
		}
		log.Printf("access: %s %s -> error channel=%s duration=%s: %v",
			r.Method, r.URL, channelLabel, time.Since(startedAt), lastErr)
		http.Error(w, fmt.Sprintf("upstream connect/request failed (channel=%s): %v", channelLabel, lastErr), http.StatusBadGateway)
		return
	}
	channelLabel := "unknown"
	if lastChanIdx >= 0 {
		channelLabel = fmt.Sprintf("%d", lastChanIdx+1)
	}
	log.Printf("access: %s %s -> all attempts failed status=%d channel=%s duration=%s",
		r.Method, r.URL, lastStatus, channelLabel, time.Since(startedAt))
	http.Error(w, fmt.Sprintf("upstream returned status %d on all attempts (channel=%s)", lastStatus, channelLabel), http.StatusBadGateway)
}

func (h *Handler) handleConnect(w http.ResponseWriter, r *http.Request, startedAt time.Time) {
	rawTarget := strings.TrimSpace(r.Host)
	if rawTarget == "" && r.URL != nil {
		rawTarget = strings.TrimSpace(r.URL.Host)
	}
	if rawTarget == "" {
		http.Error(w, "expected CONNECT host:port", http.StatusBadRequest)
		return
	}
	target, err := canonicalConnectTarget(rawTarget)
	if err != nil {
		http.Error(w, "invalid CONNECT target", http.StatusBadRequest)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.Timeout)
	pt, err := h.connectPool.Acquire(ctx, target)
	cancel()
	if err != nil {
		log.Printf("access: CONNECT %s -> error duration=%s: %v", target, time.Since(startedAt), err)
		http.Error(w, fmt.Sprintf("upstream connect/request failed: %v", err), http.StatusBadGateway)
		return
	}

	clientConn, _, err := hj.Hijack()
	if err != nil {
		pt.channel.releaseFailed(pt, 0)
		http.Error(w, "failed to hijack client connection", http.StatusInternalServerError)
		return
	}

	if _, err = fmt.Fprintf(clientConn, "HTTP/1.1 200 Connection Established\r\nX-Proxy-Channel: %d\r\n\r\n", pt.channel.chanID+1); err != nil {
		clientConn.Close()
		pt.channel.releaseFailed(pt, 0)
		return
	}

	setupDuration := time.Since(startedAt)
	log.Printf("connect-established: %s -> status=200 channel=%d port=%d setup_duration=%s",
		target, pt.channel.chanID+1, pt.cfg.Port, setupDuration)
	tunnelStartedAt := time.Now()

	errCh := make(chan error, 2)
	go func() {
		_, copyErr := io.Copy(pt.conn, clientConn)
		errCh <- copyErr
	}()
	go func() {
		upstreamSrc := io.Reader(pt.conn)
		if pt.reader.Buffered() > 0 {
			upstreamSrc = io.MultiReader(pt.reader, pt.conn)
		}
		_, copyErr := io.Copy(clientConn, upstreamSrc)
		errCh <- copyErr
	}()

	copyErr := <-errCh
	clientConn.Close()
	pt.channel.releaseConsumed(pt)
	if copyErr != nil && copyErr != io.EOF {
		log.Printf("connect-closed: %s -> channel=%d port=%d tunnel_duration=%s copy_err=%v",
			target, pt.channel.chanID+1, pt.cfg.Port, time.Since(tunnelStartedAt), copyErr)
	} else {
		log.Printf("connect-closed: %s -> channel=%d port=%d tunnel_duration=%s",
			target, pt.channel.chanID+1, pt.cfg.Port, time.Since(tunnelStartedAt))
	}
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
