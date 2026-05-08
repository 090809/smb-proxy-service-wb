package proxy

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	upstreamConnectWarmTarget      = 1
	upstreamConnectWarmTargetTotal = 60
	upstreamSlowConnectThreshold   = 500 * time.Millisecond
	upstreamSlowConnectPenaltyTTL  = time.Minute
)

type UpstreamCONNECTPool struct {
	host        string
	dialTimeout time.Duration
	selector    *portSelector
	badPorts    *badPortState
	sticky      *stickyPortState
	creds       []Credential
	warmTarget  int

	mu      sync.Mutex
	buckets map[string]*connectTargetPool
}

type connectTargetPool struct {
	parent      *UpstreamCONNECTPool
	target      string
	channels    []*connectTargetChannel
	readySignal chan struct{}
	next        atomic.Uint64

	statsMu      sync.Mutex
	portStats    map[int]connectPortStat
	lastUsedPort int
}

type connectTargetChannel struct {
	targetPool *connectTargetPool
	chanID     int
	cred       Credential

	ready  chan *pooledConnectTunnel
	refill chan struct{}

	mu          sync.Mutex
	activePorts map[int]int
	dialing     int
}

type pooledConnectTunnel struct {
	cfg             UpstreamConfig
	target          string
	proxyAuthUser   string
	proxyAuthPass   string
	proxyAuthHeader string
	warmDuration    time.Duration
	conn            net.Conn
	reader          *bufio.Reader
	channel         *connectTargetChannel
}

type connectPortStat struct {
	ewma time.Duration
}

func NewUpstreamCONNECTPool(host string, dialTimeout time.Duration, creds []Credential, portMin int, portMax int, stickyPortTTL time.Duration, badProxyPenaltyBase time.Duration, badProxyPenaltyMax time.Duration) *UpstreamCONNECTPool {
	return NewUpstreamCONNECTPoolWithWarmTarget(host, dialTimeout, creds, portMin, portMax, stickyPortTTL, badProxyPenaltyBase, badProxyPenaltyMax, upstreamConnectWarmTarget)
}

func NewUpstreamCONNECTPoolWithWarmTarget(host string, dialTimeout time.Duration, creds []Credential, portMin int, portMax int, stickyPortTTL time.Duration, badProxyPenaltyBase time.Duration, badProxyPenaltyMax time.Duration, warmTarget int) *UpstreamCONNECTPool {
	if len(creds) == 0 {
		panic("credentials are required")
	}
	if badProxyPenaltyBase <= 0 {
		badProxyPenaltyBase = 15 * time.Second
	}
	if badProxyPenaltyMax <= 0 {
		badProxyPenaltyMax = 5 * time.Minute
	}
	if badProxyPenaltyMax < badProxyPenaltyBase {
		badProxyPenaltyMax = badProxyPenaltyBase
	}
	if warmTarget <= 0 {
		warmTarget = upstreamConnectWarmTarget
	}

	return &UpstreamCONNECTPool{
		host:        host,
		dialTimeout: defaultDialTimeout(dialTimeout),
		selector:    newPortSelector(portMin, portMax),
		badPorts:    newBadPortState(badProxyPenaltyBase, badProxyPenaltyMax),
		sticky:      newStickyPortState(stickyPortTTL),
		creds:       append([]Credential(nil), creds...),
		warmTarget:  warmTarget,
		buckets:     make(map[string]*connectTargetPool),
	}
}

func NewUpstreamCONNECTPoolWithTotalWarmTarget(host string, dialTimeout time.Duration, creds []Credential, portMin int, portMax int, stickyPortTTL time.Duration, badProxyPenaltyBase time.Duration, badProxyPenaltyMax time.Duration, totalWarmTarget int) *UpstreamCONNECTPool {
	if totalWarmTarget <= 0 {
		totalWarmTarget = upstreamConnectWarmTargetTotal
	}
	perChannel := int(math.Ceil(float64(totalWarmTarget) / float64(maxInt(len(creds), 1))))
	return NewUpstreamCONNECTPoolWithWarmTarget(host, dialTimeout, creds, portMin, portMax, stickyPortTTL, badProxyPenaltyBase, badProxyPenaltyMax, perChannel)
}

func (p *UpstreamCONNECTPool) Acquire(ctx context.Context, target string) (*pooledConnectTunnel, error) {
	target, err := canonicalConnectTarget(target)
	if err != nil {
		return nil, err
	}
	bucket := p.getOrCreateBucket(target)

	for {
		if pt, ok := bucket.tryAcquire(); ok {
			return pt, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-bucket.readySignal:
		}
	}
}

func (p *UpstreamCONNECTPool) WaitUntilReady(ctx context.Context, target string, want int) error {
	if want <= 0 {
		return nil
	}

	target, err := canonicalConnectTarget(target)
	if err != nil {
		return err
	}
	bucket := p.getOrCreateBucket(target)
	for {
		if bucket.readyCount() >= want {
			return nil
		}
		bucket.signalRefill()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-bucket.readySignal:
		}
	}
}

func (p *UpstreamCONNECTPool) PrewarmTargets(ctx context.Context, targets []string) error {
	normalized := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		canonical, err := canonicalConnectTarget(target)
		if err != nil {
			return fmt.Errorf("invalid connect prewarm target %q: %w", target, err)
		}
		normalized[canonical] = struct{}{}
	}
	if len(normalized) == 0 {
		return nil
	}

	want := p.startupReadyPerTarget()
	errCh := make(chan error, len(normalized))
	var wg sync.WaitGroup
	for target := range normalized {
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			if err := p.WaitUntilReady(ctx, target, want); err != nil {
				errCh <- fmt.Errorf("prewarm CONNECT target %s: %w", target, err)
			}
		}(target)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		return err
	}
	return nil
}

func (p *UpstreamCONNECTPool) getOrCreateBucket(target string) *connectTargetPool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if bucket, ok := p.buckets[target]; ok {
		return bucket
	}

	bucket := &connectTargetPool{
		parent:      p,
		target:      target,
		readySignal: make(chan struct{}, 1),
		channels:    make([]*connectTargetChannel, 0, len(p.creds)),
		portStats:   make(map[int]connectPortStat),
	}
	for idx, cred := range p.creds {
		ch := &connectTargetChannel{
			targetPool:  bucket,
			chanID:      idx,
			cred:        cred,
			ready:       make(chan *pooledConnectTunnel, p.warmTarget),
			refill:      make(chan struct{}, 1),
			activePorts: make(map[int]int),
		}
		bucket.channels = append(bucket.channels, ch)
		go ch.fillLoop()
		ch.signalRefill()
	}

	p.buckets[target] = bucket
	return bucket
}

func (p *UpstreamCONNECTPool) startupReadyPerTarget() int {
	return 1
}

func (p *connectTargetPool) tryAcquire() (*pooledConnectTunnel, bool) {
	channelCount := len(p.channels)
	if channelCount == 0 {
		return nil, false
	}

	start := int((p.next.Add(1) - 1) % uint64(channelCount))
	candidates := make([]*pooledConnectTunnel, 0, channelCount)
	for i := 0; i < channelCount; i++ {
		ch := p.channels[(start+i)%channelCount]
		if pt, ok := ch.tryAcquire(); ok {
			candidates = append(candidates, pt)
		}
	}
	if len(candidates) == 0 {
		return nil, false
	}

	bestIdx := 0
	for i := 1; i < len(candidates); i++ {
		if p.shouldPreferCandidate(candidates[i], candidates[bestIdx]) {
			bestIdx = i
		}
	}
	chosen := candidates[bestIdx]
	for i, candidate := range candidates {
		if i == bestIdx {
			continue
		}
		candidate.channel.returnReady(candidate)
	}
	p.markPortUsed(chosen.cfg.Port)
	return chosen, true
}

func (p *connectTargetPool) readyCount() int {
	ready := 0
	for _, ch := range p.channels {
		ready += len(ch.ready)
	}
	return ready
}

func (p *connectTargetPool) signalReady() {
	select {
	case p.readySignal <- struct{}{}:
	default:
	}
}

func (p *connectTargetPool) signalRefill() {
	for _, ch := range p.channels {
		ch.signalRefill()
	}
}

func (c *connectTargetChannel) tryAcquire() (*pooledConnectTunnel, bool) {
	select {
	case pt := <-c.ready:
		c.signalRefill()
		return pt, true
	default:
		c.signalRefill()
		return nil, false
	}
}

func (c *connectTargetChannel) returnReady(pt *pooledConnectTunnel) {
	select {
	case c.ready <- pt:
	default:
		closeConnectTunnel(pt)
		c.signalRefill()
	}
}

func (c *connectTargetChannel) signalRefill() {
	select {
	case c.refill <- struct{}{}:
	default:
	}
}

func (c *connectTargetChannel) fillLoop() {
	for range c.refill {
		for i := 0; i < upstreamWarmBatchSize; i++ {
			port, ok := c.reserveFillPort()
			if !ok {
				break
			}
			go c.warmPort(port)
		}
	}
}

func (c *connectTargetChannel) warmPort(port int) {
	startedAt := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), c.targetPool.parent.dialTimeout)
	pt, status, err := c.dial(ctx, port)
	cancel()
	if err != nil {
		c.finishDialFailure(port, 0)
		return
	}
	if status != http.StatusOK {
		c.finishDialStatus(port, status)
		return
	}

	warmDuration := time.Since(startedAt)
	if warmDuration > upstreamSlowConnectThreshold {
		log.Printf("connect-pool-penalty: target=%s channel=%d port=%d reason=slow_warmup duration=%s cooldown=%s",
			c.targetPool.target, c.chanID+1, port, warmDuration, upstreamSlowConnectPenaltyTTL)
		c.finishSlowDial(pt)
		return
	}

	pt.warmDuration = warmDuration
	c.finishDialSuccess(pt)
}

func (c *connectTargetChannel) reserveFillPort() (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.ready)+c.dialing >= c.targetPool.parent.warmTarget {
		return 0, false
	}

	if preferredPort, ok := c.targetPool.preferredPort(func(port int) bool {
		return c.activePorts[port] > 0 || c.targetPool.parent.badPorts.IsPenalized(port)
	}); ok {
		c.activePorts[preferredPort]++
		c.dialing++
		return preferredPort, true
	}

	if stickyPort, ok := c.targetPool.parent.sticky.Get(c.chanID); ok && c.activePorts[stickyPort] == 0 && !c.targetPool.parent.badPorts.IsPenalized(stickyPort) {
		c.activePorts[stickyPort]++
		c.dialing++
		return stickyPort, true
	}

	port, ok := c.targetPool.parent.selector.pick(func(port int) bool {
		return c.activePorts[port] > 0 || c.targetPool.parent.badPorts.IsPenalized(port)
	})
	if !ok || c.targetPool.parent.badPorts.IsPenalized(port) {
		return 0, false
	}

	c.activePorts[port]++
	c.dialing++
	return port, true
}

func (c *connectTargetChannel) dial(ctx context.Context, port int) (*pooledConnectTunnel, int, error) {
	cfg := UpstreamConfig{
		Host:        c.targetPool.parent.host,
		Port:        port,
		User:        c.cred.User,
		Pass:        c.cred.Pass,
		DialTimeout: c.targetPool.parent.dialTimeout,
	}

	conn, reader, status, err := OpenCONNECTTunnelViaUpstreamProxy(ctx, cfg, c.targetPool.target)
	if err != nil {
		return nil, 0, err
	}
	if status != http.StatusOK {
		return nil, status, nil
	}

	return &pooledConnectTunnel{
		cfg:             cfg,
		target:          c.targetPool.target,
		proxyAuthUser:   c.cred.User,
		proxyAuthPass:   c.cred.Pass,
		proxyAuthHeader: basicProxyAuthHeader(c.cred.User, c.cred.Pass),
		warmDuration:    0,
		conn:            conn,
		reader:          reader,
		channel:         c,
	}, http.StatusOK, nil
}

func (c *connectTargetChannel) finishDialFailure(port int, cooldown time.Duration) {
	c.mu.Lock()
	c.dialing--
	c.deactivatePortLocked(port)
	c.mu.Unlock()

	if cooldown > 0 {
		c.targetPool.parent.badPorts.MarkFailureFor(port, cooldown)
	} else {
		c.targetPool.parent.badPorts.MarkFailure(port)
	}
	c.targetPool.forgetPort(port)
	c.targetPool.parent.sticky.Delete(c.chanID)
	c.signalRefill()
}

func (c *connectTargetChannel) finishDialStatus(port int, status int) {
	cooldown := time.Duration(0)
	if shouldCooldownEndpoint(status) {
		cooldown = upstreamBadStatusCooldown
	}
	c.finishDialFailure(port, cooldown)
}

func (c *connectTargetChannel) finishSlowDial(pt *pooledConnectTunnel) {
	c.mu.Lock()
	c.dialing--
	c.deactivatePortLocked(pt.cfg.Port)
	c.mu.Unlock()
	c.targetPool.parent.badPorts.MarkFailureFor(pt.cfg.Port, upstreamSlowConnectPenaltyTTL)
	c.targetPool.forgetPort(pt.cfg.Port)
	c.targetPool.parent.sticky.Delete(c.chanID)
	closeConnectTunnel(pt)
	c.signalRefill()
}

func (c *connectTargetChannel) finishDialSuccess(pt *pooledConnectTunnel) {
	c.mu.Lock()
	c.dialing--
	if !c.targetPool.parent.badPorts.IsPenalized(pt.cfg.Port) && len(c.ready) < cap(c.ready) {
		c.ready <- pt
		c.mu.Unlock()
		c.targetPool.parent.badPorts.MarkSuccess(pt.cfg.Port)
		c.targetPool.observeSuccess(pt.cfg.Port, pt.warmDuration)
		c.targetPool.parent.sticky.Set(c.chanID, pt.cfg.Port)
		c.targetPool.signalReady()
		return
	}
	c.deactivatePortLocked(pt.cfg.Port)
	c.mu.Unlock()

	closeConnectTunnel(pt)
}

func (c *connectTargetChannel) releaseConsumed(pt *pooledConnectTunnel) {
	c.mu.Lock()
	c.deactivatePortLocked(pt.cfg.Port)
	c.mu.Unlock()
	closeConnectTunnel(pt)
	c.signalRefill()
}

func (c *connectTargetChannel) releaseFailed(pt *pooledConnectTunnel, cooldown time.Duration) {
	c.mu.Lock()
	c.deactivatePortLocked(pt.cfg.Port)
	c.mu.Unlock()
	if cooldown > 0 {
		c.targetPool.parent.badPorts.MarkFailureFor(pt.cfg.Port, cooldown)
	} else {
		c.targetPool.parent.badPorts.MarkFailure(pt.cfg.Port)
	}
	c.targetPool.forgetPort(pt.cfg.Port)
	c.targetPool.parent.sticky.Delete(c.chanID)
	closeConnectTunnel(pt)
	c.signalRefill()
}

func (p *connectTargetPool) preferredPort(blocked func(int) bool) (int, bool) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()

	bestPort := 0
	bestDur := time.Duration(0)
	found := false
	for port, stat := range p.portStats {
		if blocked != nil && blocked(port) {
			continue
		}
		if !found || stat.ewma < bestDur || (stat.ewma == bestDur && port == p.lastUsedPort) {
			bestPort = port
			bestDur = stat.ewma
			found = true
		}
	}
	return bestPort, found
}

func (p *connectTargetPool) observeSuccess(port int, duration time.Duration) {
	if duration <= 0 {
		return
	}
	p.statsMu.Lock()
	defer p.statsMu.Unlock()

	stat, ok := p.portStats[port]
	if !ok || stat.ewma <= 0 {
		stat.ewma = duration
	} else {
		stat.ewma = (stat.ewma*3 + duration) / 4
	}
	p.portStats[port] = stat
}

func (p *connectTargetPool) forgetPort(port int) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	delete(p.portStats, port)
	if p.lastUsedPort == port {
		p.lastUsedPort = 0
	}
}

func (p *connectTargetPool) markPortUsed(port int) {
	p.statsMu.Lock()
	p.lastUsedPort = port
	p.statsMu.Unlock()
}

func (p *connectTargetPool) shouldPreferCandidate(candidate *pooledConnectTunnel, current *pooledConnectTunnel) bool {
	candidateDur := p.portDuration(candidate.cfg.Port, candidate.warmDuration)
	currentDur := p.portDuration(current.cfg.Port, current.warmDuration)
	if candidateDur != currentDur {
		return candidateDur < currentDur
	}

	p.statsMu.Lock()
	lastUsed := p.lastUsedPort
	p.statsMu.Unlock()
	if candidate.cfg.Port == lastUsed && current.cfg.Port != lastUsed {
		return true
	}
	if candidate.cfg.Port != lastUsed && current.cfg.Port == lastUsed {
		return false
	}
	return candidate.cfg.Port < current.cfg.Port
}

func (p *connectTargetPool) portDuration(port int, fallback time.Duration) time.Duration {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	if stat, ok := p.portStats[port]; ok && stat.ewma > 0 {
		return stat.ewma
	}
	if fallback > 0 {
		return fallback
	}
	return time.Hour
}

func (c *connectTargetChannel) deactivatePortLocked(port int) {
	count := c.activePorts[port]
	if count <= 1 {
		delete(c.activePorts, port)
		return
	}
	c.activePorts[port] = count - 1
}

func closeConnectTunnel(pt *pooledConnectTunnel) {
	_ = pt.conn.Close()
}

func canonicalConnectTarget(target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", fmt.Errorf("empty target")
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		if addrErr, ok := err.(*net.AddrError); ok && addrErr.Err == "missing port in address" {
			return net.JoinHostPort(strings.ToLower(target), "443"), nil
		}
		return "", err
	}
	return net.JoinHostPort(strings.ToLower(host), port), nil
}
