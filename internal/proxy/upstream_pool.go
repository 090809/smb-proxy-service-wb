package proxy

import (
	"bufio"
	"context"
	"errors"
	"log"
	"math"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	upstreamBadStatusCooldown = time.Minute
	upstreamWarmTargetTotal   = 60
	upstreamWarmCapacity      = 2
	upstreamWarmBatchSize     = 10
)

var errUpstreamPoolEmpty = errors.New("upstream pool has no warmed connections")

type UpstreamGETPool struct {
	host         string
	dialTimeout  time.Duration
	selector     *portSelector
	badPorts     *badPortState
	channels     []*upstreamGETChannel
	warmTarget   int
	warmCapacity int
	readySignal  chan struct{}
	next         atomic.Uint64
}

type upstreamGETChannel struct {
	pool   *UpstreamGETPool
	chanID int
	cred   Credential

	ready  chan *pooledUpstreamConn
	refill chan struct{}

	mu          sync.Mutex
	activePorts map[int]int
	dialing     int
}

type pooledUpstreamConn struct {
	cfg             UpstreamConfig
	proxyAuthUser   string
	proxyAuthPass   string
	proxyAuthHeader string
	conn            net.Conn
	reader          *bufio.Reader
	requests        int
	channel         *upstreamGETChannel
}

func NewUpstreamGETPool(host string, dialTimeout time.Duration, creds []Credential, portMin int, portMax int, badProxyPenaltyBase time.Duration, badProxyPenaltyMax time.Duration) *UpstreamGETPool {
	return NewUpstreamGETPoolWithTotalWarmTarget(host, dialTimeout, creds, portMin, portMax, badProxyPenaltyBase, badProxyPenaltyMax, upstreamWarmTargetTotal)
}

func NewUpstreamGETPoolWithWarmTarget(host string, dialTimeout time.Duration, creds []Credential, portMin int, portMax int, badProxyPenaltyBase time.Duration, badProxyPenaltyMax time.Duration, warmTarget int) *UpstreamGETPool {
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
		warmTarget = upstreamWarmCapacity
	}
	warmCapacity := warmTarget
	if warmCapacity < upstreamWarmCapacity {
		warmCapacity = upstreamWarmCapacity
	}

	pool := &UpstreamGETPool{
		host:         host,
		dialTimeout:  defaultDialTimeout(dialTimeout),
		selector:     newPortSelector(portMin, portMax),
		badPorts:     newBadPortState(badProxyPenaltyBase, badProxyPenaltyMax),
		channels:     make([]*upstreamGETChannel, 0, len(creds)),
		warmTarget:   warmTarget,
		warmCapacity: warmCapacity,
		readySignal:  make(chan struct{}, 1),
	}

	for idx, cred := range creds {
		ch := &upstreamGETChannel{
			pool:        pool,
			chanID:      idx,
			cred:        cred,
			ready:       make(chan *pooledUpstreamConn, pool.warmCapacity),
			refill:      make(chan struct{}, 1),
			activePorts: make(map[int]int),
		}
		pool.channels = append(pool.channels, ch)
		go ch.fillLoop()
		ch.signalRefill()
	}

	return pool
}

func NewUpstreamGETPoolWithTotalWarmTarget(host string, dialTimeout time.Duration, creds []Credential, portMin int, portMax int, badProxyPenaltyBase time.Duration, badProxyPenaltyMax time.Duration, totalWarmTarget int) *UpstreamGETPool {
	if totalWarmTarget <= 0 {
		totalWarmTarget = upstreamWarmTargetTotal
	}
	perChannel := int(math.Ceil(float64(totalWarmTarget) / float64(maxInt(len(creds), 1))))
	return NewUpstreamGETPoolWithWarmTarget(host, dialTimeout, creds, portMin, portMax, badProxyPenaltyBase, badProxyPenaltyMax, perChannel)
}

func (p *UpstreamGETPool) Do(ctx context.Context, r *http.Request) (*http.Response, int, int, error) {
	pc, err := p.acquire(ctx)
	if err != nil {
		return nil, 0, 0, err
	}

	resp, err := doGETViaUpstreamConn(ctx, pc, r)
	if err != nil {
		pc.channel.release(pc, false, 0, false)
		return nil, pc.channel.chanID, pc.cfg.Port, err
	}
	return resp, pc.channel.chanID, pc.cfg.Port, nil
}

func (p *UpstreamGETPool) acquire(ctx context.Context) (*pooledUpstreamConn, error) {
	channelCount := len(p.channels)
	if channelCount == 0 {
		return nil, errUpstreamPoolEmpty
	}

	for {
		start := int((p.next.Add(1) - 1) % uint64(channelCount))
		for i := 0; i < channelCount; i++ {
			channel := p.channels[(start+i)%channelCount]
			if pc, ok := channel.tryAcquire(); ok {
				return pc, nil
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.readySignal:
		}
	}
}

func (p *UpstreamGETPool) signalReady() {
	select {
	case p.readySignal <- struct{}{}:
	default:
	}
}

func (p *UpstreamGETPool) ReadyCount() int {
	ready := 0
	for _, channel := range p.channels {
		ready += len(channel.ready)
	}
	return ready
}

func (p *UpstreamGETPool) WaitUntilReady(ctx context.Context, want int) error {
	if want <= 0 {
		return nil
	}

	for {
		if p.ReadyCount() >= want {
			return nil
		}

		for _, channel := range p.channels {
			channel.signalRefill()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.readySignal:
		}
	}
}

func (c *upstreamGETChannel) tryAcquire() (*pooledUpstreamConn, bool) {
	select {
	case pc := <-c.ready:
		c.signalRefill()
		return pc, true
	default:
		c.signalRefill()
		return nil, false
	}
}

func (c *upstreamGETChannel) signalRefill() {
	select {
	case c.refill <- struct{}{}:
	default:
	}
}

func (c *upstreamGETChannel) fillLoop() {
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

func (c *upstreamGETChannel) warmPort(port int) {
	ctx, cancel := context.WithTimeout(context.Background(), c.pool.dialTimeout)
	pc, err := c.dial(ctx, port)
	cancel()
	if err != nil {
		c.finishDialFailure(port, err)
		return
	}
	c.finishDialSuccess(pc)
}

func (c *upstreamGETChannel) reserveFillPort() (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.ready)+c.dialing >= c.pool.warmTarget {
		return 0, false
	}

	port, ok := c.pool.selector.pick(func(port int) bool {
		return c.activePorts[port] > 0 || c.pool.badPorts.IsPenalized(port)
	})
	if !ok || c.pool.badPorts.IsPenalized(port) {
		return 0, false
	}

	c.activePorts[port]++
	c.dialing++
	return port, true
}

func (c *upstreamGETChannel) dial(ctx context.Context, port int) (*pooledUpstreamConn, error) {
	cfg := UpstreamConfig{
		Host:        c.pool.host,
		Port:        port,
		User:        c.cred.User,
		Pass:        c.cred.Pass,
		DialTimeout: c.pool.dialTimeout,
	}
	conn, err := dialUpstreamProxy(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return &pooledUpstreamConn{
		cfg:             cfg,
		proxyAuthUser:   c.cred.User,
		proxyAuthPass:   c.cred.Pass,
		proxyAuthHeader: basicProxyAuthHeader(c.cred.User, c.cred.Pass),
		conn:            conn,
		reader:          bufio.NewReader(conn),
		channel:         c,
	}, nil
}

func (c *upstreamGETChannel) finishDialFailure(port int, err error) {
	c.mu.Lock()
	c.dialing--
	c.deactivatePortLocked(port)
	c.mu.Unlock()

	c.pool.badPorts.MarkFailure(port)
	log.Printf("upstream-prewarm: channel=%d port=%d dial failed: %v", c.chanID+1, port, err)
	c.signalRefill()
}

func (c *upstreamGETChannel) finishDialSuccess(pc *pooledUpstreamConn) {
	c.mu.Lock()
	c.dialing--
	if !c.pool.badPorts.IsPenalized(pc.cfg.Port) && len(c.ready) < cap(c.ready) {
		c.ready <- pc
		c.mu.Unlock()
		c.pool.signalReady()
		return
	}
	c.deactivatePortLocked(pc.cfg.Port)
	c.mu.Unlock()

	closePooledConn(pc)
}

func (c *upstreamGETChannel) release(pc *pooledUpstreamConn, reusable bool, cooldown time.Duration, markSuccess bool) {
	if markSuccess {
		c.pool.badPorts.MarkSuccess(pc.cfg.Port)
	}

	if cooldown > 0 {
		c.pool.badPorts.MarkFailureFor(pc.cfg.Port, cooldown)
		c.closePort(pc.cfg.Port, pc)
		return
	}

	if reusable {
		c.mu.Lock()
		if len(c.ready) < cap(c.ready) {
			c.ready <- pc
			c.mu.Unlock()
			c.pool.signalReady()
			return
		}
		c.deactivatePortLocked(pc.cfg.Port)
		c.mu.Unlock()
		closePooledConn(pc)
		c.signalRefill()
		return
	}

	c.mu.Lock()
	c.deactivatePortLocked(pc.cfg.Port)
	c.mu.Unlock()
	closePooledConn(pc)
	c.signalRefill()
}

func (c *upstreamGETChannel) closePort(port int, current *pooledUpstreamConn) {
	var survivors []*pooledUpstreamConn
	var toClose []*pooledUpstreamConn

	c.mu.Lock()
	c.deactivatePortLocked(port)
	for {
		select {
		case pc := <-c.ready:
			if pc.cfg.Port == port {
				c.deactivatePortLocked(port)
				toClose = append(toClose, pc)
				continue
			}
			survivors = append(survivors, pc)
		default:
			for _, pc := range survivors {
				c.ready <- pc
			}
			c.mu.Unlock()

			closePooledConn(current)
			for _, pc := range toClose {
				closePooledConn(pc)
			}
			c.signalRefill()
			return
		}
	}
}

func (c *upstreamGETChannel) deactivatePortLocked(port int) {
	count := c.activePorts[port]
	if count <= 1 {
		delete(c.activePorts, port)
		return
	}
	c.activePorts[port] = count - 1
}

func closePooledConn(pc *pooledUpstreamConn) {
	_ = pc.conn.Close()
}

func resetUpstreamConnPool() {}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
