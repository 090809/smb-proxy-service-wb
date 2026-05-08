package config

import (
	"flag"
	"fmt"
	"os"
	"time"
)

type Credential struct {
	User string
	Pass string
}

type Config struct {
	ListenAddr              string
	UpstreamHost            string
	PortMin                 int
	PortMax                 int
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

	Creds []Credential // len must be 6
}

func Load() (Config, error) {
	var c Config

	flag.StringVar(&c.ListenAddr, "listen", ":3128", "listen address for clients (1C)")
	flag.StringVar(&c.UpstreamHost, "upstream-host", "pool.proxy.market", "upstream proxy host (no scheme)")
	flag.IntVar(&c.PortMin, "port-min", 10000, "min upstream port (inclusive)")
	flag.IntVar(&c.PortMax, "port-max", 10999, "max upstream port (inclusive)")
	flag.IntVar(&c.MaxRetries403, "retries-403", 2, "how many retries to do after first 403 (GET only)")
	flag.DurationVar(&c.Timeout, "timeout", 30*time.Second, "per-attempt timeout")
	flag.DurationVar(&c.DialTimeout, "dial-timeout", 5*time.Second, "TCP dial timeout to upstream (fast port failover)")
	flag.DurationVar(&c.KeepAliveIdleTimeout, "keepalive-idle-timeout", 90*time.Second, "idle keep-alive connection timeout for upstream GET pool")
	flag.IntVar(&c.KeepAliveMaxIdleConns, "keepalive-max-idle-conns", 1000, "max idle connections across upstream GET pool")
	flag.IntVar(&c.KeepAliveMaxIdlePerHost, "keepalive-max-idle-per-host", 100, "max idle connections per upstream endpoint in GET pool")
	flag.DurationVar(&c.StickyPortTTL, "sticky-port-ttl", 45*time.Second, "sticky upstream port TTL per credential channel; 0 disables sticky")
	flag.DurationVar(&c.BadProxyPenaltyBase, "bad-proxy-penalty-base", 15*time.Second, "base cooldown after a proxy port failure")
	flag.DurationVar(&c.BadProxyPenaltyMax, "bad-proxy-penalty-max", 5*time.Minute, "max cooldown for repeatedly failing proxy ports")
	flag.IntVar(&c.BadProxyPickSamples, "bad-proxy-pick-samples", 8, "how many candidate ports to sample before choosing the least penalized")
	flag.Parse()

	if c.PortMin <= 0 || c.PortMax <= 0 || c.PortMin > c.PortMax {
		return Config{}, fmt.Errorf("invalid port range: %d-%d", c.PortMin, c.PortMax)
	}
	if c.MaxRetries403 < 0 || c.MaxRetries403 > 20 {
		return Config{}, fmt.Errorf("invalid retries-403: %d", c.MaxRetries403)
	}
	if c.Timeout <= 0 || c.Timeout > 10*time.Minute {
		return Config{}, fmt.Errorf("invalid timeout: %s", c.Timeout)
	}
	if c.DialTimeout <= 0 || c.DialTimeout > c.Timeout {
		return Config{}, fmt.Errorf("invalid dial-timeout: %s (must be > 0 and <= timeout)", c.DialTimeout)
	}
	if c.KeepAliveIdleTimeout < 0 || c.KeepAliveIdleTimeout > 10*time.Minute {
		return Config{}, fmt.Errorf("invalid keepalive-idle-timeout: %s", c.KeepAliveIdleTimeout)
	}
	if c.KeepAliveMaxIdleConns < 1 || c.KeepAliveMaxIdleConns > 100000 {
		return Config{}, fmt.Errorf("invalid keepalive-max-idle-conns: %d", c.KeepAliveMaxIdleConns)
	}
	if c.KeepAliveMaxIdlePerHost < 1 || c.KeepAliveMaxIdlePerHost > 100000 {
		return Config{}, fmt.Errorf("invalid keepalive-max-idle-per-host: %d", c.KeepAliveMaxIdlePerHost)
	}
	if c.StickyPortTTL < 0 || c.StickyPortTTL > 10*time.Minute {
		return Config{}, fmt.Errorf("invalid sticky-port-ttl: %s", c.StickyPortTTL)
	}
	if c.BadProxyPenaltyBase <= 0 || c.BadProxyPenaltyBase > 10*time.Minute {
		return Config{}, fmt.Errorf("invalid bad-proxy-penalty-base: %s", c.BadProxyPenaltyBase)
	}
	if c.BadProxyPenaltyMax <= 0 || c.BadProxyPenaltyMax > 30*time.Minute {
		return Config{}, fmt.Errorf("invalid bad-proxy-penalty-max: %s", c.BadProxyPenaltyMax)
	}
	if c.BadProxyPenaltyMax < c.BadProxyPenaltyBase {
		return Config{}, fmt.Errorf("invalid bad-proxy penalties: bad-proxy-penalty-max (%s) must be >= bad-proxy-penalty-base (%s)", c.BadProxyPenaltyMax, c.BadProxyPenaltyBase)
	}
	if c.BadProxyPickSamples < 1 || c.BadProxyPickSamples > 256 {
		return Config{}, fmt.Errorf("invalid bad-proxy-pick-samples: %d", c.BadProxyPickSamples)
	}

	c.ServiceUser = os.Getenv("PROXY_SERVICE_USER")
	c.ServicePass = os.Getenv("PROXY_SERVICE_PASS")
	if c.ServiceUser == "" || c.ServicePass == "" {
		return Config{}, fmt.Errorf("missing service auth: set PROXY_SERVICE_USER and PROXY_SERVICE_PASS")
	}

	// Expect exactly 6 credential pairs in env.
	for i := 1; i <= 6; i++ {
		user := os.Getenv(fmt.Sprintf("PROXY_MARKET_USER_%d", i))
		pass := os.Getenv(fmt.Sprintf("PROXY_MARKET_PASS_%d", i))
		if user == "" || pass == "" {
			return Config{}, fmt.Errorf("missing credentials: set PROXY_MARKET_USER_%d and PROXY_MARKET_PASS_%d", i, i)
		}
		c.Creds = append(c.Creds, Credential{User: user, Pass: pass})
	}
	return c, nil
}
