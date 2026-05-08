package config

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

type Credential struct {
	User string
	Pass string
}

type Config struct {
	ListenAddr            string
	UpstreamHost          string
	PortMin               int
	PortMax               int
	MaxRetries403         int
	Timeout               time.Duration
	DialTimeout           time.Duration
	StickyPortTTL         time.Duration
	BadProxyPenaltyBase   time.Duration
	BadProxyPenaltyMax    time.Duration
	ServiceUser           string
	ServicePass           string
	ConnectPrewarmTargets []string

	Creds []Credential // len must be 6
}

func Defaults() Config {
	return Config{
		ListenAddr:          ":3128",
		UpstreamHost:        "pool.proxy.market",
		PortMin:             10000,
		PortMax:             10999,
		MaxRetries403:       2,
		Timeout:             30 * time.Second,
		DialTimeout:         5 * time.Second,
		StickyPortTTL:       45 * time.Second,
		BadProxyPenaltyBase: 15 * time.Second,
		BadProxyPenaltyMax:  5 * time.Minute,
	}
}

func Load() (Config, error) {
	c := Defaults()
	var rawConnectPrewarmTargets string

	flag.StringVar(&c.ListenAddr, "listen", ":3128", "listen address for clients (1C)")
	flag.StringVar(&c.UpstreamHost, "upstream-host", "pool.proxy.market", "upstream proxy host (no scheme)")
	flag.IntVar(&c.PortMin, "port-min", 10000, "min upstream port (inclusive)")
	flag.IntVar(&c.PortMax, "port-max", 10999, "max upstream port (inclusive)")
	flag.IntVar(&c.MaxRetries403, "retries-403", 2, "how many retries to do after first 403 (GET only)")
	flag.DurationVar(&c.Timeout, "timeout", 30*time.Second, "per-attempt timeout")
	flag.DurationVar(&c.DialTimeout, "dial-timeout", 5*time.Second, "TCP dial timeout to upstream (fast port failover)")
	flag.DurationVar(&c.StickyPortTTL, "sticky-port-ttl", 45*time.Second, "sticky upstream port TTL per credential channel; 0 disables sticky")
	flag.DurationVar(&c.BadProxyPenaltyBase, "bad-proxy-penalty-base", 15*time.Second, "base cooldown after a proxy port failure")
	flag.DurationVar(&c.BadProxyPenaltyMax, "bad-proxy-penalty-max", 5*time.Minute, "max cooldown for repeatedly failing proxy ports")
	flag.StringVar(&rawConnectPrewarmTargets, "connect-prewarm-targets", "", "comma-separated CONNECT targets to prewarm on startup (host[:port], default port 443)")
	flag.Parse()
	c.ConnectPrewarmTargets = splitCommaSeparated(rawConnectPrewarmTargets)

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

func splitCommaSeparated(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}

	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}
