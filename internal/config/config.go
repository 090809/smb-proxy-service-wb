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
	ListenAddr    string
	UpstreamHost  string
	PortMin       int
	PortMax       int
	MaxRetries403 int
	Timeout       time.Duration
	ServiceUser   string
	ServicePass   string

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
