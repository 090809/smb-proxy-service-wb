package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"port-router/internal/config"
	"port-router/internal/proxy"
)

func BenchmarkServiceE2E_Google(b *testing.B) {
	cfg := benchmarkConfigFromEnv(b)

	creds := make([]proxy.Credential, 0, len(cfg.Creds))
	for _, c := range cfg.Creds {
		creds = append(creds, proxy.Credential{User: c.User, Pass: c.Pass})
	}

	getPool := proxy.NewUpstreamGETPoolWithTotalWarmTarget(
		cfg.UpstreamHost,
		cfg.DialTimeout,
		creds,
		cfg.PortMin,
		cfg.PortMax,
		cfg.BadProxyPenaltyBase,
		cfg.BadProxyPenaltyMax,
		20,
	)
	warmCtx, warmCancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer warmCancel()
	if err := getPool.WaitUntilReady(warmCtx, 20); err != nil {
		b.Fatalf("wait for warmed pool: %v", err)
	}
	connectPool := proxy.NewUpstreamCONNECTPoolWithTotalWarmTarget(
		cfg.UpstreamHost,
		cfg.DialTimeout,
		creds,
		cfg.PortMin,
		cfg.PortMax,
		cfg.StickyPortTTL,
		cfg.BadProxyPenaltyBase,
		cfg.BadProxyPenaltyMax,
		60,
	)
	handler := proxy.NewHandler(proxy.HandlerConfig{
		MaxRetries403: cfg.MaxRetries403,
		Timeout:       cfg.Timeout,
		StickyPortTTL: cfg.StickyPortTTL,
		ServiceUser:   cfg.ServiceUser,
		ServicePass:   cfg.ServicePass,
		GETPool:       getPool,
		CONNECTPool:   connectPool,
		Creds:         proxy.NewCredentialProvider(creds),
	})

	server := httptest.NewServer(handler)
	b.Cleanup(server.Close)

	proxyURL, err := url.Parse(server.URL)
	if err != nil {
		b.Fatalf("parse proxy url: %v", err)
	}
	proxyURL.User = url.UserPassword(cfg.ServiceUser, cfg.ServicePass)

	client := &http.Client{
		Transport: &http.Transport{
			Proxy:             http.ProxyURL(proxyURL),
			ForceAttemptHTTP2: false,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: cfg.Timeout,
	}

	const targetURL = "https://www.google.com/"
	warmReq, err := http.NewRequest(http.MethodGet, targetURL, nil)
	if err != nil {
		b.Fatalf("create warmup request: %v", err)
	}
	warmResp, err := client.Do(warmReq)
	if err != nil {
		b.Fatalf("warmup request failed: %v", err)
	}
	if warmResp.StatusCode < 200 || warmResp.StatusCode >= 400 {
		_ = warmResp.Body.Close()
		b.Fatalf("warmup returned unexpected status: %d", warmResp.StatusCode)
	}
	_, _ = io.Copy(io.Discard, warmResp.Body)
	_ = warmResp.Body.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, err := http.NewRequest(http.MethodGet, targetURL, nil)
		if err != nil {
			b.Fatalf("create request: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			b.Fatalf("request failed: %v", err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 400 {
			_ = resp.Body.Close()
			b.Fatalf("unexpected status: %d", resp.StatusCode)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

func benchmarkConfigFromEnv(tb testing.TB) config.Config {
	tb.Helper()

	root, err := findRepositoryRoot()
	if err != nil {
		tb.Fatalf("find repository root: %v", err)
	}

	envPath := filepath.Join(root, ".env")
	envValues, err := parseDotEnv(envPath)
	if err != nil {
		tb.Fatalf("read %s: %v", envPath, err)
	}
	for key, value := range envValues {
		tb.Setenv(key, value)
	}

	cfg := config.Defaults()
	cfg.ServiceUser = envOrDefault("PROXY_SERVICE_USER", "benchmark-service-user")
	cfg.ServicePass = envOrDefault("PROXY_SERVICE_PASS", "benchmark-service-pass")

	for i := 1; i <= 6; i++ {
		userKey := fmt.Sprintf("PROXY_MARKET_USER_%d", i)
		passKey := fmt.Sprintf("PROXY_MARKET_PASS_%d", i)
		user := strings.TrimSpace(os.Getenv(userKey))
		pass := strings.TrimSpace(os.Getenv(passKey))
		if user == "" || pass == "" {
			tb.Fatalf("missing upstream credentials in .env: %s / %s", userKey, passKey)
		}
		cfg.Creds = append(cfg.Creds, config.Credential{User: user, Pass: pass})
	}

	return cfg
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func findRepositoryRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("repository root not found")
		}
		dir = parent
	}
}

func parseDotEnv(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, rawValue, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("invalid line: %q", line)
		}

		key = strings.TrimSpace(key)
		rawValue = strings.TrimSpace(rawValue)
		value, err := parseEnvValue(rawValue)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", key, err)
		}
		values[key] = value
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func parseEnvValue(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if raw[0] == '"' || raw[0] == '\'' {
		return strconv.Unquote(raw)
	}
	return raw, nil
}
