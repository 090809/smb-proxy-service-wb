package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"port-router/internal/config"
	"port-router/internal/proxy"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	creds := make([]proxy.Credential, 0, len(cfg.Creds))
	for _, c := range cfg.Creds {
		creds = append(creds, proxy.Credential{User: c.User, Pass: c.Pass})
	}
	cp := proxy.NewCredentialProvider(creds)
	getPool := proxy.NewUpstreamGETPool(
		cfg.UpstreamHost,
		cfg.DialTimeout,
		creds,
		cfg.PortMin,
		cfg.PortMax,
		cfg.BadProxyPenaltyBase,
		cfg.BadProxyPenaltyMax,
	)
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
	if len(cfg.ConnectPrewarmTargets) > 0 {
		warmTimeout := cfg.Timeout
		minWarmTimeout := cfg.DialTimeout * time.Duration(len(cfg.ConnectPrewarmTargets)*2)
		if warmTimeout < minWarmTimeout {
			warmTimeout = minWarmTimeout
		}
		warmCtx, warmCancel := context.WithTimeout(context.Background(), warmTimeout)
		defer warmCancel()

		log.Printf("prewarming CONNECT targets: targets=%d ready_per_target=1 timeout=%s", len(cfg.ConnectPrewarmTargets), warmTimeout)
		if err := connectPool.PrewarmTargets(warmCtx, cfg.ConnectPrewarmTargets); err != nil {
			log.Fatal(err)
		}
	}

	h := proxy.NewHandler(proxy.HandlerConfig{
		MaxRetries403: cfg.MaxRetries403,
		Timeout:       cfg.Timeout,
		StickyPortTTL: cfg.StickyPortTTL,
		ServiceUser:   cfg.ServiceUser,
		ServicePass:   cfg.ServicePass,
		GETPool:       getPool,
		CONNECTPool:   connectPool,
		Creds:         cp,
	})

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("listening on %s; upstream=%s ports=%d-%d retries403=%d channels=%d timeout=%s",
		cfg.ListenAddr, cfg.UpstreamHost, cfg.PortMin, cfg.PortMax, cfg.MaxRetries403, len(cfg.Creds), cfg.Timeout)

	log.Fatal(srv.ListenAndServe())
}
