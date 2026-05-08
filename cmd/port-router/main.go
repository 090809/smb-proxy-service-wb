package main

import (
	"log"
	"net/http"
	"time"

	"port-router/internal/config"
	"port-router/internal/picker"
	"port-router/internal/proxy"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	p := picker.New(cfg.PortMin, cfg.PortMax)

	creds := make([]proxy.Credential, 0, len(cfg.Creds))
	for _, c := range cfg.Creds {
		creds = append(creds, proxy.Credential{User: c.User, Pass: c.Pass})
	}
	cp := proxy.NewCredentialProvider(creds)

	h := proxy.NewHandler(proxy.HandlerConfig{
		UpstreamHost:            cfg.UpstreamHost,
		Picker:                  p,
		MaxRetries403:           cfg.MaxRetries403,
		Timeout:                 cfg.Timeout,
		DialTimeout:             cfg.DialTimeout,
		KeepAliveIdleTimeout:    cfg.KeepAliveIdleTimeout,
		KeepAliveMaxIdleConns:   cfg.KeepAliveMaxIdleConns,
		KeepAliveMaxIdlePerHost: cfg.KeepAliveMaxIdlePerHost,
		StickyPortTTL:           cfg.StickyPortTTL,
		BadProxyPenaltyBase:     cfg.BadProxyPenaltyBase,
		BadProxyPenaltyMax:      cfg.BadProxyPenaltyMax,
		BadProxyPickSamples:     cfg.BadProxyPickSamples,
		ServiceUser:             cfg.ServiceUser,
		ServicePass:             cfg.ServicePass,
		Creds:                   cp,
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
