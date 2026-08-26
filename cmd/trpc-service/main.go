package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:]); err != nil {
		log.Printf("service stopped with error: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("trpc-service", flag.ContinueOnError)
	configPath := flags.String("config", os.Getenv("TRPC_SERVICE_CONFIG"), "optional YAML configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load node configuration: %w", err)
	}

	var adminHandler http.Handler
	if cfg.MySQLDSN != "" {
		configStore, err := tenant.OpenMySQLStore(ctx, cfg.MySQLDSN)
		if err != nil {
			return err
		}
		defer configStore.Close()
		if err := storage.ApplyMigrations(ctx, configStore.DB()); err != nil {
			return fmt.Errorf("apply database migrations: %w", err)
		}
		cache, err := tenant.NewConfigCache(configStore, cfg.ConfigCacheTTL)
		if err != nil {
			return fmt.Errorf("create config cache: %w", err)
		}
		password, err := tenant.ResolveSecret(cfg.AdminPasswordRef)
		if err != nil {
			return fmt.Errorf("resolve admin password: %w", err)
		}
		adminHandler, err = admin.NewHandler(configStore, cache, cfg.AdminUsername, password)
		if err != nil {
			return fmt.Errorf("create Admin API: %w", err)
		}
	}

	server := &http.Server{
		Addr: cfg.ListenAddr,
		Handler: trpcservice.NewHTTPHandler(trpcservice.HTTPOptions{
			AdminHandler: adminHandler,
		}),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("trpc-agent-service %s listening on %s", trpcservice.Version, cfg.ListenAddr)
		errCh <- server.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful HTTP shutdown: %w", err)
	}
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP during shutdown: %w", err)
	}
	return nil
}
