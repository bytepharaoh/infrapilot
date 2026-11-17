// Command telemetry-ingestion serves the authenticated public OTLP metrics endpoints.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/bytepharaoh/infrapilot/pkg/config"
	"github.com/bytepharaoh/infrapilot/pkg/log"
	"github.com/bytepharaoh/infrapilot/services/telemetry-ingestion/internal/app"
)

func main() {
	if err := run(); err != nil {
		log.Fallback(os.Stderr).Error("telemetry-ingestion exited with error", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load[app.Config]()
	if err != nil {
		return err
	}
	logCfg := log.Config{Level: cfg.Log.Level, Format: cfg.Log.Format, AddSource: cfg.Log.AddSource}
	logger, err := log.New(logCfg, os.Stdout, cfg.ServiceName, cfg.Version, string(cfg.Environment))
	if err != nil {
		return err
	}
	return app.Serve(context.Background(), &cfg, logCfg, logger)
}
