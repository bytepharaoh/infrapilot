// Command query-service serves authenticated, tenant-scoped telemetry reads.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/bytepharaoh/infrapilot/pkg/config"
	"github.com/bytepharaoh/infrapilot/pkg/log"
	"github.com/bytepharaoh/infrapilot/services/query-service/internal/app"
)

func main() {
	cfg, err := config.Load[app.Config]()
	if err == nil {
		err = app.Run(context.Background(), cfg)
	}
	if err != nil {
		log.Fallback(os.Stderr).Error("query-service exited", slog.Any("error", err))
		os.Exit(1)
	}
}
