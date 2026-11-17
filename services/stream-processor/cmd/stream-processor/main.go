// Command stream-processor persists and serves customer telemetry and check results.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/bytepharaoh/infrapilot/pkg/config"
	"github.com/bytepharaoh/infrapilot/pkg/log"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/app"
)

func main() {
	cfg, err := config.Load[app.Config]()
	if err == nil {
		mode := "serve"
		if len(os.Args) > 1 {
			mode = os.Args[1]
		}
		err = app.Run(context.Background(), cfg, mode)
	}
	if err != nil {
		log.Fallback(os.Stderr).Error("stream-processor exited", slog.Any("error", err))
		os.Exit(1)
	}
}
