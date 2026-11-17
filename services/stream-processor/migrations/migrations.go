// Package migrations installs the stream processor's owned TimescaleDB storage.
package migrations

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/bytepharaoh/infrapilot/pkg/postgres"
)

//go:embed *.sql
var migrations embed.FS

// Migrate applies the migrations as the database's owner role.
func Migrate(ctx context.Context, url string, logger *slog.Logger) error {
	db, err := postgres.OpenSQL(postgres.MigrationConfig(postgres.Config{URL: url, ApplicationName: "stream-processor-migrate"}))
	if err != nil {
		return fmt.Errorf("open migration pool: %w", err)
	}
	defer func() { _ = db.Close() }()
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("create migration lock: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations, goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}
	for attempt := range 5 {
		results, runErr := provider.Up(ctx)
		for _, result := range results {
			if result.Error == nil {
				logger.InfoContext(ctx, "stream migration applied", slog.Int64("version", result.Source.Version), slog.Duration("duration", result.Duration))
			}
		}
		if runErr == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(runErr, &pgErr) || pgErr.Code != "55P03" || attempt == 4 {
			return fmt.Errorf("apply stream storage migrations: %w", runErr)
		}
		timer := time.NewTimer(time.Second << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("wait to retry migration: %w", ctx.Err())
		case <-timer.C:
		}
	}
	panic("unreachable migration retry state")
}
