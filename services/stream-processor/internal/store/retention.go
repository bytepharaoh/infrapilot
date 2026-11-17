package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/bytepharaoh/infrapilot/pkg/postgres"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/store/db"
)

// Purge trims post-aggregation state and acknowledged outbox entries. Timescale owns raw chunk retention.
func Purge(ctx context.Context, pool *postgres.Pool, logger *slog.Logger) error {
	if !pool.BypassesRLS() {
		return errors.New("stream purge requires the database batch role")
	}
	jobs := []struct {
		table string
		run   func(*db.Queries, context.Context) (int64, error)
	}{
		{"check_rollups", (*db.Queries).PruneCheckRollups},
		{"metric_rollups", (*db.Queries).PruneMetricRollups},
		{"threshold_state", (*db.Queries).PruneThresholdState},
		{"signals_outbox", (*db.Queries).PruneSignals},
	}
	for _, job := range jobs {
		var total int64
		for {
			var count int64
			err := pool.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
				rows, queryErr := job.run(db.New(tx), ctx)
				count = rows
				return queryErr
			})
			if err != nil {
				return fmt.Errorf("prune %s: %w", job.table, err)
			}
			total += count
			if count < 1000 {
				break
			}
		}
		logger.InfoContext(ctx, "stream retention complete", slog.String("table", job.table), slog.Int64("rows_removed", total))
	}
	return nil
}
