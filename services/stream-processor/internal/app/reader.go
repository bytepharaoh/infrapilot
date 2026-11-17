package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/bytepharaoh/infrapilot/pkg/postgres"
)

// Verify the read pool credentials before accepting requests. Besides checking the
// grants, reject roles that can bypass tenant isolation or select the outbox.
func ensureReaderRole(ctx context.Context, pool *postgres.Pool) error {
	if pool.BypassesRLS() {
		return errors.New("stream reader must not bypass tenant row security")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	err := pool.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var valid bool
		queryErr := tx.QueryRow(ctx, `SELECT coalesce(bool_and(
            has_table_privilege(current_user, tbl, 'SELECT')
            AND NOT has_table_privilege(current_user, tbl, 'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER')
        ), false) AND NOT has_table_privilege(current_user, 'signals_outbox', 'SELECT,INSERT,UPDATE,DELETE')
        FROM unnest(ARRAY['check_results','metrics','check_rollups','metric_rollups']) AS tbl`).Scan(&valid)
		if queryErr != nil {
			return queryErr
		}
		if !valid {
			return errors.New("stream reader role lacks the required SELECT-only grants")
		}
		return nil
	}, postgres.ReadOnly())
	if err != nil {
		return fmt.Errorf("validate stream reader database role: %w", err)
	}
	return nil
}
