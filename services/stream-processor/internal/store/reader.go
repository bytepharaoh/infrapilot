package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/bytepharaoh/infrapilot/pkg/postgres"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/domain"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/store/db"
)

// Reader's pool authenticates separately as the SELECT-only role.
type Reader struct{ Pool *postgres.Pool }

type Window struct {
	domain.Scope
	Start, End time.Time
	AfterAt    time.Time
	AfterID    []byte
	Limit      int32
}

func (w Window) after() pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: w.AfterAt, Valid: !w.AfterAt.IsZero()}
}

func (r Reader) Checks(ctx context.Context, w Window, check uuid.UUID) ([]db.ReadCheckPageRow, error) {
	var rows []db.ReadCheckPageRow
	err := r.Pool.WithTx(ctx, w.OrganizationID, func(ctx context.Context, tx pgx.Tx) error {
		page, queryErr := db.New(tx).ReadCheckPage(ctx, db.ReadCheckPageParams{
			OrganizationID: w.OrganizationID, ProjectID: w.ProjectID, ResourceID: w.ResourceID, CheckID: check,
			StartOn: w.Start, EndBefore: w.End, AfterAt: w.after(), PageLimit: w.Limit,
		})
		rows = page
		return queryErr
	}, postgres.ReadOnly())
	if err != nil {
		return nil, fmt.Errorf("query tenant check results: %w", err)
	}
	return rows, nil
}

func (r Reader) Metrics(ctx context.Context, w Window, name string) ([]db.ReadMetricPageRow, error) {
	var rows []db.ReadMetricPageRow
	err := r.Pool.WithTx(ctx, w.OrganizationID, func(ctx context.Context, tx pgx.Tx) error {
		page, queryErr := db.New(tx).ReadMetricPage(ctx, db.ReadMetricPageParams{
			OrganizationID: w.OrganizationID, ProjectID: w.ProjectID, ResourceID: w.ResourceID, MetricName: name,
			StartOn: w.Start, EndBefore: w.End, AfterAt: w.after(), AfterID: w.AfterID, PageLimit: w.Limit,
		})
		rows = page
		return queryErr
	}, postgres.ReadOnly())
	if err != nil {
		return nil, fmt.Errorf("query tenant metric samples: %w", err)
	}
	return rows, nil
}
