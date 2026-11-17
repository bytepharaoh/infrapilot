// Package store is the check-runner's PostgreSQL transaction boundary.
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	checksv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/checks/v1"
	"github.com/bytepharaoh/infrapilot/pkg/envelope"
	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/pkg/postgres"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/domain"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/store/db"
)

const (
	// DefinitionLimit caps check definitions across all projects in an organization.
	DefinitionLimit = 10000
	// ResultsTopic carries committed probe measurements.
	ResultsTopic = "check.results.v1"
)

var (
	ErrDefinitionLimit = errors.New("organization check limit reached")
	ErrKeyBusy         = errors.New("idempotent create is already in progress")
	ErrKeyReused       = errors.New("idempotency key already used for different settings")
)

// Store keeps tenant request transactions separate from cross-tenant scheduling jobs.
type Store struct {
	app, batch *postgres.Pool
}

// New validates the two pool roles before they reach any queries.
func New(app, batch *postgres.Pool) (*Store, error) {
	if app.BypassesRLS() || !batch.BypassesRLS() {
		return nil, errors.New("check store requires separate tenant and scheduling roles")
	}
	return &Store{app: app, batch: batch}, nil
}

// Scope names a validated tenant and project.
type Scope struct{ Organization, Project uuid.UUID }

// Idempotency is the validated caller and retry key for a create request.
type Idempotency struct {
	Principal string
	Key       string
}

// Create enforces the tenant quota and persists the retry key in the same transaction.
func (s *Store) Create(ctx context.Context, scope Scope, def domain.Definition, idem Idempotency) (db.Check, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return db.Check{}, fmt.Errorf("generate check ID: %w", err)
	}
	var check db.Check
	err = s.app.WithTx(ctx, scope.Organization, func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		var idemParams db.SaveIdempotencyParams
		if idem.Key != "" {
			existing, params, replayErr := existingCreate(ctx, q, scope, idem, def, id)
			if replayErr != nil {
				return replayErr
			}
			if existing.ID != uuid.Nil {
				check = existing
				return nil
			}
			idemParams = params
		}
		if err := q.EnsureOrganization(ctx, scope.Organization); err != nil {
			return fmt.Errorf("initialize check quota: %w", err)
		}
		if _, err := q.LockOrganization(ctx, scope.Organization); err != nil {
			return fmt.Errorf("lock check quota: %w", err)
		}
		count, countErr := q.CountChecks(ctx, scope.Organization)
		if countErr != nil {
			return fmt.Errorf("read check quota: %w", countErr)
		}
		if count >= DefinitionLimit {
			return ErrDefinitionLimit
		}
		var createErr error
		check, createErr = q.CreateCheck(ctx, db.CreateCheckParams{
			ID: id, OrganizationID: scope.Organization, ProjectID: scope.Project, ResourceID: def.Resource,
			Kind: int16(def.Kind), Target: def.Target, Port: def.Port, IntervalSeconds: def.IntervalSeconds,
			TimeoutSeconds: def.TimeoutSeconds, Enabled: def.Enabled, MaxDurationMs: nullableDuration(def), MinTlsDays: nullableTLSDays(def),
			JitterMs: domain.JitterMS(id),
		})
		if createErr != nil {
			return fmt.Errorf("insert check: %w", createErr)
		}
		if idem.Key != "" {
			if err := q.SaveIdempotency(ctx, idemParams); err != nil {
				return fmt.Errorf("save check idempotency key: %w", err)
			}
		}
		return nil
	})
	return check, err
}

func existingCreate(ctx context.Context, q *db.Queries, scope Scope, idem Idempotency, def domain.Definition, id uuid.UUID) (db.Check, db.SaveIdempotencyParams, error) {
	principal := sha256.Sum256([]byte(idem.Principal))
	ok, err := q.TryIdempotencyLock(ctx, scope.Organization.String()+":"+hex.EncodeToString(principal[:])+":"+idem.Key)
	if err != nil {
		return db.Check{}, db.SaveIdempotencyParams{}, fmt.Errorf("lock check idempotency key: %w", err)
	}
	if !ok {
		return db.Check{}, db.SaveIdempotencyParams{}, ErrKeyBusy
	}
	encoded, err := json.Marshal(def)
	if err != nil {
		return db.Check{}, db.SaveIdempotencyParams{}, fmt.Errorf("encode check definition: %w", err)
	}
	fingerprint := sha256.Sum256(append(scope.Project[:], encoded...))
	params := db.SaveIdempotencyParams{OrganizationID: scope.Organization, PrincipalKey: principal[:], Key: idem.Key,
		Fingerprint: fingerprint[:], CheckID: id}
	prior, err := q.GetIdempotency(ctx, db.GetIdempotencyParams{OrganizationID: scope.Organization, PrincipalKey: principal[:], Key: idem.Key})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.Check{}, params, nil
	}
	if err != nil {
		return db.Check{}, params, fmt.Errorf("read check idempotency key: %w", err)
	}
	if !bytes.Equal(prior.Fingerprint, fingerprint[:]) {
		return db.Check{}, params, ErrKeyReused
	}
	existing, err := q.GetCheck(ctx, db.GetCheckParams{OrganizationID: scope.Organization, ProjectID: scope.Project, ID: prior.CheckID})
	return existing, params, err
}

// Get sees only checks in the authorized project and the transaction's tenant.
func (s *Store) Get(ctx context.Context, scope Scope, id uuid.UUID) (db.Check, error) {
	var row db.Check
	err := s.app.WithTx(ctx, scope.Organization, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		row, err = db.New(tx).GetCheck(ctx, db.GetCheckParams{OrganizationID: scope.Organization, ProjectID: scope.Project, ID: id})
		return err
	}, postgres.ReadOnly())
	return row, err
}

// List returns a bounded ascending ID page.
func (s *Store) List(ctx context.Context, scope Scope, after uuid.UUID, limit int32) ([]db.Check, error) {
	var rows []db.Check
	err := s.app.WithTx(ctx, scope.Organization, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		rows, err = db.New(tx).ListChecks(ctx, db.ListChecksParams{OrganizationID: scope.Organization, ProjectID: scope.Project,
			AfterID: after, LimitCount: limit})
		return err
	}, postgres.ReadOnly())
	return rows, err
}

// Update replaces a definition and fences its in-flight probe, if any.
func (s *Store) Update(ctx context.Context, scope Scope, id uuid.UUID, def domain.Definition) (db.Check, error) {
	var row db.Check
	err := s.app.WithTx(ctx, scope.Organization, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		row, err = db.New(tx).UpdateCheck(ctx, db.UpdateCheckParams{OrganizationID: scope.Organization, ProjectID: scope.Project,
			ID: id, ResourceID: def.Resource, Kind: int16(def.Kind), Target: def.Target, Port: def.Port,
			IntervalSeconds: def.IntervalSeconds, TimeoutSeconds: def.TimeoutSeconds, Enabled: def.Enabled,
			MaxDurationMs: nullableDuration(def), MinTlsDays: nullableTLSDays(def)})
		return err
	})
	return row, err
}

// Delete removes a definition and makes its lease ineligible to finish.
func (s *Store) Delete(ctx context.Context, scope Scope, id uuid.UUID) error {
	return s.app.WithTx(ctx, scope.Organization, func(ctx context.Context, tx pgx.Tx) error {
		n, err := db.New(tx).DeleteCheck(ctx, db.DeleteCheckParams{OrganizationID: scope.Organization, ProjectID: scope.Project, ID: id})
		if n == 0 && err == nil {
			return pgx.ErrNoRows
		}
		return err
	})
}

// Definition converts a persisted check into its immutable execution input.
func Definition(row db.Check) domain.Definition {
	d := domain.Definition{Resource: row.ResourceID, Kind: domain.Kind(row.Kind), Target: row.Target, Port: row.Port,
		IntervalSeconds: row.IntervalSeconds, TimeoutSeconds: row.TimeoutSeconds, Enabled: row.Enabled}
	if row.MaxDurationMs.Valid {
		d.MaxDurationMS = &row.MaxDurationMs.Float64
	}
	if row.MinTlsDays.Valid {
		d.MinTLSDays = &row.MinTlsDays.Int32
	}
	return d
}

func nullableDuration(def domain.Definition) pgtype.Float8 {
	if def.MaxDurationMS == nil {
		return pgtype.Float8{}
	}
	return pgtype.Float8{Float64: *def.MaxDurationMS, Valid: true}
}

func nullableTLSDays(def domain.Definition) pgtype.Int4 {
	if def.MinTLSDays == nil {
		return pgtype.Int4{}
	}
	return pgtype.Int4{Int32: *def.MinTLSDays, Valid: true}
}

// Claim selects due organizations, locks their quota rows, and then counts
// outstanding leases in a fresh statement snapshot before assigning new leases.
func (s *Store) Claim(ctx context.Context, owner uuid.UUID, free, tenantLimit int32) ([]db.Check, error) {
	var checks []db.Check
	err := s.batch.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		orgs, err := q.LockDueOrganizations(ctx, db.LockDueOrganizationsParams{WorkerLimit: free, TenantLimit: tenantLimit})
		if err != nil || len(orgs) == 0 {
			return err
		}
		var claimErr error
		checks, claimErr = q.ClaimChecks(ctx, db.ClaimChecksParams{Owner: owner, Organizations: orgs, WorkerLimit: free, TenantLimit: tenantLimit})
		return claimErr
	})
	return checks, err
}

// Complete commits both the schedule transition and the serialized result for
// this exact lease. A stale holder cannot write to either table.
func (s *Store) Complete(ctx context.Context, row db.Check, measurement domain.Measurement) (bool, error) {
	if row.LeaseOwner == nil || row.LeaseEpoch <= 0 || row.ThresholdVersion <= 0 {
		return false, errors.New("result requires a valid check lease")
	}
	outcome := checksv1.CheckOutcome_CHECK_OUTCOME_FAILURE
	if measurement.Success {
		outcome = checksv1.CheckOutcome_CHECK_OUTCOME_SUCCESS
	}
	thresholds := &checksv1.Thresholds{}
	if row.MaxDurationMs.Valid {
		thresholds.MaxDurationMs = &row.MaxDurationMs.Float64
	}
	if row.MinTlsDays.Valid && row.MinTlsDays.Int32 > 0 {
		days := uint32(row.MinTlsDays.Int32)
		thresholds.MinTlsDays = &days
	}
	event, err := envelope.New(ctx, envelope.Params{Type: "checks.result.observed", SchemaVersion: 1,
		OrganizationID: row.OrganizationID.String(), SubjectID: row.ID.String(), Producer: "check-runner",
		OccurredAt: measurement.ObservedAt, Payload: &checksv1.CheckResult{
			ProjectId: row.ProjectID.String(), ResourceId: row.ResourceID.String(), CheckId: row.ID.String(),
			ScheduledAt: timestamppb.New(row.NextRunAt), ObservedAt: timestamppb.New(measurement.ObservedAt),
			LeaseEpoch: uint64(row.LeaseEpoch), ThresholdVersion: uint64(row.ThresholdVersion), Outcome: outcome,
			DurationMs: measurement.DurationMS, Detail: measurement.Detail, Thresholds: thresholds, TlsDaysRemaining: measurement.TLSDays,
		}})
	if err != nil {
		return false, err
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return false, fmt.Errorf("marshal check result: %w", err)
	}
	id, err := uuid.Parse(event.GetEventId())
	if err != nil {
		return false, fmt.Errorf("parse check result event ID: %w", err)
	}
	var changed int64
	err = s.batch.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		var err error
		changed, err = q.CompleteCheck(ctx, db.CompleteCheckParams{ID: row.ID, OrganizationID: row.OrganizationID,
			NextRunAt: row.NextRunAt, LeaseEpoch: row.LeaseEpoch, LeaseOwner: row.LeaseOwner,
			Column1: domain.JitterMS(row.ID), LastOutcome: pgtype.Int2{Int16: int16(outcome), Valid: true}, LastObservedAt: &measurement.ObservedAt})
		if err != nil || changed == 0 {
			return err
		}
		return q.SaveResult(ctx, db.SaveResultParams{ID: id, RoutingKey: row.OrganizationID.String() + "/" + row.ID.String(),
			TraceParent: event.GetTraceParent(), Payload: payload})
	})
	return changed == 1 && err == nil, err
}

// PublishResults dispatches one locked batch and marks it only after broker acknowledgment.
func (s *Store) PublishResults(ctx context.Context, producer *kgo.Client) (int, error) {
	var count int
	err := s.batch.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		rows, err := q.NextResults(ctx)
		if err != nil || len(rows) == 0 {
			return err
		}
		ids := make([]uuid.UUID, 0, len(rows))
		records := make([]*kgo.Record, 0, len(rows))
		for _, row := range rows {
			records = append(records, &kgo.Record{Topic: ResultsTopic, Key: []byte(row.RoutingKey), Value: row.Payload,
				Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte(row.TraceParent)}}})
			ids = append(ids, row.ID)
		}
		if err := kafka.Publish(ctx, producer, records...); err != nil {
			return err
		}
		count = len(rows)
		return q.MarkResultsSent(ctx, ids)
	})
	return count, err
}

// PendingResults reports queue size and oldest unacknowledged result age.
func (s *Store) PendingResults(ctx context.Context) (db.PendingResultsRow, error) {
	var pending db.PendingResultsRow
	err := s.batch.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		pending, err = db.New(tx).PendingResults(ctx)
		return err
	}, postgres.ReadOnly())
	return pending, err
}

// Prune removes expired data in independent bounded batches. Pending results are preserved.
func (s *Store) Prune(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, remove := range []func(*db.Queries, context.Context) (int64, error){
		(*db.Queries).PruneResults, (*db.Queries).PruneIdempotency, (*db.Queries).PruneStaleProjection,
	} {
		for {
			var changed int64
			err := s.batch.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
				var err error
				changed, err = remove(db.New(tx), ctx)
				return err
			})
			if err != nil {
				return err
			}
			if changed < 1000 {
				break
			}
		}
	}
	return nil
}
