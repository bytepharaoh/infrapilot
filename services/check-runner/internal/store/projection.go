package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/store/db"
)

const projectionExpiry = 6 * 24 * time.Hour

// ErrProjectionChanged asks a reader to resample the topic and persisted cursor.
var ErrProjectionChanged = errors.New("resource projection generation changed")

// PreparePartition chooses a resumable offset or starts a new empty generation.
func (s *Store) PreparePartition(ctx context.Context, partition int32, topic string, first, end int64) (db.ResourcePartition, error) {
	var state db.ResourcePartition
	err := s.app.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		var err error
		state, err = q.LockPartition(ctx, partition)
		if err != nil {
			return err
		}
		var now time.Time
		if clockErr := tx.QueryRow(ctx, "SELECT now()").Scan(&now); clockErr != nil {
			return clockErr
		}
		fresh := state.CaughtUpAt != nil && now.Sub(*state.CaughtUpAt) < projectionExpiry
		building := state.RebuildSince != nil && now.Sub(*state.RebuildSince) < projectionExpiry
		if state.TopicID != topic || state.NextOffset < first || state.NextOffset > end || (!fresh && !building) {
			state, err = q.StartPartitionRebuild(ctx, db.StartPartitionRebuildParams{KafkaPartition: partition,
				TopicID: topic, NextOffset: first})
		}
		return err
	})
	return state, err
}

// ProjectedChange is a validated resource envelope bound to its Kafka key and offset.
type ProjectedChange struct {
	Organization, Resource, Project uuid.UUID
	Revision                        int64
	Active, Removed                 bool
}

// ApplyProjection advances the durable cursor and the tenant row together. Each
// replica can retry the same record, but only the first transaction has an effect.
func (s *Store) ApplyProjection(ctx context.Context, state db.ResourcePartition, offset int64, change *ProjectedChange) error {
	apply := func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		current, err := q.LockPartition(ctx, state.KafkaPartition)
		if err != nil {
			return err
		}
		if current.Generation != state.Generation || current.TopicID != state.TopicID {
			return ErrProjectionChanged
		}
		if current.NextOffset > offset {
			return nil
		}
		if change != nil {
			if change.Removed {
				err = q.DeleteProjectedResource(ctx, db.DeleteProjectedResourceParams{OrganizationID: change.Organization,
					ResourceID: change.Resource, Generation: state.Generation, Revision: change.Revision})
			} else {
				err = q.UpsertProjectedResource(ctx, db.UpsertProjectedResourceParams{OrganizationID: change.Organization,
					ResourceID: change.Resource, ProjectID: change.Project, KafkaPartition: state.KafkaPartition,
					Generation: state.Generation, Revision: change.Revision, Active: change.Active})
			}
			if err != nil {
				return err
			}
		}
		return q.AdvancePartition(ctx, db.AdvancePartitionParams{KafkaPartition: state.KafkaPartition, NextOffset: offset + 1})
	}
	if change == nil {
		return s.app.WithSystemTx(ctx, apply)
	}
	return s.app.WithTx(ctx, change.Organization, apply)
}

// ConfirmProjection enables the generation when its durable offset reached the
// sampled read-committed end. A failed check leaves the generation unavailable.
func (s *Store) ConfirmProjection(ctx context.Context, state db.ResourcePartition, end int64, sampled time.Time) error {
	return s.app.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		current, err := q.LockPartition(ctx, state.KafkaPartition)
		if err != nil {
			return err
		}
		if current.Generation != state.Generation || current.TopicID != state.TopicID {
			return ErrProjectionChanged
		}
		if current.NextOffset < end || (current.CaughtUpAt != nil && !sampled.After(*current.CaughtUpAt)) {
			return nil
		}
		var now time.Time
		if clockErr := tx.QueryRow(ctx, "SELECT now()").Scan(&now); clockErr != nil {
			return clockErr
		}
		if current.RebuildSince != nil && now.Sub(*current.RebuildSince) >= projectionExpiry {
			return ErrProjectionChanged
		}
		return q.MarkPartitionCaughtUp(ctx, db.MarkPartitionCaughtUpParams{KafkaPartition: state.KafkaPartition, SampleTime: &sampled})
	})
}

// HoldProjection disables new checks when the next event cannot be safely interpreted.
func (s *Store) HoldProjection(ctx context.Context, state db.ResourcePartition, offset int64) error {
	return s.app.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		current, err := q.LockPartition(ctx, state.KafkaPartition)
		if err != nil || current.NextOffset > offset || current.Generation != state.Generation {
			return err
		}
		return q.HoldPartition(ctx, state.KafkaPartition)
	})
}

// Project serializes one bounded refresh round without requiring a designated
// primary process; a replica that dies releases the transaction-scoped lock.
func (s *Store) Project(ctx context.Context, refresh func(context.Context) error) error {
	return s.app.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var owner bool
		if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock(33103)").Scan(&owner); err != nil || !owner {
			return err
		}
		return refresh(ctx)
	})
}
