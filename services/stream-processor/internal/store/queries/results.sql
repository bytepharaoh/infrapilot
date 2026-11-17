-- name: LockCheck :exec
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(organization_id)::uuid::text || '/' || sqlc.arg(check_id)::uuid::text, 18203));

-- name: PriorCheck :one
SELECT lease_epoch, succeeded, duration_ms FROM check_results
WHERE organization_id = $1 AND check_id = $2 AND scheduled_at = $3;

-- name: SaveCheck :exec
INSERT INTO check_results (organization_id, project_id, resource_id, check_id, scheduled_at, observed_at, lease_epoch, succeeded, duration_ms, payload)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (organization_id, check_id, scheduled_at) DO UPDATE
SET observed_at=EXCLUDED.observed_at, lease_epoch=EXCLUDED.lease_epoch, succeeded=EXCLUDED.succeeded,
    duration_ms=EXCLUDED.duration_ms, payload=EXCLUDED.payload;

-- name: UpdateCheckRollup :exec
INSERT INTO check_rollups (organization_id, project_id, resource_id, check_id, bucket_start, result_count, failure_count, duration_sum_ms)
VALUES (sqlc.arg(organization_id), sqlc.arg(project_id), sqlc.arg(resource_id), sqlc.arg(check_id),
        date_bin(interval '5 minutes', sqlc.arg(scheduled_at)::timestamptz, '1970-01-01T00:00:00Z'),
        sqlc.arg(result_count), sqlc.arg(failure_count), sqlc.arg(duration_delta_ms)::float8::numeric)
ON CONFLICT (organization_id, check_id, bucket_start) DO UPDATE
SET result_count=check_rollups.result_count+EXCLUDED.result_count,
    failure_count=check_rollups.failure_count+EXCLUDED.failure_count,
    duration_sum_ms=check_rollups.duration_sum_ms+EXCLUDED.duration_sum_ms;

-- name: SaveMetric :batchexec
WITH written AS (
    INSERT INTO metrics (organization_id, project_id, resource_id, metric_name, observed_at, series_id, sample_id, scalar_value, payload)
    VALUES (sqlc.arg(organization_id), sqlc.arg(project_id), sqlc.arg(resource_id), sqlc.arg(metric_name), sqlc.arg(observed_at),
            sqlc.arg(series_id), sqlc.arg(sample_id), sqlc.narg(scalar_value), sqlc.arg(payload))
    ON CONFLICT DO NOTHING
    RETURNING organization_id, project_id, resource_id, metric_name, series_id, observed_at, scalar_value
)
INSERT INTO metric_rollups (organization_id, project_id, resource_id, metric_name, series_id, step_seconds, bucket_start,
    sample_count, scalar_count, scalar_sum, scalar_min, scalar_max)
SELECT written.organization_id, written.project_id, written.resource_id, written.metric_name, written.series_id, steps.seconds,
       date_bin(make_interval(secs => steps.seconds), written.observed_at, '1970-01-01T00:00:00Z'),
       1, CASE WHEN written.scalar_value IS NOT NULL THEN 1 ELSE 0 END, coalesce(written.scalar_value::numeric, 0), written.scalar_value, written.scalar_value
FROM written CROSS JOIN (VALUES (60::smallint), (3600::smallint)) AS steps(seconds)
ON CONFLICT (organization_id, resource_id, series_id, step_seconds, bucket_start) DO UPDATE
SET sample_count=metric_rollups.sample_count+EXCLUDED.sample_count,
    scalar_count=metric_rollups.scalar_count+EXCLUDED.scalar_count,
    scalar_sum=metric_rollups.scalar_sum+EXCLUDED.scalar_sum,
    scalar_min=least(metric_rollups.scalar_min,EXCLUDED.scalar_min),
    scalar_max=greatest(metric_rollups.scalar_max,EXCLUDED.scalar_max);

-- name: GetThresholdState :one
SELECT scheduled_at, lease_epoch, firing FROM threshold_state
WHERE organization_id = $1 AND check_id = $2 AND threshold_kind = $3;

-- name: SaveThresholdState :exec
INSERT INTO threshold_state (organization_id, check_id, threshold_kind, scheduled_at, lease_epoch, firing)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (organization_id, check_id, threshold_kind) DO UPDATE
SET scheduled_at=EXCLUDED.scheduled_at, lease_epoch=EXCLUDED.lease_epoch, firing=EXCLUDED.firing;
