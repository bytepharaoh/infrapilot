-- name: EnsureOrganization :exec
INSERT INTO check_organizations (organization_id) VALUES (@organization_id) ON CONFLICT DO NOTHING;

-- name: LockOrganization :one
SELECT organization_id FROM check_organizations WHERE organization_id = @organization_id FOR UPDATE;

-- name: CountChecks :one
SELECT count(*) FROM checks WHERE organization_id = @organization_id;

-- name: TryIdempotencyLock :one
SELECT pg_try_advisory_xact_lock(hashtextextended(@lock_key::text, 3))::boolean;

-- name: GetIdempotency :one
SELECT fingerprint, check_id FROM check_idempotency
WHERE organization_id = @organization_id AND principal_key = @principal_key AND key = @key
  AND created_at >= now() - interval '24 hours';

-- name: SaveIdempotency :exec
INSERT INTO check_idempotency (organization_id, principal_key, key, fingerprint, check_id)
VALUES (@organization_id, @principal_key, @key, @fingerprint, @check_id)
ON CONFLICT (organization_id, principal_key, key) DO UPDATE
SET fingerprint = excluded.fingerprint, check_id = excluded.check_id, created_at = excluded.created_at;

-- name: CreateCheck :one
INSERT INTO checks (id, organization_id, project_id, resource_id, kind, target, port, interval_seconds,
    timeout_seconds, enabled, max_duration_ms, min_tls_days, next_run_at)
VALUES (@id, @organization_id, @project_id, @resource_id, @kind, @target, @port, @interval_seconds,
    @timeout_seconds, @enabled, sqlc.narg(max_duration_ms), sqlc.narg(min_tls_days),
    now() + sqlc.arg(jitter_ms)::integer * interval '1 millisecond')
RETURNING *;

-- name: GetCheck :one
SELECT * FROM checks WHERE organization_id = @organization_id AND project_id = @project_id AND id = @id;

-- name: ListChecks :many
SELECT * FROM checks WHERE organization_id = @organization_id AND project_id = @project_id AND id > @after_id
ORDER BY id LIMIT @limit_count;

-- name: UpdateCheck :one
UPDATE checks SET resource_id = @resource_id, kind = @kind, target = @target, port = @port,
    interval_seconds = @interval_seconds, timeout_seconds = @timeout_seconds, enabled = @enabled,
    max_duration_ms = sqlc.narg(max_duration_ms), min_tls_days = sqlc.narg(min_tls_days),
    threshold_version = threshold_version + 1, updated_at = now(), lease_epoch = lease_epoch + 1,
    lease_owner = NULL, lease_until = NULL, next_run_at = greatest(next_run_at, now())
WHERE organization_id = @organization_id AND project_id = @project_id AND id = @id
RETURNING *;

-- name: DeleteCheck :execrows
DELETE FROM checks WHERE organization_id = @organization_id AND project_id = @project_id AND id = @id;

-- name: LockDueOrganizations :many
    SELECT org.organization_id
    FROM check_organizations org
    JOIN LATERAL (
        SELECT c.next_run_at
        FROM checks c
        JOIN resource_projection r ON r.organization_id = c.organization_id AND r.resource_id = c.resource_id
          AND r.project_id = c.project_id AND r.active
        JOIN resource_partitions p ON p.kafka_partition = r.kafka_partition AND p.generation = r.generation
          AND p.rebuild_since IS NULL AND p.caught_up_at >= now() - interval '6 days'
        WHERE c.organization_id = org.organization_id AND c.enabled AND c.next_run_at <= now()
          AND (c.lease_until IS NULL OR c.lease_until <= now())
        ORDER BY c.next_run_at LIMIT 1
    ) oldest ON true
    WHERE (SELECT count(*) FROM checks leased WHERE leased.organization_id = org.organization_id
              AND leased.lease_owner IS NOT NULL AND leased.lease_until > now()) < sqlc.arg(tenant_limit)::integer
    ORDER BY oldest.next_run_at, org.organization_id
    LIMIT sqlc.arg(worker_limit)::integer
    FOR UPDATE OF org SKIP LOCKED;

-- name: ClaimChecks :many
WITH due AS MATERIALIZED (
    SELECT d.id, d.next_run_at
    FROM unnest(sqlc.arg(organizations)::uuid[]) AS org(organization_id)
    JOIN LATERAL (
        SELECT c.id, c.next_run_at
        FROM checks c
        JOIN resource_projection r ON r.organization_id = c.organization_id AND r.resource_id = c.resource_id
          AND r.project_id = c.project_id AND r.active
        JOIN resource_partitions p ON p.kafka_partition = r.kafka_partition AND p.generation = r.generation
          AND p.rebuild_since IS NULL AND p.caught_up_at >= now() - interval '6 days'
        WHERE c.organization_id = org.organization_id AND c.enabled AND c.next_run_at <= now()
          AND (c.lease_until IS NULL OR c.lease_until <= now())
        ORDER BY c.next_run_at, c.id
        LIMIT greatest(0, sqlc.arg(tenant_limit)::integer - (
            SELECT count(*) FROM checks leased WHERE leased.organization_id = org.organization_id
              AND leased.lease_owner IS NOT NULL AND leased.lease_until > now()))
        FOR UPDATE OF c SKIP LOCKED
    ) d ON true
    ORDER BY d.next_run_at, d.id LIMIT sqlc.arg(worker_limit)::integer
)
UPDATE checks c SET lease_owner = sqlc.arg(owner)::uuid, lease_epoch = c.lease_epoch + 1,
    lease_until = now() + (c.timeout_seconds + 5) * interval '1 second'
FROM due WHERE c.id = due.id
RETURNING c.*;

-- name: CompleteCheck :execrows
UPDATE checks c SET lease_owner = NULL, lease_until = NULL,
    next_run_at = greatest(c.next_run_at + c.interval_seconds * interval '1 second', now())
      + $1::integer * interval '1 millisecond',
    last_outcome = $2, last_observed_at = $3
WHERE c.id = $4 AND c.organization_id = $5 AND c.enabled
  AND c.next_run_at = $6 AND c.lease_epoch = $7 AND c.lease_owner = $8
  AND c.lease_until > now()
  AND EXISTS (
    SELECT 1 FROM resource_projection r
    JOIN resource_partitions p ON p.kafka_partition = r.kafka_partition AND p.generation = r.generation
      AND p.rebuild_since IS NULL AND p.caught_up_at >= now() - interval '6 days'
    WHERE r.organization_id = c.organization_id AND r.resource_id = c.resource_id
      AND r.project_id = c.project_id AND r.active
  );

-- name: SaveResult :exec
INSERT INTO check_result_outbox (id, routing_key, trace_parent, payload) VALUES (@id, @routing_key, @trace_parent, @payload);

-- name: NextResults :many
SELECT o.* FROM check_result_outbox o WHERE o.sent_at IS NULL
  AND NOT EXISTS (SELECT 1 FROM check_result_outbox prior
    WHERE prior.routing_key = o.routing_key AND prior.sent_at IS NULL AND (prior.created_at, prior.id) < (o.created_at, o.id))
ORDER BY o.created_at, o.id LIMIT 128 FOR UPDATE OF o SKIP LOCKED;

-- name: MarkResultsSent :exec
UPDATE check_result_outbox SET sent_at = now() WHERE id = ANY(sqlc.arg(ids)::uuid[]);

-- name: PendingResults :one
SELECT count(*) AS pending, coalesce(extract(epoch FROM now() - min(created_at))::double precision, 0)::double precision AS oldest_seconds
FROM check_result_outbox WHERE sent_at IS NULL;

-- name: PruneResults :execrows
DELETE FROM check_result_outbox WHERE id IN
  (SELECT id FROM check_result_outbox WHERE sent_at < now() - interval '7 days' ORDER BY sent_at LIMIT 1000 FOR UPDATE SKIP LOCKED);

-- name: PruneIdempotency :execrows
DELETE FROM check_idempotency WHERE (organization_id, principal_key, key) IN
  (SELECT organization_id, principal_key, key FROM check_idempotency
   WHERE created_at < now() - interval '24 hours' ORDER BY created_at LIMIT 1000);
