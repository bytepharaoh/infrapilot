-- name: EnqueueSignal :exec
INSERT INTO signals_outbox (id, routing_key, payload, trace_parent) VALUES ($1,$2,$3,$4);

-- name: ClaimSignals :many
SELECT id, routing_key, payload, trace_parent FROM signals_outbox o
WHERE sent_at IS NULL
  AND NOT EXISTS (SELECT 1 FROM signals_outbox predecessor
    WHERE predecessor.routing_key=o.routing_key AND predecessor.sent_at IS NULL
      AND (predecessor.created_at,predecessor.id)<(o.created_at,o.id))
ORDER BY created_at,id LIMIT $1 FOR UPDATE SKIP LOCKED;

-- name: MarkSignalSent :exec
UPDATE signals_outbox SET sent_at=clock_timestamp() WHERE id=$1;

-- name: SignalBacklog :one
SELECT count(*)::bigint AS messages,
  coalesce(extract(epoch FROM clock_timestamp()-min(created_at))::float8,0)::float8 AS oldest_seconds
FROM signals_outbox WHERE sent_at IS NULL;

-- name: PruneCheckRollups :execrows
DELETE FROM check_rollups WHERE ctid IN (
    SELECT ctid FROM check_rollups WHERE bucket_start<now()-interval '90 days' LIMIT 1000
);

-- name: PruneMetricRollups :execrows
DELETE FROM metric_rollups WHERE ctid IN (
    SELECT ctid FROM metric_rollups WHERE step_seconds=60 AND bucket_start<now()-interval '30 days'
    UNION ALL
    SELECT ctid FROM metric_rollups WHERE step_seconds=3600 AND bucket_start<now()-interval '400 days'
    LIMIT 1000
);

-- name: PruneThresholdState :execrows
DELETE FROM threshold_state WHERE ctid IN (
    SELECT ctid FROM threshold_state WHERE scheduled_at<now()-interval '90 days' LIMIT 1000
);

-- name: PruneSignals :execrows
DELETE FROM signals_outbox WHERE ctid IN (
    SELECT ctid FROM signals_outbox WHERE sent_at<now()-interval '7 days' LIMIT 1000
);
