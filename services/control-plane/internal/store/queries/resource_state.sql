-- name: LockUnpreparedResourceState :many
SELECT revision, routing_key, project_id, status, captured_at, trace_parent
FROM resource_state_outbox WHERE payload IS NULL ORDER BY revision LIMIT @lim FOR UPDATE SKIP LOCKED;

-- name: PrepareResourceState :exec
UPDATE resource_state_outbox SET payload = @payload, kafka_partition = @kafka_partition
WHERE revision = @revision AND payload IS NULL;

-- name: LockResourceStatePartition :one
SELECT p.partition FROM resource_state_partitions p
WHERE EXISTS (
    SELECT FROM resource_state_outbox o WHERE o.kafka_partition = p.partition AND o.sent_at IS NULL
      AND NOT EXISTS (SELECT FROM resource_state_outbox prev
        WHERE prev.routing_key = o.routing_key AND prev.revision < o.revision AND prev.sent_at IS NULL)
)
ORDER BY p.checked_at, p.partition LIMIT 1 FOR UPDATE OF p SKIP LOCKED;

-- name: ListResourceStateHeads :many
SELECT o.revision, o.routing_key, o.status, o.payload, o.trace_parent
FROM resource_state_outbox o
WHERE o.kafka_partition = @partition AND o.sent_at IS NULL
  AND NOT EXISTS (SELECT FROM resource_state_outbox prev
    WHERE prev.routing_key = o.routing_key AND prev.revision < o.revision AND prev.sent_at IS NULL)
ORDER BY o.revision LIMIT @lim;

-- name: ConfirmResourceStateLock :exec
UPDATE resource_state_partitions SET checked_at = clock_timestamp() WHERE partition = @partition;

-- name: MarkResourceStateSent :execrows
UPDATE resource_state_outbox SET sent_at = clock_timestamp()
WHERE revision = ANY(@revisions::bigint[]) AND sent_at IS NULL;

-- name: ResourceStateOutboxStats :one
SELECT count(*)::bigint AS pending,
       coalesce(extract(epoch FROM (clock_timestamp() - min(captured_at))), 0)::double precision AS oldest_seconds
FROM resource_state_outbox WHERE sent_at IS NULL;

-- name: PurgeSentResourceStates :execrows
DELETE FROM resource_state_outbox WHERE revision IN (
    SELECT revision FROM resource_state_outbox WHERE sent_at < now() - interval '7 days'
    ORDER BY sent_at LIMIT @lim);
