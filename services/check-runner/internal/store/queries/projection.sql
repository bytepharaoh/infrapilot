-- name: LockPartition :one
SELECT * FROM resource_partitions WHERE kafka_partition = @kafka_partition FOR UPDATE;

-- name: StartPartitionRebuild :one
UPDATE resource_partitions SET topic_id = @topic_id, generation = generation + 1,
    next_offset = @next_offset, caught_up_at = NULL, rebuild_since = now()
WHERE kafka_partition = @kafka_partition RETURNING *;

-- name: AdvancePartition :exec
UPDATE resource_partitions SET next_offset = @next_offset
WHERE kafka_partition = @kafka_partition;

-- name: MarkPartitionCaughtUp :exec
UPDATE resource_partitions SET caught_up_at = @sample_time, rebuild_since = NULL
WHERE kafka_partition = @kafka_partition;

-- name: UpsertProjectedResource :exec
INSERT INTO resource_projection (organization_id, resource_id, project_id, kafka_partition, generation, revision, active)
VALUES (@organization_id, @resource_id, @project_id, @kafka_partition, @generation, @revision, @active)
ON CONFLICT (organization_id, resource_id, generation) DO UPDATE
SET project_id = excluded.project_id, revision = excluded.revision, active = excluded.active
WHERE resource_projection.revision < excluded.revision;

-- name: DeleteProjectedResource :exec
DELETE FROM resource_projection WHERE organization_id = @organization_id AND resource_id = @resource_id
  AND generation = @generation AND revision < @revision;

-- name: PruneStaleProjection :execrows
DELETE FROM resource_projection WHERE (organization_id, resource_id, generation) IN (
  SELECT r.organization_id, r.resource_id, r.generation FROM resource_projection r
  JOIN resource_partitions p ON p.kafka_partition = r.kafka_partition AND p.generation > r.generation
  LIMIT 1000);

-- name: HoldPartition :exec
UPDATE resource_partitions SET caught_up_at = NULL, rebuild_since = coalesce(rebuild_since, now())
WHERE kafka_partition = @kafka_partition;
