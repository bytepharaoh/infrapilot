-- name: ReadCheckPage :many
WITH page AS MATERIALIZED (
    SELECT scheduled_at, payload FROM check_results
    WHERE organization_id=sqlc.arg(organization_id) AND project_id=sqlc.arg(project_id)
      AND resource_id=sqlc.arg(resource_id) AND check_id=sqlc.arg(check_id)
      AND scheduled_at>=sqlc.arg(start_on) AND scheduled_at<sqlc.arg(end_before)
      AND (sqlc.narg(after_at)::timestamptz IS NULL OR scheduled_at>sqlc.narg(after_at)::timestamptz)
    ORDER BY scheduled_at LIMIT sqlc.arg(page_limit)
), sized AS (
    SELECT scheduled_at, payload, sum(octet_length(payload)) OVER (ORDER BY scheduled_at) AS total_bytes
    FROM page
)
SELECT scheduled_at, (CASE WHEN total_bytes <= 3145728 THEN sized.payload END)::bytea AS payload
FROM sized ORDER BY scheduled_at;

-- name: ReadMetricPage :many
WITH page AS MATERIALIZED (
    SELECT observed_at, sample_id, payload FROM metrics
    WHERE organization_id=sqlc.arg(organization_id) AND project_id=sqlc.arg(project_id)
      AND resource_id=sqlc.arg(resource_id) AND metric_name=sqlc.arg(metric_name)
      AND observed_at>=sqlc.arg(start_on) AND observed_at<sqlc.arg(end_before)
      AND (sqlc.narg(after_at)::timestamptz IS NULL OR (observed_at,sample_id)>(sqlc.narg(after_at)::timestamptz,sqlc.arg(after_id)::bytea))
    ORDER BY observed_at,sample_id LIMIT sqlc.arg(page_limit)
), sized AS (
    SELECT observed_at, sample_id, payload, sum(octet_length(payload)) OVER (ORDER BY observed_at,sample_id) AS total_bytes
    FROM page
)
SELECT observed_at, sample_id, (CASE WHEN total_bytes <= 3145728 THEN sized.payload END)::bytea AS payload
FROM sized ORDER BY observed_at,sample_id;
