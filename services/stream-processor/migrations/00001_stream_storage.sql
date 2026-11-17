-- +goose Up

CREATE TABLE check_results (
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    check_id uuid NOT NULL,
    scheduled_at timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    lease_epoch bigint NOT NULL CHECK (lease_epoch > 0),
    succeeded boolean NOT NULL,
    duration_ms double precision NOT NULL CHECK (duration_ms >= 0 AND duration_ms < 'Infinity'::float8),
    payload bytea COMPRESSION lz4 NOT NULL,
    PRIMARY KEY (organization_id, check_id, scheduled_at)
);
SELECT create_hypertable('check_results', by_range('scheduled_at', interval '1 day'), create_default_indexes => false);
CREATE INDEX check_results_read_idx ON check_results (organization_id, project_id, resource_id, check_id, scheduled_at);

CREATE TABLE metrics (
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    metric_name text NOT NULL CHECK (length(metric_name) BETWEEN 1 AND 255),
    observed_at timestamptz NOT NULL,
    series_id bytea NOT NULL CHECK (octet_length(series_id) = 32),
    sample_id bytea NOT NULL CHECK (octet_length(sample_id) = 32),
    scalar_value double precision CHECK (scalar_value > '-Infinity'::float8 AND scalar_value < 'Infinity'::float8),
    payload bytea COMPRESSION lz4 NOT NULL,
    PRIMARY KEY (organization_id, resource_id, observed_at, sample_id)
);
SELECT create_hypertable('metrics', by_range('observed_at', interval '1 day'), create_default_indexes => false);
CREATE INDEX metrics_read_idx ON metrics (organization_id, project_id, resource_id, metric_name, observed_at, sample_id);

CREATE TABLE check_rollups (
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    check_id uuid NOT NULL,
    bucket_start timestamptz NOT NULL,
    result_count bigint NOT NULL,
    failure_count bigint NOT NULL,
    duration_sum_ms numeric NOT NULL,
    PRIMARY KEY (organization_id, check_id, bucket_start)
);
CREATE INDEX check_rollups_retention_idx ON check_rollups (bucket_start);

CREATE TABLE metric_rollups (
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    metric_name text NOT NULL,
    series_id bytea NOT NULL,
    step_seconds smallint NOT NULL CHECK (step_seconds IN (60, 3600)),
    bucket_start timestamptz NOT NULL,
    sample_count bigint NOT NULL,
    scalar_count bigint NOT NULL,
    scalar_sum numeric NOT NULL,
    scalar_min double precision,
    scalar_max double precision,
    PRIMARY KEY (organization_id, resource_id, series_id, step_seconds, bucket_start)
);
CREATE INDEX metric_rollups_retention_idx ON metric_rollups (step_seconds, bucket_start);

CREATE TABLE threshold_state (
    organization_id uuid NOT NULL,
    check_id uuid NOT NULL,
    threshold_kind text NOT NULL CHECK (threshold_kind IN ('duration_ms', 'tls_days_remaining')),
    scheduled_at timestamptz NOT NULL,
    lease_epoch bigint NOT NULL CHECK (lease_epoch > 0),
    firing boolean NOT NULL,
    PRIMARY KEY (organization_id, check_id, threshold_kind)
);
CREATE INDEX threshold_state_retention_idx ON threshold_state (scheduled_at);

CREATE TABLE signals_outbox (
    id uuid NOT NULL PRIMARY KEY,
    routing_key text NOT NULL,
    payload bytea NOT NULL,
    trace_parent text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    sent_at timestamptz
);
CREATE INDEX signals_outbox_pending_idx ON signals_outbox (created_at, id) WHERE sent_at IS NULL;
CREATE INDEX signals_outbox_retention_idx ON signals_outbox (sent_at) WHERE sent_at IS NOT NULL;

ALTER TABLE check_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE check_rollups ENABLE ROW LEVEL SECURITY;
ALTER TABLE metric_rollups ENABLE ROW LEVEL SECURITY;
ALTER TABLE threshold_state ENABLE ROW LEVEL SECURITY;

-- +goose StatementBegin
DO $$
DECLARE
    prefix text := regexp_replace(current_user, '_migrator$', '');
    relation text;
BEGIN
    IF prefix = current_user THEN
        RAISE EXCEPTION 'stream storage migration requires a migrator role';
    END IF;
    FOREACH relation IN ARRAY ARRAY['check_results', 'metrics', 'check_rollups', 'metric_rollups', 'threshold_state'] LOOP
        EXECUTE format('CREATE POLICY tenant ON %I USING (organization_id = nullif(current_setting(''app.org_id'', true), '''')::uuid) WITH CHECK (organization_id = nullif(current_setting(''app.org_id'', true), '''')::uuid)', relation);
        EXECUTE format('GRANT SELECT, INSERT, UPDATE ON %I TO %I', relation, prefix || '_app');
    END LOOP;
    EXECUTE format('GRANT SELECT ON check_results, metrics, check_rollups, metric_rollups TO %I', prefix || '_reader');
    EXECUTE format('GRANT SELECT, DELETE ON check_rollups, metric_rollups, threshold_state, signals_outbox TO %I', prefix || '_batch');
    EXECUTE format('GRANT INSERT, SELECT, UPDATE (sent_at) ON signals_outbox TO %I', prefix || '_app');
END
$$;
-- +goose StatementEnd

SELECT add_retention_policy('check_results', drop_after => interval '7 days', schedule_interval => interval '1 hour');
SELECT add_retention_policy('metrics', drop_after => interval '7 days', schedule_interval => interval '1 hour');

-- +goose Down
DROP TABLE signals_outbox, threshold_state, metric_rollups, check_rollups, metrics, check_results;
