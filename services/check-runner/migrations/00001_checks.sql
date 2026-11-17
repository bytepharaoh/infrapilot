-- +goose Up
CREATE TABLE check_organizations (
    organization_id uuid PRIMARY KEY,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE checks (
    id                 uuid PRIMARY KEY,
    organization_id    uuid NOT NULL REFERENCES check_organizations (organization_id),
    project_id         uuid NOT NULL,
    resource_id        uuid NOT NULL,
    kind               smallint NOT NULL CHECK (kind BETWEEN 1 AND 5),
    target             text NOT NULL CHECK (octet_length(target) BETWEEN 1 AND 2048),
    port               integer NOT NULL CHECK (port BETWEEN 0 AND 65535),
    interval_seconds   integer NOT NULL CHECK (interval_seconds BETWEEN 30 AND 86400),
    timeout_seconds    integer NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 15),
    enabled            boolean NOT NULL,
    max_duration_ms    double precision CHECK (max_duration_ms > 0 AND max_duration_ms <= 15000),
    min_tls_days       integer CHECK (min_tls_days BETWEEN 1 AND 365),
    threshold_version  bigint NOT NULL DEFAULT 1 CHECK (threshold_version > 0),
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    next_run_at        timestamptz NOT NULL,
    lease_owner        uuid,
    lease_until        timestamptz,
    lease_epoch        bigint NOT NULL DEFAULT 0,
    last_outcome       smallint CHECK (last_outcome IN (1, 2)),
    last_observed_at   timestamptz,
    CHECK ((lease_owner IS NULL) = (lease_until IS NULL))
);
CREATE INDEX checks_project_idx ON checks (organization_id, project_id, id);
CREATE INDEX checks_due_idx ON checks (next_run_at, id) WHERE enabled;
CREATE INDEX checks_org_due_idx ON checks (organization_id, next_run_at, id) WHERE enabled;
CREATE INDEX checks_active_leases_idx ON checks (organization_id, lease_until) WHERE lease_owner IS NOT NULL;

-- Tenant-scoped idempotency records outlive a hard-deleted check for the 24-hour retry window.
CREATE TABLE check_idempotency (
    organization_id uuid NOT NULL,
    principal_key   bytea NOT NULL,
    key             text NOT NULL CHECK (octet_length(key) BETWEEN 1 AND 128),
    fingerprint     bytea NOT NULL,
    check_id        uuid NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, principal_key, key)
);
CREATE INDEX check_idempotency_created_idx ON check_idempotency (created_at);

-- Kafka resource topic checkpoints are operational metadata; each partition has at most one serving
-- generation and one in progress. Tenant rows are visible to claims only when those generations match.
CREATE TABLE resource_partitions (
    kafka_partition integer PRIMARY KEY CHECK (kafka_partition BETWEEN 0 AND 11),
    topic_id        text NOT NULL DEFAULT '',
    generation      bigint NOT NULL DEFAULT 0,
    next_offset     bigint NOT NULL DEFAULT 0,
    caught_up_at    timestamptz,
    rebuild_since   timestamptz
);
INSERT INTO resource_partitions (kafka_partition) SELECT generate_series(0, 11);

CREATE TABLE resource_projection (
    organization_id uuid NOT NULL,
    resource_id     uuid NOT NULL,
    project_id      uuid NOT NULL,
    kafka_partition integer NOT NULL REFERENCES resource_partitions(kafka_partition),
    generation      bigint NOT NULL,
    revision        bigint NOT NULL CHECK (revision > 0),
    active          boolean NOT NULL,
    PRIMARY KEY (organization_id, resource_id, generation)
);
CREATE INDEX resource_projection_generation_idx ON resource_projection (kafka_partition, generation);

-- Outbox payload is an already serialized Envelope. Infrastructure bookkeeping is system-scoped.
CREATE TABLE check_result_outbox (
    id           uuid PRIMARY KEY,
    routing_key  text NOT NULL,
    trace_parent text NOT NULL,
    payload      bytea NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    sent_at      timestamptz
);
CREATE INDEX check_result_outbox_pending_idx ON check_result_outbox (created_at, id) WHERE sent_at IS NULL;
CREATE INDEX check_result_outbox_pending_key_idx ON check_result_outbox (routing_key, created_at, id) WHERE sent_at IS NULL;
CREATE INDEX check_result_outbox_sent_idx ON check_result_outbox (sent_at) WHERE sent_at IS NOT NULL;

ALTER TABLE check_organizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE checks ENABLE ROW LEVEL SECURITY;
ALTER TABLE check_idempotency ENABLE ROW LEVEL SECURITY;
ALTER TABLE resource_projection ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant ON check_organizations USING (organization_id = NULLIF(current_setting('app.org_id', true), '')::uuid)
    WITH CHECK (organization_id = NULLIF(current_setting('app.org_id', true), '')::uuid);
CREATE POLICY tenant ON checks USING (organization_id = NULLIF(current_setting('app.org_id', true), '')::uuid)
    WITH CHECK (organization_id = NULLIF(current_setting('app.org_id', true), '')::uuid);
CREATE POLICY tenant ON check_idempotency USING (organization_id = NULLIF(current_setting('app.org_id', true), '')::uuid)
    WITH CHECK (organization_id = NULLIF(current_setting('app.org_id', true), '')::uuid);
CREATE POLICY tenant ON resource_projection USING (organization_id = NULLIF(current_setting('app.org_id', true), '')::uuid)
    WITH CHECK (organization_id = NULLIF(current_setting('app.org_id', true), '')::uuid);

-- +goose StatementBegin
DO $$
DECLARE
    app   text := regexp_replace(current_user, '_migrator$', '_app');
    batch text := regexp_replace(current_user, '_migrator$', '_batch');
BEGIN
    IF app = current_user THEN RAISE EXCEPTION 'migrations require the migrator role'; END IF;
    EXECUTE format('GRANT SELECT, INSERT ON check_organizations, checks, check_idempotency TO %I', app);
    EXECUTE format('GRANT UPDATE (updated_at) ON check_organizations TO %I', app);
    EXECUTE format('GRANT UPDATE (resource_id, kind, target, port, interval_seconds, timeout_seconds, enabled, max_duration_ms, min_tls_days, threshold_version, updated_at, lease_epoch, lease_owner, lease_until, next_run_at) ON checks TO %I', app);
    EXECUTE format('GRANT DELETE ON checks TO %I', app);
    EXECUTE format('GRANT UPDATE (fingerprint, check_id, created_at) ON check_idempotency TO %I', app);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON resource_projection TO %I', app);
    EXECUTE format('GRANT SELECT, UPDATE ON resource_partitions TO %I', app);
    EXECUTE format('GRANT SELECT ON check_organizations, checks, resource_partitions, resource_projection TO %I', batch);
    EXECUTE format('GRANT UPDATE (updated_at) ON check_organizations TO %I', batch);
    EXECUTE format('GRANT UPDATE (lease_owner, lease_until, lease_epoch, next_run_at, last_outcome, last_observed_at) ON checks TO %I', batch);
    EXECUTE format('GRANT INSERT, SELECT ON check_result_outbox TO %I', batch);
    EXECUTE format('GRANT UPDATE (sent_at) ON check_result_outbox TO %I', batch);
    EXECUTE format('GRANT DELETE ON check_result_outbox, resource_projection TO %I', batch);
    EXECUTE format('GRANT SELECT, DELETE ON check_idempotency TO %I', batch);
END
$$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE check_result_outbox;
DROP TABLE resource_projection;
DROP TABLE resource_partitions;
DROP TABLE check_idempotency;
DROP TABLE checks;
DROP TABLE check_organizations;
