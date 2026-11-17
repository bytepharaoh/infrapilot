-- +goose Up
ALTER TABLE resource_state_outbox
    ADD COLUMN kafka_partition SMALLINT CHECK (kafka_partition BETWEEN 0 AND 11),
    ADD COLUMN payload BYTEA,
    ADD COLUMN sent_at TIMESTAMPTZ,
    ADD CHECK ((kafka_partition IS NULL) = (payload IS NULL)),
    ADD CHECK (sent_at IS NULL OR payload IS NOT NULL);

CREATE INDEX resource_state_unprepared_idx ON resource_state_outbox (revision) WHERE payload IS NULL;
CREATE INDEX resource_state_pending_key_idx ON resource_state_outbox (routing_key, revision) WHERE sent_at IS NULL;
CREATE INDEX resource_state_partition_idx ON resource_state_outbox (kafka_partition, revision)
    WHERE sent_at IS NULL AND kafka_partition IS NOT NULL;
CREATE INDEX resource_state_sent_idx ON resource_state_outbox (sent_at) WHERE sent_at IS NOT NULL;

CREATE TABLE resource_state_partitions (
    partition SMALLINT PRIMARY KEY CHECK (partition BETWEEN 0 AND 11),
    checked_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity'
);
INSERT INTO resource_state_partitions (partition) SELECT generate_series(0, 11);

-- +goose StatementBegin
DO $$
DECLARE
    svc text := regexp_replace(current_user, '_migrator$', '');
BEGIN
    IF svc = current_user THEN RAISE EXCEPTION 'migrations must run as <service>_migrator'; END IF;
    EXECUTE format('GRANT SELECT ON resource_state_outbox, resource_state_partitions TO %I', svc || '_app');
    EXECUTE format('GRANT UPDATE (kafka_partition, payload, sent_at) ON resource_state_outbox TO %I', svc || '_app');
    EXECUTE format('GRANT UPDATE (checked_at) ON resource_state_partitions TO %I', svc || '_app');
    EXECUTE format('GRANT SELECT, DELETE ON resource_state_outbox TO %I', svc || '_batch');
END
$$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE resource_state_partitions;
DROP INDEX resource_state_sent_idx;
DROP INDEX resource_state_partition_idx;
DROP INDEX resource_state_pending_key_idx;
DROP INDEX resource_state_unprepared_idx;
ALTER TABLE resource_state_outbox DROP COLUMN sent_at, DROP COLUMN payload, DROP COLUMN kafka_partition;
