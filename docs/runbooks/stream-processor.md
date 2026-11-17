# Stream processor

The processor consumes `check.results.v1` and `telemetry.metrics.v1`. It writes raw records, rollups
and threshold state to its TimescaleDB database. The owning read RPC serves raw metrics and check
results to query-service over mTLS.

## Configuration and rollout

- Provision the TimescaleDB extension in the service database before running `stream-processor migrate`.
  It uses `MIGRATION_DATABASE_URL` and takes the goose session lock. The migrator owns the database schema
  and retention policy jobs. Install `timescaledb` in the provisioning database with the privileges
  required by your cluster operator.
- Use `DATABASE_URL` for `streamprocessor_app`, `READ_DATABASE_URL` for the SELECT-only
  `streamprocessor_reader`, and `BATCH_DATABASE_URL` for the audited `streamprocessor_batch` retention job.
  Writer and reader pool limits default to six each. Budget both pools for the maximum deployment size.
- Set `KAFKA_BROKERS` and provision the stream topics before serving. The service needs access to consume
  the two inputs, commit offsets to `KAFKA_CONSUMER_GROUP` (default `stream-processor-v1`) and write to
  `signals.v1` and both input dead-letter topics. Set `KAFKA_CA_FILE`, `KAFKA_CERT_FILE`, and
  `KAFKA_KEY_FILE` to the Kafka credentials in environments with mTLS.
- Set `QUERY_IDENTITIES` to the query-service SPIFFE identity. The owner read RPC checks the scope and
  range again and opens a tenant-scoped read-only transaction with the reader credential.
- Run `stream-processor purge` hourly under the batch role. It deletes 90-day check buckets, 30-day
  one-minute metric buckets, 400-day hourly metric buckets, 90-day threshold state, and acknowledged
  signal rows after seven days. Timescale drops raw chunks based on a seven-day horizon. Physical retention
  includes the remainder of one daily chunk plus the retention job's schedule interval.

## Consumer lag and paused partitions

Inspect `stream_processor_consumer_lag` by `topic` and `partition` and compare it with
`stream_processor_consumer_partition_paused`, database saturation, and PostgreSQL lock waits. Each
application poll retrieves up to 32 records. Processing pauses each fetched partition until the batch is
durable and its offset is acknowledged. A stalled batch leaves offsets uncommitted and the consumer can
rejoin from durable offsets. Open transactions have timeouts and connection counts are bounded.

1. Check TimescaleDB reachability, active locks and storage latency. Review the processor log and trace for
   the first write failure.
2. If one partition repeatedly fails, check its DLQ and the producer deployment version. An invalid record
   is acknowledged in the DLQ before the source offset advances. The DLQ contains tenant data and is
   accessible only to authorized service operators. Use the source topic, partition and offset headers to
   correlate and deduplicate records. Inspect payloads in the approved environment.
3. Confirm backlog declines and stalled partitions resume. Recover offsets only after assessing the impact
   of input-topic retention and the raw storage horizon. Check the owning database before any reset.

## Threshold signal outbox

Inspect `stream_processor_signal_outbox_pending` and `stream_processor_signal_outbox_oldest_seconds`.
If the age increases, verify Kafka reachability, `signals.v1` ACLs and ISR. Retry resumes from the same
stored envelope. Consumers deduplicate by `event_id`; threshold state advances by scheduled slot and
lease epoch. Keep pending records until acknowledged and investigate before changing retention.
