# Telemetry ingestion

Telemetry ingestion authenticates and publishes OTLP metrics through `telemetry.metrics.v1`. HTTP
accepts `POST /v1/metrics` with `application/x-protobuf` or `application/json`. The gRPC listener exposes
`opentelemetry.proto.collector.metrics.v1.MetricsService/Export`. Both accept gzip. Send the project API
key as an Authorization Bearer credential and set `x-infrapilot-resource-id` to the resource UUID.

Set `HTTP_ADDR=:4318`, `GRPC_ADDR=:4317` and `ADMIN_ADDR=:8081` for a conventional deployment.
Terminate public HTTP TLS at ingress and send traffic to the in-cluster OTLP port. Configure
`GRPC_TLS_CERT_FILE` and `GRPC_TLS_KEY_FILE` for the public TLS gRPC listener in production.
The control-plane client uses `CONTROLPLANE_TARGET`, `CONTROLPLANE_SERVER_IDENTITY` and
`CONTROLPLANE_TLS_CA_FILE`, `CONTROLPLANE_TLS_CERT_FILE`, `CONTROLPLANE_TLS_KEY_FILE`.
Add the receiver's service identity to the control-plane `INGESTION_IDENTITIES` allow-list.

Set `REDIS_ADDR` and supply credentials via a Secret. The Redis ACL needs access to the
`infrapilot:ingest:quota:*` key pattern and the scripting, server-time and basic string commands
used by the token bucket. Use `REDIS_TLS=true` with a TLS-enabled endpoint. Set `KAFKA_BROKERS`
and mount the Kafka CA, certificate and key using `KAFKA_TLS_CA_FILE`, `KAFKA_TLS_CERT_FILE`
and `KAFKA_TLS_KEY_FILE` in production. Give this workload only the Kafka permissions needed
for idempotent production to `telemetry.metrics.v1`; see the [Kafka topology](../../deploy/kafka/README.md).

## TelemetryIngestionUnavailable

**Meaning.** Valid exports are failing during control-plane authentication, Redis quota admission,
or Kafka acknowledgment. Each export that succeeds has been acknowledged by the Kafka broker.

**Check.** Inspect `telemetry_ingestion_exports_total{outcome="unavailable"}` and dependency traces.
Check the control-plane authorization RPC latency and availability, the Redis endpoint, and the
Kafka partition leaders and ISR count. A process can stay ready while a dependency is unavailable
so that senders receive explicit retry guidance.

**Mitigate.** Restore the failing dependency. After Kafka recovery, watch the accepted points
counter and the stream processor's consumer lag. Export retries can produce duplicate records
with reproducible sample IDs. The storage writer deduplicates by sample identity.

## TelemetryIngestionBackpressure

**Meaning.** A replica's admission slots or Kafka buffer were exhausted, or a tenant exceeded its
quota. HTTP overload returns 503 with Retry-After; a quota denial returns 429. gRPC sends
RESOURCE_EXHAUSTED with RetryInfo for both capacity and quota rejection.

**Check.** Compare the `busy` and `quota` outcomes on the exports counter with the in-flight gauge,
broker produce latency, Redis errors and memory use. Sustained `quota` suggests a tenant is sending
more points than the configured budget. An increasing `busy` count combined with slow Kafka acks
indicates broker pressure.

**Mitigate.** Give exporters time to honor retry hints, reduce sampling volume or split an oversized
batch. Increase replica count only if Kafka, Redis and control-plane have capacity. Evaluate the
configured limits with the storage team before increasing them. The defaults and sample-window rules
are in [ADR-0033](../adr/0033-otlp-ingestion-limits.md).

## Authentication and malformed exports

A missing, revoked or invalid key returns HTTP 401 or gRPC UNAUTHENTICATED. A key that lacks ingest
scope or a resource outside its project returns HTTP 403 or gRPC PERMISSION_DENIED. Correct the key
or resource binding before retrying. The receiver verifies the resource against control-plane on
every request. Payload attributes cannot select a different tenant or project.

Requests whose wire or decompressed size exceeds the cap return HTTP 413 or gRPC RESOURCE_EXHAUSTED
without a retry hint. Split the batch. Malformed data or samples outside the retention window return
a non-retryable bad-data response. The metrics receiver does not log API keys, request bodies or
resource attributes.

## Local verification

From the telemetry-ingestion module, run `go test -race -count=1 -tags integration -run TestF04 -v ./internal/app`
with Docker running. The test starts the real control-plane executable against PostgreSQL, starts
Redis and the pinned Kafka image, pauses Kafka, verifies both transport retry hints under capacity
pressure, and checks the acknowledged samples and their stable identity after recovery.
