# ADR-0033 — Bound OTLP metric ingestion and identify retries

- Status: accepted (refines ADR-0029)
- Date: 2026-11-16
- Decider: InfraPilot maintainers

## Context

A single OTLP export can contain many data points. A slow Kafka broker can hold accepted work across
both listeners, and serializing one Kafka record per point can duplicate shared resource metadata.
The receiver needs bounded CPU and memory and a way for storage to identify retried samples.

## Options

1. Enqueue HTTP and gRPC requests asynchronously and acknowledge intake. This lets ingestion return
   before broker delivery and loses acknowledged data when the receiver stops.
2. Publish whole requests and wait for Kafka. This reduces repeated metadata but one export can
   contain many resources and series. Storage would still have to split records and define stable
   per-point identities for deduplication.
3. Batch typed per-point envelopes within explicit limits and wait for every Kafka acknowledgment.

## Decision

Use option 3. OTLP/gRPC and OTLP/HTTP share admission, validation, quota and publication. HTTP
accepts the binary and JSON Protobuf encodings and identity or gzip compression. The case-insensitive
Bearer scheme carries a project API key. Exactly one resource UUID is supplied in
`x-infrapilot-resource-id`. The tenant, project and resource used for routing and quota come from
that input and the control-plane authorization result.

Defaults per replica are one MiB per compressed or decompressed request, 1,024 points per export,
32 active exports and four MiB of serialized Kafka envelopes per export. A single preserved
OTLP point can occupy at most 512 KiB. Admission rejects capacity overload before decoding the
request. Both transports send retry hints for capacity overload. The finite Kafka producer
buffer and acknowledgement deadline also bound work after admission. Exports with invalid
points fail before publication. Exports containing no metric points succeed after authentication.

Charge points in an atomic Redis token bucket keyed by the authenticated organization. Use Redis
server time so replicas share one rate of 2,000 points per second and a burst of 4,000 by default.
Idle buckets expire. Redis failures return retryable errors so a shared quota continues to protect
Kafka during cluster degradation. This quota serves a different purpose from the fail-open edge
request limiter in ADR-0026.

Use trusted identifiers, resource and instrumentation-scope attributes, metric name, unit,
point kind, applicable temporality and monotonicity, and data-point attributes to identify a
series. Canonicalize attribute key order, including nested key-value collections. Hash the series
identity and the original nanosecond window timestamps to identify a sample. Retain the first
accepted value for a sample identity. A response lost after a partial Kafka publish can be retried
using the same sample identities. Measurements must fall within seven days in the past and five
minutes in the future at receipt time. Earlier start times for cumulative points are valid.

Preserve the original typed OTLP point in each payload together with its resource and scope.
Finite scalar gauge and sum values also populate the typed numeric field for rollups. Histograms,
exponential histograms, summaries and points without recorded scalar values remain available in
the retained OTLP payload.

## Consequences

The receiver can wait only within the export deadline and never acknowledges a batch based on
buffer admission alone. Repeating resource and scope metadata costs storage and bandwidth, so
we cap serialized bytes as well as input bytes and point count. Quota charges admission attempts,
including attempts whose broker acknowledgment is lost. This keeps retry traffic subject to the
same budget as initial exports.

The transport and identity rules follow the [OTLP specification](https://opentelemetry.io/docs/specs/otlp/)
and [OTel metrics data model](https://opentelemetry.io/docs/specs/otel/metrics/data-model/).
