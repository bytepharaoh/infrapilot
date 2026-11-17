# ADR-0032: Check-runner scheduling, quotas and outgoing connections

- Status: accepted (refines ADR-0013) · Decider: InfraPilot maintainers · Date: 2026-11-16

## Context

M03 introduces checks defined by tenant operators and executed across replicas. Accepted lease fencing
needs concrete admission, scheduling and network bounds. Stream-processor also needs the exact threshold
settings used for each run.

## Options considered

1. Keep tenant counts in each check-runner process. Replica changes would vary each tenant's effective quota.
2. Enforce count and in-flight limits in the same PostgreSQL transactions that create and claim checks.
3. Use Redis counters for admission and in-flight leases. This introduces cross-store failure paths for a
   scheduler already coordinated through PostgreSQL.

## Decision

Use option 2. Serialize creation and lease claims for each organization through its quota row. Allow 10,000
check definitions per organization and eight unexpired leases per organization by default. A replica claims
only as many leases as its worker pool can start. Cadences run from 30 seconds to 24 hours, with a 1–15 second
probe timeout and a five-second lease margin. Apply deterministic jitter of up to 999 milliseconds to the
first and subsequent runs. Renew the next scheduled slot from the later of the end of the cadence and the
current database time, so a long outage cannot cause a burst of missed slots. The current committed slot
remains eligible after a worker dies and can be completed by a new epoch holder.

A check update fences any in-flight probe and increments the threshold version. The committed result includes
the version and options captured when its lease began. Check create, read, update and delete use control-plane's
existing project-scoped resource permissions. Check creation and change of resource also verify the live
resource with control-plane. Scheduling uses the compacted resource state projection. Results are accepted only
while their check, lease and projected resource remain eligible.

Workers issue a bounded HTTP GET, a TCP connection, a server-authenticated TLS handshake, a DNS address
lookup or one datagram ping. They never request administrative credentials. Outgoing addresses must be public,
including redirects and resolved addresses, and are checked against configured cluster ranges. HTTP sends no
cookies or authentication and validates all TLS server certificates. Probes retain a bounded result code,
measurement and certificate lifetime only. The worker stores neither response bodies nor error text from
targets. The deployment must deny egress to service and internal network CIDRs independently of the dialer.

## Consequences

Replica counts and restarts leave quotas consistent. Isolation uses both tenant RLS and a narrow batch role for
leasing several tenants in one transaction. Probe execution is bounded and can repeat after lease expiry.
Only a fenced completion emits a result. A recovered runner does not flood a target with skipped work. Network
probes intentionally monitor public services. Private endpoints require the later enrolled-agent path.

Verify concurrency and expired-worker fencing on PostgreSQL, prove address checks under DNS changes and
redirects, and measure drift on three replicas at the M03 load gate.
