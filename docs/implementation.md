# Implementation specification

Architecture a265a2c7389678e4bd00aff6b8f3ebcce5db6800; current batch baseline 28d0b4db485fa3f35973435f738b514d3d46c117. LOG-01–08, ADR-0017 tenant isolation and ADR-0018 policy authority.

The independent service owns technical records and delivery projections, not source outcomes or Audit evidence. Fixed deployment rules establish immutable admission and destination maxima. Optional host-approved effective policy may only narrow. Strict structured envelopes, numeric/boolean allowlists, bounded history and rate admission, UTC timestamp validation, separate tenant/source decisions for reads, policy and delivery. Local sink acknowledgement precedes optional external routing; explicit refusal permits finite retries, uncertain external delivery quarantines that destination, uncertain local delivery closes intake. No automatic restart/replay. Shutdown aborts and reports uncertainty if work does not settle within the configured timeout.

Threats: forged tenant, untrusted producer, sensitive fields, sink timeout, cross-tenant read, policy capture, resource exhaustion and global health leakage. Host must bound provider latency and transport size, resolve verified policy freshness and classify identifier/numeric provenance. Sink ownership and retention must be documented by host. No legal hold or durable custody promise.

Remaining: independently evidenced privileged policy changes, protected export/rights and legal holds, persistent recovery and lifecycle outbox, scoped aggregate health, real provider acceptance, package installation/build and lockfile/CI, protected Nuxt endpoints and optional GUI. No tests or builds executed by AI.
