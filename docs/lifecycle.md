# Protected lifecycle specification

LOG-06/08, ADR-0017/18. Batch 03 stacked on PR #2 e0d72987d04574c28bd318b3ba0051b7192c7678. Local cache projection is not durable custody. Provide scoped health and protected bounded export, exact-record restriction and erasure. Affirmative resource permission plus independent approval evidence required; recheck permission after approval. Erasure requires clear preservation; held and unknown block. No global deletion assertion. No restricted projection in reads/export/health. Serialize privileged commands with intake. Request IDs are correlation only.

Outstanding: durable Logging-owned state and transactional outbox, provider-qualified retention/hold/restriction/erasure/recovery, committed lifecycle events, independent provider integration, privileged policy mutation, complete health and acceptance. The approval port is not an implemented Audit/Privacy/Legal Compliance integration. No actual provider contract or persistence API is invented. No tests/builds executed by AI.

Continuation on accepted master d6009b9c1299c9c12417866fd230a201a669eced: optional durable lifecycle journal, concrete Node SQLite implementation, atomic tombstone/request/outbox commit, scoped visibility checks and bounded explicit relay. Diagnostic persistence, provider-level holds/deletion, export events, policy mutations, distributed relay coordination and governed journal retention remain outstanding. No CI evidence yet.

Persistent diagnostic store implemented in same journal database, before-sink persistence, scoped recovery without replay, bounded capacity, observed delivery updates, atomic explicit erasure, verified-clock preservation-aware maintenance. Purpose-specific retention, journal compaction and provider lifecycle remain outstanding. No test execution by AI.

Architecture master rechecked at e9dac240c16891fcbed9669228dac6cd5cde9c3c. Purpose-specific retention and rotating maintenance scans implemented. Journal evidence compaction requires explicit governed retention and is not performed by this batch.

Governed acknowledged-event payload pruning implemented with verified acknowledgement time, bounded rotating scan, explicit preservation clearance and conditional deletion. Request identity and marker retirement remain intentionally unsupported.

Qualified provider operation and Configuration compare-and-set policy ports added. Actual provider implementations, atomic external hold/recovery checks and event coverage remain unverified.

Final hardening batch: provider and policy results are explicitly projected to public fields. Shutdown aborts active delivery/provider/relay signals and waits within the configured budget for admitted intake, privileged mutations and relay operations; a timeout reports uncertainty. Host ports that ignore cancellation still require independent bounded execution. Regression tests added; local build and acceptance pending.

Verified-clock, preservation-cleared durable expiry now atomically removes the diagnostic and inserts a minimised logging.expired event. No actor or independent evidence reference is fabricated for automated maintenance. Repeated expiry of an absent record emits no event. External policy/provider commits remain distinct from Logging-owned state. Request and erasure-marker retirement requires an explicit replay/backup/provider safety horizon; no unsafe deletion has been introduced.

Durable protected export now commits a release event and unique scoped request identity before returning records. A release records the authorised local disclosure decision, not transport completion or independent Audit custody. Missing or uncertain journal confirmation fails closed. Replay rejects rather than re-reading a potentially changed snapshot. No export payload is persisted in the event.

External confirmations now have a distinct durable observation event; Logging does not claim external atomicity. A failed local observation after external success returns uncertainty. Host-side authoritative reconciliation remains required after a crash between external mutation and local observation; replay of external operations must be idempotent at the authority.
