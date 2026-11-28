# Storage register

Process-local diagnostic history: bounded trusted tenant/source, codes, opaque correlation, numeric/boolean fields and timestamps; delivery states and local restriction flag. Purpose technical diagnostics. Host-configured retention at most 30 days, lazy expiry and process termination. Native process-local mode has no durable storage; no browser cookies or browser storage are introduced. Protected local export discloses only scoped sanitised records; local erasure/restriction do not alter provider data. Independent approval references are retained in configured durable metadata; process-local operations return references. Host local/external sinks independently document location, retention, access, holds, deletion and backups. Rate buckets and global health counters are process-local. Cache is not legal preservation custody; durable hold, provider rights and lifecycle evidence remain unavailable. Do not deploy for workloads requiring them.

Optional Logging-owned SQLite journal: persistent restriction/erasure tombstones, scoped request fingerprints, outbox events with opaque subject/evidence references and acknowledgement flag. WAL and backups contain the same sensitive operational metadata. Host selects secured file location, permissions and backup controls. Explicit governed maintenance is available; host schedules it and supplies qualified authority. Journal-only mode does not persist diagnostic payloads.

Optional diagnostic store persists sanitised records and delivery state in the same database. Capacity max 10000. Expiry hides aged records; explicit verified-clock maintenance with clear preservation removes local payloads only. Held/unknown retained. WAL/backups require separate controls. Separate governed journal maintenance is available.

Purpose-specific retention ceilings are recorded on admission; current policy may shorten but not extend the admitted lifetime. Maintenance uses bounded rotating scans and does not delete held/unknown payloads.

Optional governed journal maintenance deletes only aged, acknowledged outbox event payloads with verified acknowledgement time and independent preservation clearance. Pending/unknown/held events, request identities and restriction/erasure markers remain retained. Database pages, WAL and backup disposal are separate host obligations.

SQLite journal metadata capacity defaults to 100000 rows per category (request identities, markers, and events plus reserved event slots), configurable 1–1000000. Existing over-limit data is preserved. Durable external reservations hold scoped identifiers, operation kind and target or policy revisions; pending reservations survive restarts. Explicit governed retirement is available; no automatic retirement. Capacity exhaustion blocks new protected work rather than discarding evidence. Physical file/WAL/backup bounds remain host obligations.

The SQLite `logging_sink_state` table stores tenant/source/destination identifiers, last observed state and observation timestamp. Cardinality is bounded by journal capacity. No diagnostic payload or credentials are stored in this table. Governed retirement of these projections is available subject to eligibility and independent authority.

Request identities and lifecycle markers now have opaque retirement versions (random 128-bit identifiers); used for compare-and-delete under independent host authority, not personal identifiers or retention periods.

SQLite `logging_delivery_recovery` stores scoped request, record, destination, mode, subject, approval and outcome evidence references and timestamp. Capacity is bounded; governed retirement is available for definitive recovery identities.

Recovery metadata and sink projections now carry opaque retirement versions. Definitive recovery identities are eligible only after record removal; unresolved identities remain protected. Sink projection retirement requires no scoped diagnostics or matching outbox references and independent authority.
