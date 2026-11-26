# Initial implementation specification

Authority: architecture a265a2c7389678e4bd00aff6b8f3ebcce5db6800, Logging LOG-01–08. Baseline 42fe347437f49a51f69170f0eb691024df2350ee.

LOG-01/03: independent service, no domain authority. LOG-02/04: host local sink, trusted producer and fixed numeric/boolean allowlist. LOG-05: one in-flight write, fixed capacity, rate admission, timeout uncertainty and circuit closure. LOG-06: bounded local history, policy revision and restricted data types; legal holds and provider retention not implemented. LOG-07: headless only. LOG-08: scoped local reads; export and lifecycle events outstanding.

Threats: forged tenant, untrusted producer, sensitive fields, sink timeout, cross-tenant read, resource exhaustion and global health leakage. Controls: explicit resource decisions, server trust boundary, allowlist, bounded records/rate, single in-flight write, closed intake after uncertainty, host-only health. Trusted providers must bound their own resolution latency and input transport size. Identifier provenance is host responsibility; numeric values can still encode personal data and must be classified by rule owners.

Outstanding: policy floors and changes, sampling/routing, external destinations, retry/recovery, safe shutdown drain, retention and holds, protected export, lifecycle evidence/outbox, scoped delivery health, richer query filters, strict envelope validation, provider acceptance and optional GUI. No production/security assurance claim. CI and independent consumer evidence required before acceptance; tests not executed by AI.
