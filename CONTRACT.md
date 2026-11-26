# Logging consumer contract — initial headless subset

## Install

Use `"@nuxt-layers/logging": "github:nuxt-layers/logging#master"` after acceptance. Node 26.13+ with native TypeScript stripping; no browser or Nuxt dependency. Import `createLogging` from the package root.

## Settings

Pass options to the single composition call. No environment variables. Capacity defaults to 1000 (max 10000), ratePerMinute to 120 (max 10000), timeoutMs to 1000 (max 30000). Required retentionMs is a positive integer at most 30 days; applies only to process-local read history. policyRevision is a bounded identifier. Invalid limits or rules throw at composition.

## Host passes

`createLogging<E>({resolveCaller, resolveProducer, decide, now, localSink, rules, policyRevision, retentionMs})`.

- resolveCaller(event: E): Promise<{tenantId:string;subjectId:string|null;assurance:unknown}|null>
- resolveProducer(event: E): same caller plus source:string; must authenticate server producer, never trust submitted labels.
- decide(caller, action:string, resource:{tenantId:string;source:string}): Promise<{allowed:boolean;reason:string}>. Only explicit true permits.
- now(): {instant:string;quality:'verified'|'degraded'|'unknown'}; UTC RFC3339 with Z, monotonic receipt times.
- localSink.write(record:RecordEntry, signal:AbortSignal): Promise<'delivered'|'refused'|'uncertain'>. Host supplies bounded sink, honours abort and independently governs persistence, retention and legal holds. Never log raw payloads. Acknowledgement does not guarantee durability.
- rules: array of {source:string;code:string;purpose:string;attributes:readonly string[]}; trusted fixed allowlist, only approved non-personal numeric/boolean fields.

## Client and server functions

Server only. submit(event, {version:1,code,severity,occurrenceTime,correlation?,attributes?}) returns Promise<Receipt>. Severity debug/info/warn/error/fatal. Attributes numeric or boolean only, maximum 16, magnitude <= 1e12; finite numbers. Codes and correlation are opaque approved identifiers, never sensitive values. Producer and tenant come from trusted resolver. Unknown attributes reject. Other envelope fields are ignored and never forwarded.

Receipt: {status:'accepted'|'rejected'|'throttled'|'unavailable'|'uncertain';code:string;id?:string}. Codes LOG_ACCEPTED, LOG_DENIED, LOG_INVALID, LOG_RULE, LOG_RATE, LOG_CAPACITY, LOG_BUSY, LOG_SINK_REFUSED, LOG_SINK_UNCERTAIN, LOG_PROVIDER. Accepted means local sink acknowledged processing, not durable storage, business success or Audit custody. Uncertain closes intake until host recomposes; no automatic retry.

RecordEntry: id, tenantId, source, code, severity, occurrenceTime, receiptTime, timeQuality, optional correlation, readonly attributes, policyRevision.

readDiagnostics(event,{tenantId,source,from,to,limit,offset?}) returns status completed/rejected/unavailable and records; completed includes coverage warning. UTC receipt time range at most 30 days, limit 1–100, nonnegative integer offset. Process-local, mutable pagination; no total count. Expiry occurs during submit/read.

hostHealth() exposes deployment-wide counts/state to trusted composition root only; do not expose to tenant callers. close() stops intake and clears local history; an in-flight write may still finish.

## Permissions

logging.submit, logging.read on explicit tenant/source. Reads require matching trusted tenant and affirmative resource decision before lookup. No cross-tenant override. Host owns assurance policy.

## Screens

Not applicable: no presentation shipped.

## Headless surface

Intake, local acknowledgement, scoped local read and composition-root health. No external sink routing, privileged policy changes, protected export, legal hold workflow, lifecycle events or persistent recovery in this initial subset.

## You will hit this

Identifiers match ASCII alphanumeric first character followed by up to 95 alphanumeric, underscore, dot, colon or hyphen characters. Times use UTC Z. Only one submission in flight; concurrent calls receive unavailable. Sink timeout is uncertain and stops new intake. Producer retry may duplicate; no deduplication promise. Do not use this subset for regulated retention or evidence custody. No real IAM/Clock/Configuration integration verified.
