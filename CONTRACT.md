# Logging consumer contract — headless service

## Install

Use `"@nuxt-layers/logging": "github:nuxt-layers/logging#master"` after acceptance. Node 26.13+ with native TypeScript stripping; no browser or Nuxt dependency. Import `createLogging` from the package root.

## Settings

Pass options to the single composition call. No environment variables for the core. Optional Nuxt layer entry: extend '@nuxt-layers/logging/nuxt'. Its server-only runtimeConfig.logging.presentation defaults to false; no pages or HTTP endpoints are registered yet. Capacity defaults to 1000 (max 10000), ratePerMinute to 120 (max 10000), timeoutMs to 1000 (max 30000). Required retentionMs is a positive integer at most 30 days; applies only to process-local read history. policyRevision is a bounded identifier. Invalid limits or rules throw at composition.

## Host passes

`createLogging<E>({resolveCaller, resolveProducer, decide, now, localSink, rules, policyRevision, retentionMs})`.

- resolveCaller(event: E): Promise<{tenantId:string;subjectId:string|null;assurance:unknown}|null>
- resolveProducer(event: E): same caller plus source:string; must authenticate server producer, never trust submitted labels.
- decide(caller, action:string, resource:{tenantId:string;source:string}): Promise<{allowed:boolean;reason:string}>. Only explicit true permits.
- now(): {instant:string;quality:'verified'|'degraded'|'unknown'}; UTC RFC3339 with Z, monotonic receipt times.
- localSink.write(record:RecordEntry, signal:AbortSignal): Promise<'delivered'|'refused'|'uncertain'>. Host supplies bounded sink, honours abort and independently governs persistence, retention and legal holds. Never log raw payloads. Acknowledgement does not guarantee durability.
- rules: array of {source:string;code:string;purpose:string;attributes:readonly string[]}; trusted fixed allowlist, only approved non-personal numeric/boolean fields.

## Client and server functions

Server only. submit(event, {version:1,code,severity,occurrenceTime,correlation?,attributes?}) returns Promise<Receipt>. Severity debug/info/warn/error/fatal. Attributes numeric or boolean only, maximum 16, magnitude <= 1e12; finite numbers. Codes and correlation are opaque approved identifiers, never sensitive values. Producer and tenant come from trusted resolver. Unknown attributes, extra envelope fields and accessor properties reject.

Receipt: {status:'accepted'|'rejected'|'throttled'|'unavailable'|'uncertain';code:string;id?:string}. Codes LOG_ACCEPTED, LOG_DENIED, LOG_INVALID, LOG_RULE, LOG_RATE, LOG_CAPACITY, LOG_BUSY, LOG_SINK_REFUSED, LOG_SINK_UNCERTAIN, LOG_PROVIDER. Accepted means local sink acknowledged processing, not durable storage, business success or Audit custody. Uncertain local delivery closes intake until host recomposes. External delivery is separate from acceptance.

RecordEntry: id, tenantId, source, code, severity, occurrenceTime, receiptTime, timeQuality, optional correlation, readonly attributes, policyRevision, purpose, category and classification ('technical').

readDiagnostics(event,{tenantId,source,from,to,limit,offset?,severity?,code?,correlation?,id?}) returns status completed/rejected/unavailable and records; completed includes coverage warning. UTC receipt time range at most 30 days, limit 1–100, nonnegative integer offset. Process-local, mutable pagination; no total count. Expiry occurs during submit/read.

hostHealth() exposes deployment-wide counts/state to trusted composition root only; do not expose to tenant callers. close() stops intake, signals abort and clears local history; an in-flight write may still finish. shutdown() additionally waits up to timeoutMs and returns completed or uncertain.

## Permissions

logging.submit, logging.read, logging.delivery.read, logging.policy.read on explicit tenant/source. Reads require matching trusted tenant and affirmative resource decision before lookup. No cross-tenant override. Host owns assurance policy.

## Screens

No presentation shipped. The optional Nuxt adapter exports provideLogging(app, options), getLogging(app) and closeLogging(app) from '@nuxt-layers/logging/nuxt/server'. Compose once in a Nitro plugin using the same core options specialised to H3Event; duplicate composition throws LOG_ALREADY_COMPOSED. getLogging returns undefined before composition. Register closeLogging on the Nitro close hook. No automatic access grants, browser intake or endpoints. Nuxt 4.6.4 compatibility is targeted, not verified. The host owns Nuxt and h3 dependencies. The example is deny-all, not a production provider. The approved six screens remain outstanding.

## Headless surface

Intake, local acknowledgement, scoped local read and composition-root health. External routing, effective policy projection and scoped delivery state are available. Privileged policy changes, protected export, legal hold workflow, lifecycle events and persistent recovery remain outstanding.

## You will hit this

Identifiers match ASCII alphanumeric first character followed by up to 95 alphanumeric, underscore, dot, colon or hyphen characters. Times use UTC Z. Only one submission in flight; concurrent calls receive unavailable. Local sink timeout is uncertain and stops new intake. External uncertainty quarantines only the affected destination until recomposition. Producer retry may duplicate; no deduplication promise. Do not use this subset for regulated retention or evidence custody. No real IAM/Clock/Configuration integration verified.

### Optional policy and delivery ports

Rule also accepts category (default technical), minimumSeverity (debug), sampleEvery (1), destinations (empty). Deployment rules are immutable maxima. Optional resolvePolicy({tenantId,source}) returns Promise<{revision:string;rules:readonly Rule[]}|null>; missing or invalid effective policy fails closed. It may remove rules/fields/destinations, raise minimum severity or narrow sampling by a multiple of the deployment interval; purpose/category cannot change. Host supplies approved effective policies only, with governance and freshness verified upstream. Rules apply per source; sampling uses the source/tenant minute admission counter. Filtered returns rejected/LOG_FILTERED; missing policy returns unavailable/LOG_POLICY_UNAVAILABLE; invalid provider policy returns LOG_PROVIDER.

externalSinks: array of {id:string;sink:Sink;maxAttempts?:number}, at most eight, default one attempt, max three. Destination IDs are approved in rules. An explicit refused means definitely no write and may retry immediately; timeout, exception or unknown result is uncertain and never retried. External delivery is synchronous after local acknowledgement and bounded by configured per-attempt timeout. Sink adapter must be bounded and honour abort. No automatic restart/replay or delivery durability. Delivery attempt time uses host system UTC (not verified clock quality).

readDeliveryState(event, DiagnosticQuery) returns completed/rejected/unavailable and records [{id,delivery:[{target,status:'delivered'|'exhausted'|'uncertain',attempts,time}]}] with coverage warning on completion. Requires logging.delivery.read, independently of logging.read. readEffectivePolicy(event,{tenantId,source}) returns completed with policy or rejected/unavailable; requires logging.policy.read. No secrets or provider configuration exposed. hostHealth additionally returns quarantined destination IDs; deployment-only.
