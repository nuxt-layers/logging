# Logging public consumer contract

Contract revision 1 (diagnostic envelope version 1). Development interface; compatibility and runtime acceptance are not yet verified.

Logging is a framework-independent technical diagnostic service. It owns sanitised diagnostic records and their processing/delivery projections. It does not own business transaction outcomes, Audit evidence custody, Observability metrics/traces, Reporting, IAM or legal decisions. Routine Logging failure does not undo a committed business operation.

## Install

```json
{"@nuxt-layers/logging":"github:nuxt-layers/logging#master"}
```

Use the accepted development revision; this document describes the current development interface. Node 26.13+ with native TypeScript stripping is targeted. No runtime dependency is required by the core. Nuxt 4.6.4 is an optional peer target; neither runtime/package integration has yet passed acceptance. Import only from the package root or documented Nuxt subpaths.

```ts
import { createLogging, type Options } from '@nuxt-layers/logging'
export function compose<E>(host: Options<E>) {
  return createLogging(host)
}
```

The host supplies the following trusted ports and policy values once per service instance. Application artefacts consume its returned methods, not the composition-root providers or private source.

## Settings

All settings are composition options; no core environment variables.

| Option | Default / limit | Meaning |
|---|---|---|
| capacity | 1000; 1–10000 | Maximum retained records and rate-bucket cardinality |
| ratePerMinute | 120; 1–10000 | Admission limit per trusted tenant/source per UTC minute |
| timeoutMs | 1000; 1–30000 | Per sink attempt and shutdown wait bound |
| retentionMs | Required; 1–2592000000 | Process-local receipt-age expiry; not provider retention |
| policyRevision | Required identifier | Deployment policy provenance |
| rules | Required array, up to 10000 | Deployment admission and routing maxima |
| externalSinks | Empty; max 8 | Optional approved destinations, unique IDs other than local |
| resolvePolicy | Absent | Optional host resolution of approved effective rules; absent uses deployment rules |

Rule defaults: category technical, minimumSeverity debug, sampleEvery 1, destinations empty. Sampling interval 1–10000; destination attempts 1–3 (default 1). All limits are integers. Invalid composition can throw LOG_INVALID_CONFIG, LOG_INVALID_SINK, LOG_INVALID_RULE or LOG_DUPLICATE_RULE.

## Host passes

### Public inputs supplied by the wider application

The named artefacts identify conceptual owners, not mandatory package imports or verified live integrations. The host connects their published contracts where available.

| Input | Responsible supplier | Public entry | Availability |
|---|---|---|---|
| Trusted caller | Host / IAM Integration | resolveCaller(event) | Implemented port |
| Trusted producer | Host / IAM Integration | resolveProducer(event) | Implemented port |
| Resource access decision | Authorisation, host-composed | decide(caller, action, resource) | Implemented port |
| UTC receipt time and quality | Clock or trusted host | now() | Implemented port |
| Approved deployment rules and limits | Host / Configuration | Options | Implemented |
| Effective scoped rules and revision | Configuration, host-composed | resolvePolicy(scope) | Optional implemented port |
| Sink delivery acknowledgement/refusal/uncertainty | Host local and optional external sinks | Sink.write return | Implemented port |
| Diagnostic submission | Trusted producers, including Scheduler, Identity, Storage and other application artefacts | submit | Implemented |
| Diagnostic and delivery query | Authorised consumers | readDiagnostics / readDeliveryState | Implemented |
| Effective policy query | Authorised administrators | readEffectivePolicy | Implemented |
| Shutdown | Composition root | close / shutdown | Implemented |
| Local export, restriction and erasure requests | Authorised operators, Privacy, Legal Compliance | exportDiagnostics / restrictDiagnostic / eraseDiagnostic | Implemented, local projection only |
| Independent lifecycle approval and preservation decision | Host-composed qualified authorities | approveLifecycle | Optional implemented port; missing blocks commands |
| Scoped pipeline health query | Authorised operators / Observability | readPipelineHealth | Implemented, retained records only |
| Privileged policy change, provider rights/hold/recovery commands | Authorised operators, Privacy, Legal Compliance | No callable API | Approved; unavailable |

The following are the exported TypeScript input and output types (package root):

```ts
export type Severity = 'debug' | 'info' | 'warn' | 'error' | 'fatal'
export type Caller = { tenantId: string; subjectId: string | null; assurance: unknown }
export type Producer = Caller & { source: string }
export type Diagnostic = { version: 1; code: string; severity: Severity; occurrenceTime: string; correlation?: string; attributes?: Record<string, number | boolean> }
export type RecordEntry = Readonly<{ id: string; tenantId: string; source: string; code: string; severity: Severity; occurrenceTime: string; receiptTime: string; timeQuality: 'verified' | 'degraded' | 'unknown'; correlation?: string; attributes: Readonly<Record<string, number | boolean>>; policyRevision: string; purpose: string; category: string; classification: 'technical' }>
export type Delivery = 'delivered' | 'refused' | 'uncertain'
export type Receipt = { status: 'accepted' | 'rejected' | 'throttled' | 'unavailable' | 'uncertain'; code: string; id?: string }
export type Sink = { write(record: RecordEntry, signal: AbortSignal): Promise<Delivery> }
export type Rule = { source: string; code: string; attributes: readonly string[]; purpose: string; category?: string; minimumSeverity?: Severity; sampleEvery?: number; destinations?: readonly string[] }
export type DiagnosticQuery = { tenantId:string; source:string; from:string; to:string; limit:number; offset?:number; severity?:Severity; code?:string; correlation?:string; id?:string }
export type Policy = { revision:string; rules:readonly Rule[] }
export type ExternalSink = { id:string; sink:Sink; maxAttempts?:number }
export type DeliveryState = { target:string; status:'delivered'|'exhausted'|'uncertain'; attempts:number; time:string }
export type LifecycleAction = 'export' | 'erase' | 'restrict'
export type LifecycleTarget = { tenantId:string; source:string; id:string }
export type LifecycleApproval = {
  action:LifecycleAction
  caller:Caller
  requestId:string
  target:LifecycleTarget | DiagnosticQuery
}
export type LifecycleDecision =
  | { status:'approved'; evidenceRef:string; preservation:'clear'|'held'|'unknown' }
  | { status:'blocked'|'unknown' }
export type LifecycleResult =
  | { status:'completed-local'; requestId:string; evidenceRef:string; providerState:'unknown' }
  | { status:'blocked'|'rejected'|'unavailable'|'uncertain' }
export type ExportResult =
  | { status:'completed-local'; requestId:string; evidenceRef:string; records:RecordEntry[]; coverage:string }
  | { status:'blocked'|'rejected'|'unavailable'|'uncertain'; records:never[] }
export type Options<E> = {
  resolveCaller(event: E): Promise<Caller | null>
  resolveProducer(event: E): Promise<Producer | null>
  decide(caller: Caller, action: string, resource: { tenantId: string; source: string }): Promise<{ allowed: boolean; reason: string }>
  now(): { instant: string; quality: 'verified' | 'degraded' | 'unknown' }
  localSink: Sink
  rules: readonly Rule[]
  policyRevision: string
  capacity?: number
  retentionMs: number
  ratePerMinute?: number
  timeoutMs?: number
  externalSinks?: readonly ExternalSink[]
  approveLifecycle?: (request:LifecycleApproval) => Promise<LifecycleDecision>
  resolvePolicy?: (scope:{tenantId:string;source:string}) => Promise<Policy | null>
}
```

Host responsibilities:

- E is the host's trusted execution/request context type; do not accept arbitrary browser identity, tenant, producer or assurance labels. resolveProducer authenticates the service/source. resolveCaller resolves the current principal independently.
- Only an explicit positive decide result permits an operation. Read access additionally requires the caller's tenant to match the requested tenant. No implicit parent/platform-admin rights. Host validates assurance, group/resource scope and policy freshness.
- now returns UTC Z time with nondecreasing receipt timestamps. Quality is verified, degraded or unknown; it is recorded, not fabricated. Invalid or regressing time fails processing (internal LOG_CLOCK, surfaced as LOG_PROVIDER during intake or unavailable during reads).
- localSink is required and bounded. Sink.write receives only a sanitised RecordEntry and an abort signal; delivered acknowledges the write according to that sink's semantics, refused guarantees no write, uncertain means a write may have occurred. Providers must honour cancellation and bound work. Host independently governs sink storage, residency, access, retention and legal holds.
- Rules contain approved source/code, purpose and numeric/boolean attribute allowlists (max 16), category and optional severity/sampling/destination controls. No passwords, credentials, session tokens, payment data, personal identifiers or raw payloads. Approved identifier provenance and non-personal numeric semantics are host responsibilities.
- Optional resolvePolicy returns an approved, current source-scoped policy or null. Effective rules may remove deployment rules, fields or destinations, raise minimum severity or narrow sampling by a multiple of the deployment interval. They cannot change purpose/category or widen deployment rules. Null/invalid policy fails closed; no silent fallback. Host verifies policy authority, effective period and legal constraints; Logging does not own Configuration's approval workflow.
- No real IAM, Clock, Configuration, Audit, Privacy or Observability provider integration is verified. No mandatory direct dependency on those artefacts.

## Client and server functions

Server only; no browser-facing API. The service type can be derived publicly as ReturnType<typeof createLogging<E>>. Its callable surface is:

```ts
type QueryFailure = { status: 'rejected' | 'unavailable'; records: never[] }
type DiagnosticResult =
  | { status: 'completed'; records: RecordEntry[]; coverage: string }
  | QueryFailure
type DeliveryResult =
  | { status: 'completed'; records: { id: string; delivery: DeliveryState[] }[]; coverage: string }
  | QueryFailure
type PolicyResult =
  | { status: 'completed'; policy: Policy }
  | { status: 'rejected' | 'unavailable' }
type HostHealth = {
  state: 'degraded' | 'busy' | 'unknown'
  lastDelivery: Delivery | 'unknown'
  drops: number
  uncertain: number
  buffered: number
  quarantined: string[]
}
// Descriptive result aliases above are not separate package exports.
interface LoggingSurface<E> {
  submit(event: E, input: Diagnostic): Promise<Receipt>
  readDiagnostics(event: E, query: DiagnosticQuery): Promise<DiagnosticResult>
  readDeliveryState(event: E, query: DiagnosticQuery): Promise<DeliveryResult>
  readEffectivePolicy(event: E, scope: {tenantId:string;source:string}): Promise<PolicyResult>
  readPipelineHealth(event: E, query: DiagnosticQuery): Promise<
    {status:'completed';records:number;delivered:number;exhausted:number;uncertain:number;coverage:string}
    | {status:'rejected'|'unavailable'}>
  exportDiagnostics(event:E,query:DiagnosticQuery,requestId:string):Promise<ExportResult>
  eraseDiagnostic(event:E,target:LifecycleTarget,requestId:string):Promise<LifecycleResult>
  restrictDiagnostic(event:E,target:LifecycleTarget,requestId:string):Promise<LifecycleResult>
  hostHealth(): HostHealth
  close(): void
  shutdown(): Promise<{status:'completed'|'uncertain'}>
}
```

### Public outputs supplied to the wider application

| Output | Consumer | Source | Availability |
|---|---|---|---|
| Intake receipt | Diagnostic producer | submit | Implemented |
| Sanitised diagnostic envelope | Host-composed sinks | Sink.write input | Implemented |
| Scoped diagnostic records | Authorised support/application/GUI consumer | readDiagnostics | Implemented, process-local |
| Scoped record delivery state | Authorised operations / Observability consumer | readDeliveryState | Implemented, process-local |
| Effective policy projection | Authorised administrators | readEffectivePolicy | Implemented |
| Deployment pipeline health | Trusted composition root | hostHealth | Implemented, not tenant-scoped |
| Shutdown result | Composition root | shutdown | Implemented |
| Scoped retained-record health | Authorised operators / Observability | readPipelineHealth | Implemented, incomplete |
| Local export, restriction and erasure outcomes | Authorised operators / Privacy / Legal Compliance | exportDiagnostics / restrictDiagnostic / eraseDiagnostic | Implemented, not provider custody |
| Provider retention/hold/rights/recovery outcomes | Authorised operators / Privacy / Legal Compliance | No callable API | Approved; unavailable |
| Committed lifecycle events | Audit / Observability | No event or relay API | Approved; unavailable |

### Diagnostic intake

Diagnostic.version must be 1. Code identifies an approved source event; severity is debug/info/warn/error/fatal; occurrenceTime is producer-supplied UTC Z time, distinct from trusted receipt time. Optional correlation is an opaque safe reference. Attributes are finite numbers (absolute magnitude <= 1e12) or booleans, max 16, allowlisted. Unexpected envelope fields, unknown attributes and accessor properties reject during validation. Tenant/source and classification/purpose/category are supplied or derived by trusted context/policy, not by the diagnostic payload. No raw error, free text, body, credential or personal information. Validate/limit input transport upstream.

Receipt.status and code:

| Status | Code | Meaning |
|---|---|---|
| accepted | LOG_ACCEPTED | Local sink acknowledged; opaque id assigned |
| rejected | LOG_DENIED | Producer or permission refused |
| rejected | LOG_RULE | No matching effective rule |
| rejected | LOG_INVALID | Invalid envelope or attributes |
| rejected | LOG_FILTERED | Severity or sampling exclusion |
| throttled | LOG_RATE | Admission rate/cardinality exceeded |
| unavailable | LOG_BUSY | Closed, closing or another submission active |
| unavailable | LOG_CAPACITY | History full or shutdown in progress |
| unavailable | LOG_POLICY_UNAVAILABLE | Effective policy missing |
| unavailable | LOG_SINK_REFUSED | Local sink confirmed no write |
| unavailable | LOG_PROVIDER | Provider, clock, policy validation or processing error |
| uncertain | LOG_SINK_UNCERTAIN | Local write may have occurred; id supplied; intake closes |

Acceptance is not durable persistence, remote delivery, business success or Audit custody. A provider error does not establish that no effect occurred. Producer retries can duplicate records; no idempotency guarantee.

### Diagnostic records and queries

RecordEntry fields: id (opaque UUID), tenantId/source (trusted scope), code/severity, occurrenceTime/receiptTime, timeQuality, optional correlation, readonly attributes, policyRevision, purpose/category and classification (technical). Records are sanitised, not raw source data.

DiagnosticQuery requires tenantId, source, UTC receipt-time from/to (inclusive, max 30 days), limit 1–100. Optional offset (nonnegative integer, default 0), severity, code, correlation and id narrow results. Unexpected query fields or accessors reject. No wildcard tenant/source or total count. Rejected means invalid query or denied access; unavailable means processing failed; completed includes coverage warning. Results are process-local, incomplete and may shift with expiry or concurrent submissions. Empty completed results are not proof that no events occurred.

### Delivery and policy projections

readDeliveryState uses the same query bounds, returns record IDs and per-target delivery projections. DeliveryState.target identifies local or configured destination; status delivered/exhausted/uncertain; attempts counts attempted writes (zero if skipped); time is system UTC attempt/result time, not verified Clock quality. In-flight destinations may be omitted. Explicit refusal alone permits bounded immediate retry of an external write (max three attempts). Timeout, exception or invalid sink result is uncertain; that external destination is quarantined until recomposition. Local uncertainty closes intake. No automatic restart/replay or confirmed provider deletion. External attempts are synchronous after local acknowledgement; acceptance waits for the bounded attempts.

readEffectivePolicy accepts exactly tenantId and source and returns the authorised scoped rules and revision, not credentials or provider secrets. Missing/invalid policy returns unavailable. It is a projection, not a policy mutation API.

hostHealth is deployment-wide and must remain at the trusted composition root: state degraded if closed/quarantined, busy if active, otherwise unknown; lastDelivery is last local acknowledgement outcome or unknown; drops counts selected admission exclusions/refusals/exhausted deliveries (not all losses); uncertain counts uncertain deliveries; buffered counts retained records; quarantined contains destination IDs. No completeness or healthy-business-process claim. Do not expose this method or aggregate data to tenant callers.

close stops intake, signals abort and clears local history/rate buckets. shutdown additionally waits up to timeoutMs for the active submission, returning completed if settled or uncertain if wait expired. Neither proves provider deletion or cancellation. Host must bound caller/policy provider latency.

### Protected local lifecycle and health

approveLifecycle is an optional host-supplied port. It receives the action, authenticated human subject/caller, opaque requestId and exact tenant/source target or query. The host must independently establish appropriate assurance, current authority, purpose, attributable evidence and applicable preservation obligations, then return approved with an opaque evidenceRef and clear/held/unknown preservation, or blocked/unknown. Never fabricate an approval. Missing, malformed or failed approval blocks execution. The approval reference is not a committed Audit receipt. No real provider integration is asserted.

exportDiagnostics accepts the same bounded query as readDiagnostics, plus requestId. Returns completed-local with records and evidence reference, or rejected/blocked/unavailable/uncertain and empty records. It excludes restricted records. This is a bounded in-memory projection, not a portable export from every sink. Recheck permission and the same authenticated subject after approval before disclosure.

eraseDiagnostic and restrictDiagnostic accept exact tenantId/source/id plus requestId. Erasure requires explicit clear preservation; held blocks and unknown fails closed. Restriction hides the retained record from reads, exports, delivery projections and scoped health. Recheck permission and the same authenticated subject after approval. completed-local means the local projection was removed or hidden, or was already absent; no existence oracle or provider deletion confirmation. providerState is always unknown. No durable restriction or legal-hold enforcement across sinks, restarts or cache expiry; do not use this for regulated rights fulfilment. Request IDs support correlation only, not durable idempotency; repeat requests require independent approval again. Privileged operations serialize with intake; busy returns unavailable. Shutdown does not wait for approval ports; no local change follows a closed instance.

readPipelineHealth uses the same validated tenant/source/time/filter query and separate permission. Returns completed with retained record count and delivered/exhausted/uncertain destination counts, or rejected/unavailable. Pagination is ignored for aggregates; limit is still required. Counts exclude restricted, expired, refused-before-retention and unobserved records; no global loss or coverage claim.

## Permissions

| Action | Resource | Risk/effect |
|---|---|---|
| logging.submit | Trusted producer tenantId/source | Admit technical diagnostics only |
| logging.read | Requested tenantId/source | Reveal sanitised diagnostic records |
| logging.delivery.read | Requested tenantId/source | Reveal scoped delivery state |
| logging.policy.read | Requested tenantId/source | Reveal scoped effective policy |
| logging.health.read | Requested tenantId/source | Reveal retained-record aggregates |
| logging.export | Requested tenantId/source | Protected local diagnostic disclosure |
| logging.erase | Requested tenantId/source | Remove local projection; clear preservation required |
| logging.restrict | Requested tenantId/source | Hide local projection |

All require affirmative server-side resource decisions; queries require matching trusted tenant before lookup. Separate permissions are not interchangeable. hostHealth, close and shutdown are composition-root-only, not user-facing permissions. No cross-tenant override or privilege bypass. Local protected commands additionally require independently attributable approval. Provider-level rights and privileged policy mutation remain unavailable.

## Screens

No screens or HTTP endpoints currently shipped. Optional Nuxt layer:

```ts
export default defineNuxtConfig({ extends: ['@nuxt-layers/logging/nuxt'] })
```

In one host Nitro plugin, import provideLogging, getLogging and closeLogging from '@nuxt-layers/logging/nuxt/server'. Given the Nitro application and trusted host Options<H3Event>, call provideLogging(app, options) once; it returns the same service surface specialised to H3Event. getLogging(app) returns service or undefined; duplicate registration throws LOG_ALREADY_COMPOSED. Register app.hooks.hook('close', () => closeLogging(app)); this closes and unregisters the service. Host supplies Nuxt and h3. No automatic registration, permission grant or browser intake. runtimeConfig.logging.presentation defaults false; no pages exist even if changed. Nuxt compatibility is targeted, not verified. Approved future routes: /logging, /logging/records, /logging/records/:id, /logging/delivery, /logging/settings, /logging/data-management. No presentation messages or routes are published yet.

## Headless surface

Implemented: validated diagnostic admission, sanitised sink output, scoped reads, delivery projection, effective policy projection, scoped retained-record health, protected local export/restriction/erasure, host health and shutdown. These work without a GUI, Nuxt or external destinations. No mandatory database.

Approved but unavailable: governed privileged policy mutations, durable/provider export, retention/hold/restriction/erasure commands and results, complete scoped loss/backlog health, committed lifecycle events and durable outbox/relay, persistent recovery and manual retry. Do not call invented methods or substitute Logging receipts for Audit evidence. Missing provider contracts must be reconciled before claiming those integrations.

## You will hit this

Identifiers use ASCII alphanumeric first character, then up to 95 alphanumeric/underscore/dot/colon/hyphen characters. Syntax is not proof of safe provenance. UTC timestamps use YYYY-MM-DDTHH:mm:ss[.SSS]Z with 1–3 optional fractional digits. Only one submission in flight; concurrent calls receive unavailable. Policy sampling uses the tenant/source minute admission counter. Expiry is lazy on submit/read; history is not durable and provider retention is independent. Shutdown and local uncertainty do not prove write reversal. Never use this subset for regulated retention, evidence custody or legal hold fulfilment.

Compatibility: version 1 envelope and named receipt categories/codes are the current development contract; no released compatibility assurance. Breaking field, result or semantic changes require coordinated contract revision and affected consumer acceptance. Tests and package/Nuxt integration acceptance remain pending.
