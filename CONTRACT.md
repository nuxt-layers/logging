# Logging public consumer contract

Contract revision 1 (diagnostic envelope version 1). Development interface; compatibility and runtime acceptance are not yet verified.

Logging is a framework-independent technical diagnostic service. It owns sanitised diagnostic records and their processing/delivery projections. It does not own business transaction outcomes, Audit evidence custody, Observability metrics/traces, Reporting, IAM or legal decisions. Routine Logging failure does not undo a committed business operation.

## Install

```json
{"@nuxt-layers/logging":"github:nuxt-layers/logging#master"}
```

Use the accepted development revision; this document describes the current development interface. Node 26.13+ runs compiled ESM JavaScript from dist/index.js; declarations are published at dist/index.d.ts. Git-backed packages include generated dist files, built from source with the pinned TypeScript compiler. Maintainers regenerate and commit dist on source changes; CI checks reproducibility. Consumers do not compile Logging or enable TypeScript stripping in node_modules. No runtime dependency is required by the core. Nuxt 4.6.4 is an optional peer target; neither runtime/package integration has yet passed acceptance. Import only from the package root or documented subpaths.

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
| Durable diagnostic persistence | Host-composed Logging-owned store | diagnosticStore | Optional implemented port; SQLite adapter |
| Preservation decision for maintenance | Qualified host authority | retentionAuthority.resolvePreservation | Optional; missing prevents maintenance |
| Durable lifecycle state and outbox | Host-composed Logging-owned journal | lifecycleJournal | Optional implemented port; SQLite adapter provided |
| Independent lifecycle approval and preservation decision | Host-composed qualified authorities | approveLifecycle | Optional implemented port; missing blocks commands |
| Scoped pipeline health query | Authorised operators / Observability | readPipelineHealth | Implemented, retained records only |
| Privileged policy change, provider rights/hold/recovery commands | Authorised operators, Privacy, Legal Compliance | No callable API | Approved; unavailable |

The following are the exported TypeScript input and output types (package root):

```ts
export type Severity = 'debug' | 'info' | 'warn' | 'error' | 'fatal'
export type Caller = { tenantId: string; subjectId: string | null; assurance: unknown }
export type Producer = Caller & { source: string }
export type Diagnostic = { version: 1; code: string; severity: Severity; occurrenceTime: string; correlation?: string; attributes?: Record<string, number | boolean> }
export type RecordEntry = Readonly<{ id: string; tenantId: string; source: string; code: string; severity: Severity; occurrenceTime: string; receiptTime: string; timeQuality: 'verified' | 'degraded' | 'unknown'; correlation?: string; attributes: Readonly<Record<string, number | boolean>>; policyRevision: string; purpose: string; category: string; classification: 'technical'; retentionMs?:number }>
export type Delivery = 'delivered' | 'refused' | 'uncertain'
export type Receipt = { status: 'accepted' | 'rejected' | 'throttled' | 'unavailable' | 'uncertain'; code: string; id?: string }
export type Sink = { write(record: RecordEntry, signal: AbortSignal): Promise<Delivery> }
export type Rule = { source: string; code: string; attributes: readonly string[]; purpose: string; category?: string; minimumSeverity?: Severity; sampleEvery?: number; destinations?: readonly string[] }
export type DiagnosticQuery = { tenantId:string; source:string; from:string; to:string; limit:number; offset?:number; severity?:Severity; code?:string; correlation?:string; id?:string }
export type Policy = { revision:string; rules:readonly Rule[] }
export type ExternalSink = { id:string; sink:Sink; maxAttempts?:number }
export type DeliveryState = { target:string; status:'delivered'|'exhausted'|'uncertain'; attempts:number; time:string }
export type LifecycleAction = 'export' | 'erase' | 'restrict' | 'recover' | 'policy.change'
export type LifecycleTarget = { tenantId:string; source:string; id:string }
export type LifecycleApproval = {
  action:LifecycleAction
  caller:Caller
  requestId:string
  destination?:string
  target:LifecycleTarget | DiagnosticQuery | PolicyChange
}
export type LifecycleDecision =
  | { status:'approved'; evidenceRef:string; preservation:'clear'|'held'|'unknown' }
  | { status:'blocked'|'unknown' }
export type LifecycleResult =
  | { status:'completed-local'|'committed'; requestId:string; evidenceRef:string; providerState:'unknown' }
  | { status:'blocked'|'rejected'|'unavailable'|'uncertain' }
export type ExportResult =
  | { status:'completed-local'; requestId:string; evidenceRef:string; records:RecordEntry[]; coverage:string }
  | { status:'blocked'|'rejected'|'unavailable'|'uncertain'; records:never[] }
export type LifecycleEvent = Readonly<ExternalObservation & {version:1;id:string;type:'logging.external.confirmation.observed'}> | Readonly<{
  version:1; id:string; type:'logging.restricted'|'logging.erased'; tenantId:string; source:string;
  recordId:string; requestId:string; subjectId:string; evidenceRef:string; committedAt:string
}> | Readonly<{
  version:1; id:string; type:'logging.expired'; tenantId:string; source:string; recordId:string; committedAt:string
}> | Readonly<{
  version:1; id:string; type:'logging.export.released'; tenantId:string; source:string; requestId:string; subjectId:string; evidenceRef:string; committedAt:string; count:number
}>
export type ExternalObservation = {tenantId:string;source:string;requestId:string;subjectId:string;evidenceRef:string;committedAt:string} & (
  {kind:'provider';action:'erase'|'restrict'|'recover';recordId:string;destination:string} |
  {kind:'policy';expectedRevision:string;revision:string}
)
export type ExternalIntent = Omit<ExternalObservation,'committedAt'|'evidenceRef'|'kind'> & (
  {kind:'provider';action:'erase'|'restrict'|'recover';recordId:string;destination:string} |
  {kind:'policy';expectedRevision:string;revision:string}
)
export type JournalHealth = {capacity:number;requests:number;markers:number;events:number;reservations:number;pending:number;oldestPendingAt:string|null}
export type ExportRelease = { tenantId:string; source:string; requestId:string; subjectId:string; evidenceRef:string; committedAt:string; count:number }
export type LifecycleCommit = {
  action:'restrict'|'erase'; target:LifecycleTarget; requestId:string; subjectId:string;
  evidenceRef:string; committedAt:string
}
export type StoredDiagnostic = { record:RecordEntry; delivery:DeliveryState[] }
export type DiagnosticStore = LifecycleJournal & {
  save(entry:StoredDiagnostic,capacity:number):Promise<'stored'|'full'|'uncertain'>
  load(tenantId:string,source:string,limit:number):Promise<StoredDiagnostic[]>
  updateDelivery(target:LifecycleTarget,delivery:DeliveryState[]):Promise<void>
  expired(before:string,limit:number,after?:LifecycleTarget):Promise<LifecycleTarget[]>
  expire(target:LifecycleTarget,before:string,committedAt:string):Promise<'removed'|'absent'>
  retentionRecord(target:LifecycleTarget):Promise<RecordEntry|null>
}
export type PreservationDecision = 'clear'|'held'|'unknown'
export type OptionsRetention = {
  resolvePreservation(target:LifecycleTarget):Promise<PreservationDecision>
}
export type JournalMaintenance = {
  acknowledgedBefore(before:string,limit:number,after?:string):Promise<LifecycleEvent[]>
  removeAcknowledged(eventId:string,before:string):Promise<'removed'|'absent'>
}
export type JournalRetentionAuthority = {
  retentionMs:number
  resolvePreservation(event:LifecycleEvent):Promise<PreservationDecision>
}
export type LifecycleJournal = {
  commit(command:LifecycleCommit):Promise<'committed'|'duplicate'|'conflict'|'uncertain'|'full'>
  reserveExternal?(intent:ExternalIntent):Promise<'reserved'|'conflict'|'uncertain'|'full'>
  health?():Promise<JournalHealth>
  recordExternalObservation?(observation:ExternalObservation):Promise<'committed'|'duplicate'|'conflict'|'uncertain'|'full'>
  recordExportRelease?(release:ExportRelease):Promise<'committed'|'conflict'|'uncertain'|'full'>
  visibility(target:LifecycleTarget):Promise<'visible'|'restricted'|'erased'>
  pending(limit:number):Promise<LifecycleEvent[]>
  acknowledge(eventId:string,acknowledgedAt?:string):Promise<void>
  maintenance?: JournalMaintenance
}
export type LifecyclePublisher = (event:LifecycleEvent,signal:AbortSignal)=>Promise<Delivery>
export type ProviderLifecycleRequest = {
  action:'erase'|'restrict'|'recover'; target:LifecycleTarget; destination:string; requestId:string;
  subjectId:string; evidenceRef:string
}
export type ProviderLifecycleResult =
  | {status:'confirmed'; evidenceRef:string}
  | {status:'blocked'|'rejected'|'unavailable'|'uncertain'}
export type PolicyChange = {tenantId:string;source:string;expectedRevision:string;policy:Policy;requestId:string}
export type PolicyChangeResult = {status:'committed';revision:string;evidenceRef:string} | {status:'rejected'|'blocked'|'conflict'|'unavailable'|'uncertain'}
export type AccessResource = { tenantId:string; source:string; id?:string; destination?:string; requestId?:string; expectedRevision?:string; revision?:string; from?:string; to?:string; severity?:Severity; code?:string; correlation?:string; limit?:number; offset?:number }
export type Options<E> = {
  resolveCaller(event: E): Promise<Caller | null>
  resolveProducer(event: E): Promise<Producer | null>
  decide(caller: Caller, action: string, resource: AccessResource): Promise<{ allowed: boolean; reason: string }>
  now(): { instant: string; quality: 'verified' | 'degraded' | 'unknown' }
  localSink: Sink
  rules: readonly Rule[]
  policyRevision: string
  capacity?: number
  retentionMs: number
  purposeRetention?: Readonly<Record<string,number>>
  ratePerMinute?: number
  timeoutMs?: number
  externalSinks?: readonly ExternalSink[]
  lifecycleJournal?: LifecycleJournal
  diagnosticStore?: DiagnosticStore
  retentionAuthority?: OptionsRetention
  journalRetention?: JournalRetentionAuthority
  providerLifecycle?: (request:ProviderLifecycleRequest,signal:AbortSignal)=>Promise<ProviderLifecycleResult>
  commitPolicy?: (change:PolicyChange,caller:Caller,approvalEvidenceRef:string)=>Promise<PolicyChangeResult>
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
  intake: Record<Receipt['status'],number>
  activeIntake: number
  activeAgeMs: number | null
  retainedProcessLocal: number
  storage: 'durable' | 'process-local'
  quarantined: string[]
}
// Descriptive result aliases above are not separate package exports.
interface LoggingSurface<E> {
  submit(event: E, input: Diagnostic): Promise<Receipt>
  readDiagnostics(event: E, query: DiagnosticQuery): Promise<DiagnosticResult>
  readDeliveryState(event: E, query: DiagnosticQuery): Promise<DeliveryResult>
  readEffectivePolicy(event: E, scope: {tenantId:string;source:string}): Promise<PolicyResult>
  readPipelineHealth(event: E, query: DiagnosticQuery): Promise<
    {status:'completed';observedAt:string;timeQuality:'verified'|'degraded'|'unknown';storage:'durable'|'process-local';withoutDelivery:number;records:number;delivered:number;exhausted:number;uncertain:number;coverage:string}
    | {status:'rejected'|'unavailable'}>
  exportDiagnostics(event:E,query:DiagnosticQuery,requestId:string):Promise<ExportResult>
  eraseDiagnostic(event:E,target:LifecycleTarget,requestId:string):Promise<LifecycleResult>
  restrictDiagnostic(event:E,target:LifecycleTarget,requestId:string):Promise<LifecycleResult>
  runProviderLifecycle(event:E,request:Pick<ProviderLifecycleRequest,'action'|'target'|'destination'|'requestId'>):Promise<ProviderLifecycleResult>
  changePolicy(event:E,input:PolicyChange):Promise<PolicyChangeResult>
  maintainJournal(limit?:number):Promise<{status:'completed'|'unavailable'|'uncertain';removed:number;held:number}>
  maintainRetention(limit?:number):Promise<{status:'completed'|'unavailable'|'uncertain';removed:number;held:number}>
  relayLifecycle(publish:LifecyclePublisher,limit?:number):Promise<{status:'completed'|'unavailable'|'uncertain';published:number}>
  journalHealth(): Promise<({status:'completed';observedAt:string;timeQuality:'verified'|'degraded'|'unknown';oldestPendingAgeMs:number|null} & JournalHealth) | {status:'unavailable'}>
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
| Qualified provider lifecycle outcomes | Authorised operators | runProviderLifecycle | Optional port, unverified provider |
| Policy change outcome | Authorised operator / Configuration | changePolicy | Optional port, unverified provider |
| Committed restriction/erasure lifecycle events | Host relay / Message Queue / Audit / Observability | relayLifecycle / LifecycleEvent | Implemented with journal; delivery integration unverified |

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
| uncertain | LOG_STORE_UNCERTAIN | Durable record or delivery state may have committed; intake closes |
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

eraseDiagnostic and restrictDiagnostic accept exact tenantId/source/id plus requestId. Erasure requires explicit clear preservation; held blocks and unknown fails closed. Restriction hides the retained record from reads, exports, delivery projections and scoped health. Recheck permission and the same authenticated subject after approval. completed-local means the local projection was removed or hidden, or was already absent; no existence oracle or provider deletion confirmation. providerState is always unknown. Without a journal there is no durable restriction across restarts. No legal-hold enforcement across sinks or cache expiry; do not use this for regulated rights fulfilment. Without a journal request IDs support correlation only. Repeat requests always require independent approval again. Privileged operations serialize with intake; busy returns unavailable. Read projections are not transactional snapshots across multiple processes; hosts must coordinate disclosure and lifecycle changes where strict concurrent revocation is required. Shutdown does not wait for approval ports; no local change follows a closed instance.

readPipelineHealth uses the same validated tenant/source/time/filter query and separate permission. Returns completed with retained record count and delivered/exhausted/uncertain destination counts, or rejected/unavailable. Pagination is ignored for aggregates; limit is still required. Counts exclude restricted, expired, refused-before-retention and unobserved records; no global loss or coverage claim.

### Optional durable lifecycle journal and relay

Import createSqliteLifecycleJournal(path, capacity?) from '@nuxt-layers/logging/sqlite' and pass it as lifecycleJournal in Options. This optional Node adapter uses node:sqlite, a persistent host-selected file, WAL and FULL synchronous commits. The host owns file permissions, storage location, backup, retention and close. Do not share the file with unrelated capabilities. No database is required for ordinary diagnostic intake.

The journal atomically commits restriction/erasure state, request identity and a version-1 lifecycle event. Duplicate tenant/source/request with identical action, target, subject and evidence reference is idempotent; changed content is conflict (rejected). Erasure is terminal for that record ID; a subsequent restriction request conflicts. A committed result means the Logging-owned lifecycle state and event committed, not external sink deletion; providerState remains unknown. Uncertain commit closes intake and clears local history; recompose and reconcile via the durable journal. All diagnostic projections check journal visibility; lookup errors fail closed (internal LOG_JOURNAL, surfaced as unavailable). When configured as diagnosticStore (rather than lifecycleJournal alone), the same adapter persists sanitised diagnostic records and delivery projections. Existing holds still require independent preservation approval; journal is not the legal authority.

LifecycleEvent contains stable ID, version, event type, tenant/source, opaque record/request/subject/evidence references and commit UTC. No diagnostic payload. Events describe committed Logging state only, not Audit custody or provider erasure. Host must classify references and restrict event recipients. relayLifecycle(publish, limit=20) is a composition-root-only pull/ack operation, max 100, one relay in flight per instance. Publisher returns delivered only after durable downstream acknowledgement. Refusal or uncertainty leaves the event pending; acknowledgement failure may duplicate delivery. Consumers deduplicate on event ID. Host schedules relay and must use a single active relay per journal; no distributed lease or automatic retries. Acknowledged event payloads may be pruned only through governed journal maintenance; request/tombstone state is retained. SQLite bounds requests, markers and events plus reserved event slots independently to capacity (default 100000, range 1–1000000). Existing data is not deleted when a lower limit is supplied; growth fails closed. This bounds row counts, not database bytes, WAL or backups. No Message Queue integration is presumed. Host bounds journal operations and closes the adapter after all work settles.

### Optional diagnostic persistence and retention maintenance

Pass createSqliteLifecycleJournal(path) as diagnosticStore; it also supplies the lifecycle journal. A different lifecycleJournal alongside it is rejected. The store commits the sanitised envelope before sink invocation and persists observed delivery outcomes. Recovered records are read directly from the scoped store, not replayed to sinks. Empty/incomplete delivery arrays mean no confirmed result was persisted, not successful delivery. A refused or uncertain sink may still leave a diagnostic record. Store uncertainty closes intake; recompose and investigate. The capacity limit applies to persisted records across tenants; held records consume capacity, no silent eviction. Host must use one active service writer per store and coordinate provider/disclosure operations. Store is trusted infrastructure; validate its implementation before deployment.

Ordinary projections exclude records at or beyond their applicable retention limit even when preserved in storage. maintainRetention(limit=20) is composition-root-only, max 100 per call; verified clock and a qualified retentionAuthority are required. Explicit clear permits deletion from Logging's own diagnostic store; held/unknown are retained. No automatic maintenance, provider erasure, legal-authority or physical-media deletion guarantee. Host must ensure preservation decisions remain valid through deletion and coordinate concurrent hold changes. Held records consume capacity but do not permanently block subsequent maintenance pages. Journal tombstones, request fingerprints and events are not deleted by diagnostic retention. Purpose-specific limits are supported through purposeRetention, an optional map from approved rule purpose to positive integer milliseconds no greater than retentionMs. Unknown purposes or invalid limits reject composition. Each new record carries its admission-time limit; reads and maintenance use the minimum of that limit, current purpose limit and global limit, so recomposition cannot extend an admitted limit. Older records without a limit use current settings. Maintenance scans bounded pages by stable tenant/source/record key, advancing past held and nonexpired records and wrapping at the end. This is a process-local cursor, not a completion proof. Governed disposal of request identities and tombstones, and provider-level lifecycle remain outstanding.

### Governed journal maintenance

Capacity exhaustion returns unavailable for new local protected commands and reservations; expiry fails without deleting the record if an event cannot be committed. Existing markers and identities are never removed merely to free capacity. Host must monitor and qualify a safe retirement policy before regulated deployment. journalHealth is composition-root-only: durable counts, reservation count, pending event count and oldest pending timestamp; age is null without verified nonregressing time. No tenant API exposes global counts. Host health intake counters count returned receipt categories since composition; activeAgeMs is process monotonic elapsed time, not UTC. Scoped health includes observation timestamp/quality, storage provenance and count of records with no observed delivery. No claim of complete scoped intake loss.

Read projections recheck permission and reject invalidated local lifecycle generations (LOG_READ_INVALIDATED, surfaced as unavailable); cross-process coordination still belongs to the host.

Optional journalRetention supplies an approved positive retentionMs (maximum 3650 days) and independent resolvePreservation(event) callback. Requires a journal implementing maintenance. maintainJournal(limit=20) is composition-root-only, maximum 100 events per scan, requires verified clock and explicit clear preservation per event. Only acknowledged event payloads with verified acknowledgement timestamps at or before the cutoff are eligible. Unknown acknowledgement times, pending events, held or unknown preservation are never deleted. The adapter rechecks acknowledgement and cutoff at deletion. Rotating scans advance past held entries and wrap; completed means a batch completed, not that the journal is empty. Request identities and restriction/erasure markers remain intact; their governed disposal remains outstanding. Host coordinates concurrent preservation changes and independent evidence retention. No physical-media deletion or backup removal claim. Legacy acknowledged events lacking timestamp are retained.

### Provider lifecycle and policy changes

Durable external operations require reserveExternal and recordExternalObservation. Reserve the scoped identity and one event slot before invocation; collisions reject, matching outstanding reservations return uncertainty without invocation. A full journal fails closed. Confirmation replaces the reservation atomically with observation. Non-confirmed outcomes retain reservations conservatively; only qualified reconciliation may complete them. No automatic replay or deletion. The host must reconcile external authority and ensure provider idempotency, including after restart. Optional providerLifecycle is a qualified host-composed operation for one configured destination (local or named external), exact record and request identity. runProviderLifecycle requires both logging.<action> and logging.provider.<action>, independently attributable approval bound to the destination, and current same-subject permission. Erasure and recovery require clear preservation; recovery additionally requires journal visibility. The provider must atomically recheck preservation, idempotency, record visibility and authority, and return confirmed only with independently verifiable evidence for that destination. Timeout/exception/malformed response is uncertain, never automatically retried. A confirmed provider operation does not imply local erasure or other destinations' completion. No actual external provider is included. The host must serialize concurrent hold/recovery and use bounded, cancellable provider operations.

changePolicy validates the proposed rules against immutable deployment ceilings and the expected revision. Requires logging.policy.change, independent approval bound to the proposal and current same-subject permission. Optional commitPolicy receives change, caller and approval evidence reference; Configuration/host owns the atomic compare-and-set, approved policy persistence, idempotency and independent evidence. Committed means the port confirmed the specified revision, not that all consumers have refreshed. No local shadow policy or fabricated Configuration integration. Missing ports fail closed. Durable export release and external confirmation observations are produced by the journal; they are not authoritative external commits.

## Permissions

Access decisions receive the validated query filters or exact lifecycle record ID, request ID, provider destination and proposed/expected policy revision where applicable. Host authorisation must enforce these resource constraints, not merely tenant membership. The service repeats privileged decisions after independent approval.

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
| logging.recover | Requested tenantId/source | Authorise qualified recovery |
| logging.provider.erase / restrict / recover | Requested tenantId/source | Additional destination operation permission |
| logging.policy.change | Requested tenantId/source | Governed policy change |

All require affirmative server-side resource decisions; queries require matching trusted tenant before lookup. Separate permissions are not interchangeable. hostHealth, close and shutdown are composition-root-only, not user-facing permissions. No cross-tenant override or privilege bypass. Local protected commands additionally require independently attributable approval. Provider-level rights and privileged policy mutation require the optional qualified ports; absent ports fail closed. relayLifecycle, maintainRetention and maintainJournal are composition-root-only.

## Screens

Optional Nuxt presentation and server API are implemented, pending host acceptance:

```ts
export default defineNuxtConfig({ extends: ['@nuxt-layers/logging/nuxt'] })
```

In one host Nitro plugin, import provideLogging, getLogging and closeLogging from '@nuxt-layers/logging/nuxt/server'. Given the Nitro application and trusted host Options<H3Event>, call provideLogging(app, options) once; it returns the same service surface specialised to H3Event. getLogging(app) returns service or undefined; duplicate registration throws LOG_ALREADY_COMPOSED. Register app.hooks.hook('close', () => closeLogging(app)); this closes and unregisters the service. Host supplies Nuxt and h3. No automatic registration, permission grant or browser intake. runtimeConfig.logging.presentation and public.loggingPresentation default false; enable both explicitly, set logging.origin to the exact canonical browser origin, configure host route authentication and permission middleware, host request-size/rate limits, and load @nuxt-layers/logging/messages/en-GB through the host i18n configuration. The layer uses host useI18n; no mandatory i18n package is added to the headless core. Routes /logging, /logging/records, /logging/records/:id, /logging/delivery, /logging/settings, /logging/data-management provide a minimal accessible-form candidate with Theme Manager's public semantic utility classes. Host must include the Logging presentation source in its Tailwind source discovery (export @nuxt-layers/logging/presentation-source resolves the component). Theme Manager remains sole theme authority; no private styles, variables or independent theme state. Host must verify computed token rendering, route guards, focus, contrast and localisation. Theme Manager master 1333f2ee1734d741acb088ec24a69125848c1527 provides default rendering but its production storage composition is explicitly blocked by its public contract; no such integration is asserted. No SSR data preload. Earlier independent Nuxt fixture acceptance passed against 2ef4135; the current revision requires fresh acceptance.

### Protected HTTP surface

POST /api/logging/operation accepts JSON {operation,input,requestId?}; requires exact configured Origin, application/json, x-logging-request: 1, bounded Content-Length (1–32768 bytes), no chunked body. Host ingress must enforce matching body and request-rate limits and same-origin session/CSRF rules. Operations records/delivery/health/policy/export/erase/restrict/provider/policy.change map exactly to the documented service calls. All use trusted server-side caller resolution and separate permissions; no browser submit or composition-root operation. Malformed requests return safe HTTP errors; service outcomes are returned as typed results, not translated into success claims. Disabled/uncomposed service fails closed. No cache or referrer; host must enforce CSP frame-ancestors and TLS. Browser data is transient; generation-based invalidation discards stale responses even after scope changes back. Confirmation binds one selected operation and is invalidated by input edits. Scope is cleared on route changes; filters include severity, code and correlation; no browser storage or download persistence added.

## Headless surface

External provider and Configuration confirmations are separate authoritative outcomes. With a journal, Logging records a minimised `logging.external.confirmation.observed` event and deduplication identity before returning confirmation; failed observation yields uncertainty, not rollback. The host must reconcile external outcomes by scoped request ID, enforce provider idempotency and publish independently authoritative commits from their own outboxes. There is no distributed atomicity; interruption between external confirmation and local observation remains possible. Provider operations require the observation port. Process-local policy changes have no durable observation.

With a durable journal, protected export requires the journal to commit a minimised `logging.export.released` event and unique scoped request identity before returning records. The event records authorisation to release a bounded snapshot, not receipt by the client, and excludes diagnostic contents. Repeated request identities reject; use a new independently approved request for another export. A journal without this optional port cannot release protected exports. Process-local exports remain non-durable.

Durable retention expiry emits `logging.expired` atomically with removal; the maintenance event has no fabricated operator or approval evidence. The store must implement this atomic guarantee. Process-local expiry is not a durable event. External authority commits are not claimed as local atomic commits.

Implemented: validated diagnostic admission, sanitised sink output, scoped reads, delivery projection, effective policy projection, scoped retained-record health, protected local export/restriction/erasure, host health and shutdown. These work without a GUI, Nuxt or external destinations. No mandatory database.

Approved but unavailable: provider-wide export, provider hold management, complete scoped loss/backlog health, committed lifecycle events for other operations, automatic replay/reconciliation of uncertain sink writes and manual retry. Do not call invented methods or substitute Logging receipts for Audit evidence. Missing provider contracts must be reconciled before claiming those integrations.

## You will hit this

Identifiers use ASCII alphanumeric first character, then up to 95 alphanumeric/underscore/dot/colon/hyphen characters. Syntax is not proof of safe provenance. UTC timestamps use YYYY-MM-DDTHH:mm:ss[.SSS]Z with 1–3 optional fractional digits. Only one submission in flight; concurrent calls receive unavailable. Policy sampling uses the tenant/source minute admission counter. Expiry is lazy on submit/read; history is not durable unless diagnosticStore is configured; provider retention is independent. Shutdown and local uncertainty do not prove write reversal. Never use this subset for regulated retention, evidence custody or legal hold fulfilment.

Compatibility: version 1 envelope and named receipt categories/codes are the current development contract; no released compatibility assurance. Breaking field, result or semantic changes require coordinated contract revision and affected consumer acceptance. Master 2ea7df6 passed Quality on its packaged PR head 1b70343. Current branch verification and independent consumer acceptance remain pending.
