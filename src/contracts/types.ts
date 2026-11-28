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
export type LifecycleEvent = Readonly<SinkObservation & {version:1;id:string;type:'logging.sink.degraded'|'logging.sink.recovered'}> | Readonly<ExternalObservation & {version:1;id:string;type:'logging.external.confirmation.observed'}> | Readonly<{
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
export type SinkObservation = {tenantId:string;source:string;destination:string;state:'ready'|'degraded';committedAt:string;timeQuality:RecordEntry['timeQuality']}
export type RetirementCandidate = {kind:'request'|'marker';tenantId:string;source:string;id:string;version:string}
export type RetirementAuthority = {authorise(candidate:RetirementCandidate):Promise<'clear'|'held'|'unknown'>}
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
  observeSink?(observation:SinkObservation):Promise<'committed'|'unchanged'|'full'|'uncertain'>
  retirementCandidates?(limit:number,after?:RetirementCandidate):Promise<RetirementCandidate[]>
  retire?(candidate:RetirementCandidate):Promise<'removed'|'absent'|'blocked'>
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
  retirementAuthority?: RetirementAuthority
  providerLifecycle?: (request:ProviderLifecycleRequest,signal:AbortSignal)=>Promise<ProviderLifecycleResult>
  commitPolicy?: (change:PolicyChange,caller:Caller,approvalEvidenceRef:string)=>Promise<PolicyChangeResult>
  approveLifecycle?: (request:LifecycleApproval) => Promise<LifecycleDecision>
  resolvePolicy?: (scope:{tenantId:string;source:string}) => Promise<Policy | null>
}
