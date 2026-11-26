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
  | { status:'completed-local'|'committed'; requestId:string; evidenceRef:string; providerState:'unknown' }
  | { status:'blocked'|'rejected'|'unavailable'|'uncertain' }
export type ExportResult =
  | { status:'completed-local'; requestId:string; evidenceRef:string; records:RecordEntry[]; coverage:string }
  | { status:'blocked'|'rejected'|'unavailable'|'uncertain'; records:never[] }
export type LifecycleEvent = Readonly<{
  version:1; id:string; type:'logging.restricted'|'logging.erased'; tenantId:string; source:string;
  recordId:string; requestId:string; subjectId:string; evidenceRef:string; committedAt:string
}>
export type LifecycleCommit = {
  action:'restrict'|'erase'; target:LifecycleTarget; requestId:string; subjectId:string;
  evidenceRef:string; committedAt:string
}
export type StoredDiagnostic = { record:RecordEntry; delivery:DeliveryState[] }
export type DiagnosticStore = LifecycleJournal & {
  save(entry:StoredDiagnostic,capacity:number):Promise<'stored'|'full'|'uncertain'>
  load(tenantId:string,source:string,limit:number):Promise<StoredDiagnostic[]>
  updateDelivery(target:LifecycleTarget,delivery:DeliveryState[]):Promise<void>
  expired(before:string,limit:number):Promise<LifecycleTarget[]>
  expire(target:LifecycleTarget,before:string):Promise<'removed'|'absent'>
}
export type PreservationDecision = 'clear'|'held'|'unknown'
export type OptionsRetention = {
  resolvePreservation(target:LifecycleTarget):Promise<PreservationDecision>
}
export type LifecycleJournal = {
  commit(command:LifecycleCommit):Promise<'committed'|'duplicate'|'conflict'|'uncertain'>
  visibility(target:LifecycleTarget):Promise<'visible'|'restricted'|'erased'>
  pending(limit:number):Promise<LifecycleEvent[]>
  acknowledge(eventId:string):Promise<void>
}
export type LifecyclePublisher = (event:LifecycleEvent,signal:AbortSignal)=>Promise<Delivery>
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
  lifecycleJournal?: LifecycleJournal
  diagnosticStore?: DiagnosticStore
  retentionAuthority?: OptionsRetention
  approveLifecycle?: (request:LifecycleApproval) => Promise<LifecycleDecision>
  resolvePolicy?: (scope:{tenantId:string;source:string}) => Promise<Policy | null>
}
