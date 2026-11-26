export type Severity = 'debug' | 'info' | 'warn' | 'error' | 'fatal'
export type Caller = { tenantId: string; subjectId: string | null; assurance: unknown }
export type Producer = Caller & { source: string }
export type Diagnostic = { version: 1; code: string; severity: Severity; occurrenceTime: string; correlation?: string; attributes?: Record<string, number | boolean> }
export type RecordEntry = Readonly<{ id: string; tenantId: string; source: string; code: string; severity: Severity; occurrenceTime: string; receiptTime: string; timeQuality: 'verified' | 'degraded' | 'unknown'; correlation?: string; attributes: Readonly<Record<string, number | boolean>>; policyRevision: string }>
export type Delivery = 'delivered' | 'refused' | 'uncertain'
export type Receipt = { status: 'accepted' | 'rejected' | 'throttled' | 'unavailable' | 'uncertain'; code: string; id?: string }
export type Sink = { write(record: RecordEntry, signal: AbortSignal): Promise<Delivery> }
export type Rule = { source: string; code: string; attributes: readonly string[]; purpose: string }
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
}
