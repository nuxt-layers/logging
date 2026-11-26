import type { Options, RecordEntry, Delivery, Diagnostic, Receipt } from '../contracts/types.ts'
const token = (v: unknown): v is string => typeof v === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}$/.test(v)
const instant = (v: unknown): v is string => typeof v === 'string' && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,3})?Z$/.test(v) && Number.isFinite(Date.parse(v))
const levels = new Set(['debug','info','warn','error','fatal'])
const bounded = (v: number, max: number) => Number.isSafeInteger(v) && v > 0 && v <= max
export function createLogging<E>(options: Options<E>) {
  const capacity = options.capacity ?? 1000
  const rate = options.ratePerMinute ?? 120
  const timeout = options.timeoutMs ?? 1000
  const revision = options.policyRevision
  const retention = options.retentionMs
  if (!bounded(capacity, 10000) || !bounded(rate, 10000) || !bounded(timeout, 30000) || !bounded(retention, 86400000 * 30) || !token(revision)) throw new Error('LOG_INVALID_CONFIG')
  const rules = new Map<string, Set<string>>()
  for (const rule of options.rules) {
    if (!token(rule.source) || !token(rule.code) || !token(rule.purpose) || rule.attributes.length > 16 || rule.attributes.some(k => !token(k) || /password|secret|token|credential|session|payment|key|email|phone|address/i.test(k))) throw new Error('LOG_INVALID_RULE')
    const key = JSON.stringify([rule.source,rule.code])
    if (rules.has(key)) throw new Error('LOG_DUPLICATE_RULE')
    rules.set(key,new Set(rule.attributes))
  }
  const rows: RecordEntry[] = []
  const buckets = new Map<string,{ count: number; minute: number }>()
  let busy = false
  let closed = false
  let drops = 0
  let uncertain = 0
  let lastDelivery: Delivery | 'unknown' = 'unknown'
  let lastTime = -Infinity
  function time() {
    const t = options.now()
    if (!instant(t.instant) || !['verified','degraded','unknown'].includes(t.quality)) throw new Error('LOG_CLOCK')
    const ms = Date.parse(t.instant)
    if (ms < lastTime) throw new Error('LOG_CLOCK')
    lastTime = ms
    return { ...t, ms }
  }
  // Expiry affects the in-process read model only; sink retention is independently governed.
  function expire(ms: number) {
    for (let i = rows.length - 1; i >= 0; i--) {
      const row = rows[i]
      if (row && ms - Date.parse(row.receiptTime) >= retention) rows.splice(i,1)
    }
  }
  async function authorised(event: E, action: string, tenantId: string, source: string) {
    try {
      const caller = await options.resolveCaller(event)
      return !!caller && caller.tenantId === tenantId && token(source) && (await options.decide(caller,action,{tenantId,source})).allowed === true
    } catch { return false }
  }
  async function submit(event: E, input: Diagnostic): Promise<Receipt> {
    if (closed || busy) { drops++; return { status:'unavailable',code:'LOG_BUSY' } }
    busy = true
    try {
      const producer = await options.resolveProducer(event)
      if (!producer || !token(producer.tenantId) || !token(producer.source) || (await options.decide(producer,'logging.submit',{tenantId:producer.tenantId,source:producer.source})).allowed !== true) return {status:'rejected',code:'LOG_DENIED'}
      if (!input || typeof input !== 'object' || input.version !== 1 || !token(input.code) || !levels.has(input.severity) || !instant(input.occurrenceTime) || (input.correlation !== undefined && !token(input.correlation))) return {status:'rejected',code:'LOG_INVALID'}
      const allowed = rules.get(JSON.stringify([producer.source,input.code]))
      if (!allowed) return {status:'rejected',code:'LOG_RULE'}
      const attrs: Record<string,number|boolean> = Object.create(null)
      if (input.attributes !== undefined) {
        if (!input.attributes || typeof input.attributes !== 'object' || Array.isArray(input.attributes)) return {status:'rejected',code:'LOG_INVALID'}
        let count = 0
        for (const k in input.attributes) {
          if (!Object.hasOwn(input.attributes,k) || ++count > 16 || !allowed.has(k)) return {status:'rejected',code:'LOG_INVALID'}
          const v = input.attributes[k]
          if (typeof v !== 'boolean' && !(typeof v === 'number' && Number.isFinite(v) && Math.abs(v) <= 1e12)) return {status:'rejected',code:'LOG_INVALID'}
          attrs[k] = v
        }
      }
      const t = time()
      expire(t.ms)
      const minute = Math.floor(t.ms/60000)
      for (const [key,b] of buckets) if (b.minute !== minute) buckets.delete(key)
      const bucketKey = JSON.stringify([producer.tenantId,producer.source])
      const b = buckets.get(bucketKey) ?? {count:0,minute}
      if (b.count >= rate || (!buckets.has(bucketKey) && buckets.size >= capacity)) { drops++; return {status:'throttled',code:'LOG_RATE'} }
      b.count++; buckets.set(bucketKey,b)
      if (rows.length >= capacity) { drops++; return {status:'unavailable',code:'LOG_CAPACITY'} }
      const row: RecordEntry = Object.freeze({id:crypto.randomUUID(),tenantId:producer.tenantId,source:producer.source,code:input.code,severity:input.severity,occurrenceTime:input.occurrenceTime,receiptTime:t.instant,timeQuality:t.quality,correlation:input.correlation,attributes:Object.freeze(attrs),policyRevision:revision})
      const controller = new AbortController()
      let timer: ReturnType<typeof setTimeout> | undefined
      try {
        lastDelivery = await Promise.race([Promise.resolve().then(()=>options.localSink.write(row,controller.signal)),new Promise<Delivery>(resolve=>{timer=setTimeout(()=>{controller.abort();resolve('uncertain')},timeout)})])
      } catch { lastDelivery = 'uncertain' }
      finally { if(timer !== undefined) clearTimeout(timer) }
      if(lastDelivery === 'refused') { drops++; return {status:'unavailable',code:'LOG_SINK_REFUSED'} }
      if(lastDelivery !== 'delivered') { uncertain++; closed=true; return {status:'uncertain',code:'LOG_SINK_UNCERTAIN',id:row.id} }
      if (!closed) rows.push(row)
      return {status:'accepted',code:'LOG_ACCEPTED',id:row.id}
    } catch { return {status:'unavailable',code:'LOG_PROVIDER'} }
    finally { busy=false }
  }
  async function readDiagnostics(event: E, query: {tenantId:string;source:string;from:string;to:string;limit:number;offset?:number}) {
    if (!token(query.tenantId) || !token(query.source) || !instant(query.from) || !instant(query.to) || !bounded(query.limit,100) || !Number.isSafeInteger(query.offset ?? 0) || (query.offset ?? 0)<0 || Date.parse(query.to)<Date.parse(query.from) || Date.parse(query.to)-Date.parse(query.from)>86400000*30) return {status:'rejected' as const,records:[]}
    if (!await authorised(event,'logging.read',query.tenantId,query.source)) return {status:'rejected' as const,records:[]}
    try {
      expire(time().ms)
      const offset=query.offset??0
      const records=rows.filter(r=>r.tenantId===query.tenantId && r.source===query.source && Date.parse(r.receiptTime)>=Date.parse(query.from) && Date.parse(r.receiptTime)<=Date.parse(query.to)).slice(offset,offset+query.limit)
      return {status:'completed' as const,records,coverage:'process-local; incomplete; pagination may shift' as const}
    } catch { return {status:'unavailable' as const,records:[]} }
  }
  // Deployment-wide health is deliberately restricted to the composition root.
  return { submit,readDiagnostics,hostHealth:()=>({state:closed?'degraded':busy?'busy':'unknown',lastDelivery,drops,uncertain,buffered:rows.length}),close:()=>{closed=true;rows.length=0;buckets.clear()} }
}
