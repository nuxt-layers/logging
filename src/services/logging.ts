import type { Options, RecordEntry, Delivery, Diagnostic, Receipt, DiagnosticQuery, DeliveryState, Policy } from '../contracts/types.ts'
import { token, instant, bounded, levels, compileRules, effectiveRules, validateDiagnostic, diagnosticCode, plain, fields } from '../policy/validation.ts'
import { deliver } from '../sinks/delivery.ts'

export function createLogging<E>(options: Options<E>) {
  const capacity = options.capacity ?? 1000
  const rate = options.ratePerMinute ?? 120
  const timeout = options.timeoutMs ?? 1000
  const retention = options.retentionMs
  const revision = options.policyRevision
  // Snapshot provider references; callers must not mutate a composed trust boundary.
  options = {...options}
  const localSink = options.localSink
  if (!bounded(capacity,10000) || !bounded(rate,10000) || !bounded(timeout,30000) || !bounded(retention,86400000*30) || !token(revision) || !localSink || typeof localSink.write !== 'function') throw new Error('LOG_INVALID_CONFIG')
  const external = new Map<string,{sink:typeof localSink;maxAttempts:number}>()
  for (const target of options.externalSinks ?? []) {
    if (!token(target.id) || target.id === 'local' || external.has(target.id) || external.size >= 8 || !bounded(target.maxAttempts ?? 1,3) || typeof target.sink?.write !== 'function') throw new Error('LOG_INVALID_SINK')
    external.set(target.id,{sink:target.sink,maxAttempts:target.maxAttempts ?? 1})
  }
  const floors = compileRules(options.rules,new Set(external.keys()))
  const rows: {record:RecordEntry;delivery:DeliveryState[]}[] = []
  const buckets = new Map<string,{count:number;minute:number}>()
  // Uncertain external sinks remain quarantined until recomposition, so ignored aborts cannot accumulate.
  const quarantined = new Set<string>()
  let busy=false, closed=false, closing=false, drops=0, uncertain=0
  let lastDelivery:Delivery|'unknown'='unknown'
  let lastTime=-Infinity
  let active:Promise<Receipt>|undefined
  let activeAbort:AbortController|undefined
  function time() {
    const t=options.now()
    if (!instant(t.instant) || !['verified','degraded','unknown'].includes(t.quality)) throw new Error('LOG_CLOCK')
    const ms=Date.parse(t.instant)
    if (ms<lastTime) throw new Error('LOG_CLOCK')
    lastTime=ms
    return {...t,ms}
  }
  function expire(ms:number) {
    for(let i=rows.length-1;i>=0;i--) {
      const row=rows[i]
      if(row && ms-Date.parse(row.record.receiptTime)>=retention) rows.splice(i,1)
    }
  }
  async function authorised(event:E,action:string,tenantId:string,source:string) {
    try {
      const caller=await options.resolveCaller(event)
      return !!caller && caller.tenantId===tenantId && token(tenantId) && token(source) && (await options.decide(caller,action,{tenantId,source})).allowed===true
    } catch { return false }
  }
  async function policy(tenantId:string,source:string):Promise<Policy|null> {
    if(!options.resolvePolicy) return {revision,rules:[...floors.values()].filter(r=>r.source===source)}
    const p=await options.resolvePolicy({tenantId,source})
    if(!p || !token(p.revision)) return null
    return {revision:p.revision,rules:effectiveRules(floors,p.rules,source,new Set(external.keys()))}
  }
  async function perform(event:E,input:Diagnostic):Promise<Receipt> {
    try {
      const producer=await options.resolveProducer(event)
      if(!producer || !token(producer.tenantId) || !token(producer.source) || (await options.decide(producer,'logging.submit',{tenantId:producer.tenantId,source:producer.source})).allowed!==true) return {status:'rejected',code:'LOG_DENIED'}
      const p=await policy(producer.tenantId,producer.source)
      if(!p) return {status:'unavailable',code:'LOG_POLICY_UNAVAILABLE'}
      const code=diagnosticCode(input)
      if(!code) return {status:'rejected',code:'LOG_INVALID'}
      const rule=p.rules.find(r=>r.source===producer.source && r.code===code)
      if(!rule) return {status:'rejected',code:'LOG_RULE'}
      const d=validateDiagnostic(input,rule)
      if(!d) return {status:'rejected',code:'LOG_INVALID'}
      const t=time()
      expire(t.ms)
      const minute=Math.floor(t.ms/60000)
      for(const [key,b] of buckets) if(b.minute!==minute) buckets.delete(key)
      const key=JSON.stringify([producer.tenantId,producer.source])
      const b=buckets.get(key)??{count:0,minute}
      if(b.count>=rate || (!buckets.has(key)&&buckets.size>=capacity)) {drops++;return {status:'throttled',code:'LOG_RATE'}}
      b.count++;buckets.set(key,b)
      if(levels.indexOf(d.severity)<levels.indexOf(rule.minimumSeverity??'debug') || (b.count-1)%(rule.sampleEvery??1)!==0) {drops++;return {status:'rejected',code:'LOG_FILTERED'}}
      if(rows.length>=capacity || closing || closed) {drops++;return {status:'unavailable',code:'LOG_CAPACITY'}}
      const row:RecordEntry=Object.freeze({id:crypto.randomUUID(),tenantId:producer.tenantId,source:producer.source,code:d.code,severity:d.severity,occurrenceTime:d.occurrenceTime,receiptTime:t.instant,timeQuality:t.quality,correlation:d.correlation,attributes:Object.freeze(d.attributes??{}),policyRevision:p.revision,purpose:rule.purpose,category:rule.category??'technical',classification:'technical'})
      const controller=new AbortController()
      activeAbort=controller
      const local=await deliver(localSink,row,timeout,1,controller.signal)
      lastDelivery=local.outcome
      if(local.outcome==='refused') {drops++;return {status:'unavailable',code:'LOG_SINK_REFUSED'}}
      if(local.outcome!=='delivered') {uncertain++;closed=true;return {status:'uncertain',code:'LOG_SINK_UNCERTAIN',id:row.id}}
      const item={record:row,delivery:[{target:'local',status:'delivered' as const,attempts:local.attempts,time:local.time}] as DeliveryState[]}
      if(!closing && !closed) rows.push(item)
      for(const target of rule.destinations??[]) {
        const ext=external.get(target)
        if(!ext || quarantined.has(target) || controller.signal.aborted) {
          item.delivery.push({target,status:'exhausted',attempts:0,time:new Date().toISOString()});drops++;continue
        }
        const result=await deliver(ext.sink,row,timeout,ext.maxAttempts,controller.signal)
        const status=result.outcome==='refused'?'exhausted':result.outcome
        item.delivery.push({target,status,attempts:result.attempts,time:result.time})
        if(status==='uncertain') {uncertain++;quarantined.add(target)}
        if(status==='exhausted') drops++
      }
      return {status:'accepted',code:'LOG_ACCEPTED',id:row.id}
    } catch {return {status:'unavailable',code:'LOG_PROVIDER'}}
    finally {activeAbort=undefined}
  }
  function submit(event:E,input:Diagnostic):Promise<Receipt> {
    if(closed||closing||busy) {drops++;return Promise.resolve({status:'unavailable',code:'LOG_BUSY'})}
    busy=true
    const task=perform(event,input).finally(()=>{busy=false;active=undefined})
    active=task
    return task
  }
  function snapshotQuery(q:DiagnosticQuery):DiagnosticQuery|null {
    if(!plain(q)||!fields(q,['tenantId','source','from','to','limit','offset','severity','code','correlation','id']))return null
    return {...q}
  }
  function validQuery(q:DiagnosticQuery) {
    return !!q && token(q.tenantId)&&token(q.source)&&instant(q.from)&&instant(q.to)&&bounded(q.limit,100)&&Number.isSafeInteger(q.offset??0)&&(q.offset??0)>=0&&Date.parse(q.to)>=Date.parse(q.from)&&Date.parse(q.to)-Date.parse(q.from)<=86400000*30&&(q.severity===undefined||levels.includes(q.severity))&&(q.code===undefined||token(q.code))&&(q.correlation===undefined||token(q.correlation))&&(q.id===undefined||token(q.id))
  }
  function matches(q:DiagnosticQuery,r:RecordEntry) {
    return r.tenantId===q.tenantId&&r.source===q.source&&Date.parse(r.receiptTime)>=Date.parse(q.from)&&Date.parse(r.receiptTime)<=Date.parse(q.to)&&(q.severity===undefined||r.severity===q.severity)&&(q.code===undefined||r.code===q.code)&&(q.correlation===undefined||r.correlation===q.correlation)&&(q.id===undefined||r.id===q.id)
  }
  async function readDiagnostics(event:E,query:DiagnosticQuery) {
    const snapshot=snapshotQuery(query)
    if(!snapshot||!validQuery(snapshot)||!await authorised(event,'logging.read',snapshot.tenantId,snapshot.source)) return {status:'rejected' as const,records:[]}
    try {
      expire(time().ms)
      const offset=snapshot.offset??0
      const records=rows.filter(x=>matches(snapshot,x.record)).slice(offset,offset+snapshot.limit).map(x=>x.record)
      return {status:'completed' as const,records,coverage:'process-local; incomplete; pagination may shift' as const}
    } catch {return {status:'unavailable' as const,records:[]}}
  }
  async function readDeliveryState(event:E,query:DiagnosticQuery) {
    const snapshot=snapshotQuery(query)
    if(!snapshot||!validQuery(snapshot)||!await authorised(event,'logging.delivery.read',snapshot.tenantId,snapshot.source)) return {status:'rejected' as const,records:[]}
    try {
      expire(time().ms)
      const offset=snapshot.offset??0
      return {status:'completed' as const,records:rows.filter(x=>matches(snapshot,x.record)).slice(offset,offset+snapshot.limit).map(x=>({id:x.record.id,delivery:x.delivery.map(d=>({...d}))})),coverage:'process-local; incomplete; in-flight destinations may be omitted' as const}
    } catch {return {status:'unavailable' as const,records:[]}}
  }
  async function readEffectivePolicy(event:E,scope:{tenantId:string;source:string}) {
    if(!plain(scope)||!fields(scope,['tenantId','source'])||!token(scope.tenantId)||!token(scope.source))return {status:'rejected' as const}
    const {tenantId,source}=scope
    if(!await authorised(event,'logging.policy.read',tenantId,source)) return {status:'rejected' as const}
    try {
      const p=await policy(tenantId,source)
      return p?{status:'completed' as const,policy:structuredClone(p)}:{status:'unavailable' as const}
    } catch {return {status:'unavailable' as const}}
  }
  function close() {
    closing=true;closed=true;activeAbort?.abort();rows.length=0;buckets.clear()
  }
  async function shutdown() {
    close()
    const pending=active
    if(!pending) return {status:'completed' as const}
    // The host must still bound its caller/policy provider latency.
    let timer:ReturnType<typeof setTimeout>|undefined
    try {
      return await Promise.race([pending.then(()=>({status:'completed' as const})),new Promise<{status:'uncertain'}>(resolve=>{timer=setTimeout(()=>resolve({status:'uncertain'}),timeout)})])
    } finally {if(timer!==undefined)clearTimeout(timer)}
  }
  return {submit,readDiagnostics,readDeliveryState,readEffectivePolicy,hostHealth:()=>({state:closed||quarantined.size?'degraded' as const:busy?'busy' as const:'unknown' as const,lastDelivery,drops,uncertain,buffered:rows.length,quarantined:[...quarantined]}),close,shutdown}
}
