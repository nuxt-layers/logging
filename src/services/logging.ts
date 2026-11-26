import type { Options, RecordEntry, Delivery, Diagnostic, Receipt, DiagnosticQuery, DeliveryState, Policy, Caller, LifecycleTarget, LifecycleAction, LifecycleResult, ExportResult, LifecycleDecision, LifecyclePublisher, StoredDiagnostic, DiagnosticStore } from '../contracts/types.ts'
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
  const store = options.diagnosticStore
  if(store && options.lifecycleJournal && store!==options.lifecycleJournal)throw new Error('LOG_INVALID_CONFIG')
  const journal = store ?? options.lifecycleJournal
  let relayBusy=false
  if (!bounded(capacity,10000) || !bounded(rate,10000) || !bounded(timeout,30000) || !bounded(retention,86400000*30) || !token(revision) || !localSink || typeof localSink.write !== 'function') throw new Error('LOG_INVALID_CONFIG')
  const external = new Map<string,{sink:typeof localSink;maxAttempts:number}>()
  for (const target of options.externalSinks ?? []) {
    if (!token(target.id) || target.id === 'local' || external.has(target.id) || external.size >= 8 || !bounded(target.maxAttempts ?? 1,3) || typeof target.sink?.write !== 'function') throw new Error('LOG_INVALID_SINK')
    external.set(target.id,{sink:target.sink,maxAttempts:target.maxAttempts ?? 1})
  }
  const floors = compileRules(options.rules,new Set(external.keys()))
  const rows: {record:RecordEntry;delivery:DeliveryState[];restricted:boolean}[] = []
  const buckets = new Map<string,{count:number;minute:number}>()
  // Uncertain external sinks remain quarantined until recomposition, so ignored aborts cannot accumulate.
  const quarantined = new Set<string>()
  let lifecycleBusy=false
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
      if((!store&&rows.length>=capacity) || closing || closed) {drops++;return {status:'unavailable',code:'LOG_CAPACITY'}}
      const row:RecordEntry=Object.freeze({id:crypto.randomUUID(),tenantId:producer.tenantId,source:producer.source,code:d.code,severity:d.severity,occurrenceTime:d.occurrenceTime,receiptTime:t.instant,timeQuality:t.quality,correlation:d.correlation,attributes:Object.freeze(d.attributes??{}),policyRevision:p.revision,purpose:rule.purpose,category:rule.category??'technical',classification:'technical'})
      // Persist the sanitised record before invoking any sink. Recovery never replays writes.
      if(store) {
        let saved
        try {saved=await store.save({record:row,delivery:[]},capacity)} catch {saved='uncertain'}
        if(saved==='full'){drops++;return {status:'unavailable',code:'LOG_CAPACITY'}}
        if(saved!=='stored'){close();return {status:'uncertain',code:'LOG_STORE_UNCERTAIN',id:row.id}}
        if(closing||closed)return {status:'uncertain',code:'LOG_STORE_UNCERTAIN',id:row.id}
      }
      const controller=new AbortController()
      activeAbort=controller
      const local=await deliver(localSink,row,timeout,1,controller.signal)
      lastDelivery=local.outcome
      const localState:DeliveryState={target:'local',status:local.outcome==='refused'?'exhausted':local.outcome,attempts:local.attempts,time:local.time}
      if(store) {
        try {await store.updateDelivery({tenantId:row.tenantId,source:row.source,id:row.id},[localState])}
        catch {close();return {status:'uncertain',code:'LOG_STORE_UNCERTAIN',id:row.id}}
      }
      if(local.outcome==='refused') {drops++;return {status:'unavailable',code:'LOG_SINK_REFUSED'}}
      if(local.outcome!=='delivered') {uncertain++;closed=true;return {status:'uncertain',code:'LOG_SINK_UNCERTAIN',id:row.id}}
      const item={record:row,restricted:false,delivery:[{target:'local',status:'delivered' as const,attempts:local.attempts,time:local.time}] as DeliveryState[]}
      if(!store && !closing && !closed) rows.push(item)
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
      if(store) {
        try {await store.updateDelivery({tenantId:row.tenantId,source:row.source,id:row.id},item.delivery)}
        catch {close();return {status:'uncertain',code:'LOG_STORE_UNCERTAIN',id:row.id}}
      }
      return {status:'accepted',code:'LOG_ACCEPTED',id:row.id}
    } catch {return {status:'unavailable',code:'LOG_PROVIDER'}}
    finally {activeAbort=undefined}
  }
  function submit(event:E,input:Diagnostic):Promise<Receipt> {
    if(closed||closing||busy||lifecycleBusy) {drops++;return Promise.resolve({status:'unavailable',code:'LOG_BUSY'})}
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
  async function visibleEntries(q:DiagnosticQuery) {
    const loaded=store?await store.load(q.tenantId,q.source,10000):rows
    if(store&&loaded.length>10000)throw new Error('LOG_JOURNAL')
    const candidates=loaded.filter(x=>!('restricted' in x && x.restricted)&&matches(q,x.record)&&(!store||Date.parse(x.record.receiptTime)>lastTime-retention))
    if(!journal)return candidates
    const result:typeof candidates=[]
    for(const x of candidates) {
      const {tenantId,source,id}=x.record
      const state=await journal.visibility({tenantId,source,id})
      if(state==='visible')result.push(x)
      else if(state!=='restricted'&&state!=='erased')throw new Error('LOG_JOURNAL')
    }
    if(closing||closed)throw new Error('LOG_JOURNAL')
    return store?result:result.filter(x=>rows.some(r=>r===x&&!r.restricted))
  }
  async function readDiagnostics(event:E,query:DiagnosticQuery) {
    const snapshot=snapshotQuery(query)
    if(!snapshot||!validQuery(snapshot)||!await authorised(event,'logging.read',snapshot.tenantId,snapshot.source)) return {status:'rejected' as const,records:[]}
    try {
      expire(time().ms)
      const offset=snapshot.offset??0
      const records=(await visibleEntries(snapshot)).slice(offset,offset+snapshot.limit).map(x=>x.record)
      return {status:'completed' as const,records,coverage:store?'durable diagnostic projection; pagination may shift':'process-local; incomplete; pagination may shift'}
    } catch {return {status:'unavailable' as const,records:[]}}
  }
  async function readDeliveryState(event:E,query:DiagnosticQuery) {
    const snapshot=snapshotQuery(query)
    if(!snapshot||!validQuery(snapshot)||!await authorised(event,'logging.delivery.read',snapshot.tenantId,snapshot.source)) return {status:'rejected' as const,records:[]}
    try {
      expire(time().ms)
      const offset=snapshot.offset??0
      return {status:'completed' as const,records:(await visibleEntries(snapshot)).slice(offset,offset+snapshot.limit).map(x=>({id:x.record.id,delivery:x.delivery.map(d=>({...d}))})),coverage:store?'durable diagnostic projection; unconfirmed delivery may be omitted':'process-local; incomplete; in-flight destinations may be omitted'}
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
  async function lifecycleCaller(event:E,action:LifecycleAction,tenantId:string,source:string):Promise<Caller|null> {
    try {
      const caller=await options.resolveCaller(event)
      if(!caller||caller.tenantId!==tenantId||!token(tenantId)||!token(source)||!token(caller.subjectId))return null
      const snapshot=Object.freeze({...caller})
      return (await options.decide(snapshot,'logging.'+action,{tenantId,source})).allowed===true?snapshot:null
    } catch {return null}
  }
  // The approval port is an independent, attributable precondition, not a Logging receipt.
  async function approve(action:LifecycleAction,caller:Caller,requestId:string,target:LifecycleTarget|DiagnosticQuery):Promise<LifecycleDecision> {
    if(!options.approveLifecycle)return {status:'unknown' as const}
    try {
      const result=await options.approveLifecycle({action,caller,requestId,target:Object.freeze({...target})})
      if(result?.status==='approved'&&token(result.evidenceRef)&&['clear','held','unknown'].includes(result.preservation))return result
      return {status:result?.status==='blocked'?'blocked' as const:'unknown' as const}
    } catch {return {status:'unknown' as const}}
  }
  async function exportDiagnostics(event:E,query:DiagnosticQuery,requestId:string):Promise<ExportResult> {
    const q=snapshotQuery(query)
    if(!q||!validQuery(q)||!token(requestId))return {status:'rejected',records:[]}
    const caller=await lifecycleCaller(event,'export',q.tenantId,q.source)
    if(!caller)return {status:'rejected',records:[]}
    if(busy||lifecycleBusy||closing||closed)return {status:'unavailable',records:[]}
    lifecycleBusy=true
    try {
      const approval=await approve('export',caller,requestId,q)
      if(approval.status!=='approved')return {status:approval.status==='blocked'?'blocked':'unavailable',records:[]}
      if(closed||closing)return {status:'unavailable',records:[]}
      // Recheck after the external approval; no data is disclosed on a revoked decision.
      const current=await lifecycleCaller(event,'export',q.tenantId,q.source)
      if(!current||current.subjectId!==caller.subjectId||closed||closing)return {status:'rejected',records:[]}
      expire(time().ms)
      const offset=q.offset??0
      const records=(await visibleEntries(q)).slice(offset,offset+q.limit).map(x=>x.record)
      return {status:'completed-local',requestId,evidenceRef:approval.evidenceRef,records,coverage:store?'durable diagnostic projection only; no provider export':'process-local only; incomplete; no provider export'}
    } catch {return {status:'unavailable',records:[]}}
    finally {lifecycleBusy=false}
  }
  async function changeLocalRecord(event:E,action:'erase'|'restrict',target:LifecycleTarget,requestId:string):Promise<LifecycleResult> {
    if(!plain(target)||!fields(target,['tenantId','source','id'])||!token(target.tenantId)||!token(target.source)||!token(target.id)||!token(requestId))return {status:'rejected'}
    const q={tenantId:target.tenantId,source:target.source,id:target.id}
    const caller=await lifecycleCaller(event,action,q.tenantId,q.source)
    if(!caller)return {status:'rejected'}
    if(busy||lifecycleBusy||closing||closed)return {status:'unavailable'}
    lifecycleBusy=true
    try {
      const approval=await approve(action,caller,requestId,q)
      if(approval.status!=='approved')return {status:approval.status==='blocked'?'blocked':'unavailable'}
      if(action==='erase'&&approval.preservation!=='clear')return {status:approval.preservation==='held'?'blocked':'unavailable'}
      if(closed||closing)return {status:'unavailable'}
      const current=await lifecycleCaller(event,action,q.tenantId,q.source)
      if(!current||current.subjectId!==caller.subjectId)return {status:'rejected'}
      if(closed||closing)return {status:'unavailable'}
      if(journal) {
        // State and event must commit atomically in the Logging-owned journal.
        let outcome
        try {
          outcome=await journal.commit({action,target:q,requestId,subjectId:caller.subjectId!,evidenceRef:approval.evidenceRef,committedAt:time().instant})
        } catch {outcome='uncertain'}
        if(outcome==='conflict')return {status:'rejected'}
        if(outcome!=='committed'&&outcome!=='duplicate') {
          close()
          return {status:'uncertain'}
        }
      }
      // A missing record and a previously removed record give the same local result.
      const index=rows.findIndex(x=>x.record.tenantId===q.tenantId&&x.record.source===q.source&&x.record.id===q.id)
      if(index>=0) {
        if(action==='erase')rows.splice(index,1)
        else {const row=rows[index];if(row)row.restricted=true}
      }
      return {status:journal?'committed':'completed-local',requestId,evidenceRef:approval.evidenceRef,providerState:'unknown'}
    } catch {return {status:'unavailable'}}
    finally {lifecycleBusy=false}
  }
  async function readPipelineHealth(event:E,query:DiagnosticQuery) {
    const q=snapshotQuery(query)
    if(!q||!validQuery(q)||!await authorised(event,'logging.health.read',q.tenantId,q.source))return {status:'rejected' as const}
    try {
      expire(time().ms)
      const entries=await visibleEntries(q)
      const delivery=entries.flatMap(x=>x.delivery)
      return {status:'completed' as const,records:entries.length,delivered:delivery.filter(x=>x.status==='delivered').length,exhausted:delivery.filter(x=>x.status==='exhausted').length,uncertain:delivery.filter(x=>x.status==='uncertain').length,coverage:'retained scoped records only; excludes intake losses, restricted and expired records; pagination ignored' as const}
    } catch {return {status:'unavailable' as const}}
  }
  // Composition-root-only relay. Acknowledgement is after publisher confirmation; duplicates are possible.
  async function relayLifecycle(publish:LifecyclePublisher,limit=20) {
    if(!journal||relayBusy||closing||closed||!bounded(limit,100))return {status:'unavailable' as const,published:0}
    relayBusy=true
    let published=0
    try {
      const events=await journal.pending(limit)
      if(!Array.isArray(events)||events.length>limit)return {status:'unavailable' as const,published}
      for(const event of events) {
        if(closing||closed)return {status:'unavailable' as const,published}
        const controller=new AbortController()
        let timer:ReturnType<typeof setTimeout>|undefined
        try {
          const result=await Promise.race([Promise.resolve().then(()=>publish(event,controller.signal)),new Promise<Delivery>(resolve=>{timer=setTimeout(()=>{controller.abort();resolve('uncertain')},timeout)})])
          if(result!=='delivered')return {status:result==='refused'?'unavailable' as const:'uncertain' as const,published}
          await journal.acknowledge(event.id)
          published++
        } catch {return {status:'uncertain' as const,published}}
        finally {if(timer!==undefined)clearTimeout(timer)}
      }
      return {status:'completed' as const,published}
    } catch {return {status:'unavailable' as const,published}}
    finally {relayBusy=false}
  }
  // Explicit maintenance, restricted to composition root; unknown preservation never deletes.
  async function maintainRetention(limit=20) {
    if(!store||!options.retentionAuthority||busy||lifecycleBusy||closing||closed||!bounded(limit,100))return {status:'unavailable' as const,removed:0,held:0}
    lifecycleBusy=true
    let removed=0,held=0
    try {
      const t=time()
      if(t.quality!=='verified')return {status:'unavailable' as const,removed,held}
      const before=new Date(t.ms-retention).toISOString()
      const targets=await store.expired(before,limit)
      if(targets.length>limit)return {status:'unavailable' as const,removed,held}
      for(const target of targets) {
        if(closing||closed)return {status:'unavailable' as const,removed,held}
        const decision=await options.retentionAuthority.resolvePreservation(target)
        if(decision!=='clear'){held++;continue}
        if(closing||closed)return {status:'unavailable' as const,removed,held}
        if(await store.expire(target,before)==='removed')removed++
      }
      return {status:'completed' as const,removed,held}
    } catch {return {status:'uncertain' as const,removed,held}}
    finally {lifecycleBusy=false}
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
  return {maintainRetention,relayLifecycle,submit,readDiagnostics,readDeliveryState,readEffectivePolicy,readPipelineHealth,exportDiagnostics,eraseDiagnostic:(event:E,target:LifecycleTarget,requestId:string)=>changeLocalRecord(event,'erase',target,requestId),restrictDiagnostic:(event:E,target:LifecycleTarget,requestId:string)=>changeLocalRecord(event,'restrict',target,requestId),hostHealth:()=>({state:closed||quarantined.size?'degraded' as const:(busy||lifecycleBusy)?'busy' as const:'unknown' as const,lastDelivery,drops,uncertain,buffered:rows.length,quarantined:[...quarantined]}),close,shutdown}
}
