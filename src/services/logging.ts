import type { Options, RecordEntry, Delivery, Diagnostic, Receipt, DiagnosticQuery, DeliveryState, Policy, Caller, LifecycleTarget, LifecycleAction, LifecycleResult, ExportResult, LifecycleDecision, LifecyclePublisher, ProviderLifecycleRequest, ProviderLifecycleResult, PolicyChange, PolicyChangeResult, AccessResource } from '../contracts/types.ts'
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
  let journalCursor:string|undefined
  const journalRetention=options.journalRetention?{...options.journalRetention}:undefined
  if(journalRetention&&(!journal?.maintenance||!bounded(journalRetention.retentionMs,3650*86400000)||typeof journalRetention.resolvePreservation!=='function'))throw new Error('LOG_INVALID_CONFIG')
  if (!bounded(capacity,10000) || !bounded(rate,10000) || !bounded(timeout,30000) || !bounded(retention,86400000*30) || !token(revision) || !localSink || typeof localSink.write !== 'function') throw new Error('LOG_INVALID_CONFIG')
  const external = new Map<string,{sink:typeof localSink;maxAttempts:number}>()
  for (const target of options.externalSinks ?? []) {
    if (!token(target.id) || target.id === 'local' || external.has(target.id) || external.size >= 8 || !bounded(target.maxAttempts ?? 1,3) || typeof target.sink?.write !== 'function') throw new Error('LOG_INVALID_SINK')
    external.set(target.id,{sink:target.sink,maxAttempts:target.maxAttempts ?? 1})
  }
  const floors = compileRules(options.rules,new Set(external.keys()))
  const purposeRetention = new Map(Object.entries(options.purposeRetention??{}))
  for(const [purpose,ms] of purposeRetention)if(!token(purpose)||!bounded(ms,retention)||![...floors.values()].some(r=>r.purpose===purpose))throw new Error('LOG_INVALID_CONFIG')
  function retentionFor(record:RecordEntry) {
    return Math.min(retention,purposeRetention.get(record.purpose)??retention,record.retentionMs??retention)
  }
  let retentionCursor:LifecycleTarget|undefined
  const rows: {record:RecordEntry;delivery:DeliveryState[];restricted:boolean}[] = []
  const buckets = new Map<string,{count:number;minute:number}>()
  // Uncertain external sinks remain quarantined until recomposition, so ignored aborts cannot accumulate.
  const quarantined = new Set<string>()
  let lifecycleBusy=false
  let lifecycleAbort:AbortController|undefined
  let relayAbort:AbortController|undefined
  let lifecycleDone:Promise<void>|undefined, relayDone:Promise<void>|undefined
  let finishLifecycle:(()=>void)|undefined, finishRelay:(()=>void)|undefined
  function beginLifecycle(){ lifecycleBusy=true; lifecycleDone=new Promise(resolve=>{finishLifecycle=resolve}) }
  function endLifecycle(){ lifecycleBusy=false; finishLifecycle?.(); lifecycleDone=undefined; finishLifecycle=undefined; lifecycleAbort=undefined }
  function beginRelay(){ relayBusy=true; relayDone=new Promise(resolve=>{finishRelay=resolve}) }
  function endRelay(){ relayBusy=false; finishRelay?.(); relayDone=undefined; finishRelay=undefined; relayAbort=undefined }
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
      if(row && ms-Date.parse(row.record.receiptTime)>=retentionFor(row.record)) rows.splice(i,1)
    }
  }
  async function authorised(event:E,action:string,resource:AccessResource) {
    const {tenantId,source}=resource
    try {
      const caller=await options.resolveCaller(event)
      return !!caller && caller.tenantId===tenantId && token(tenantId) && token(source) && (await options.decide(caller,action,Object.freeze({...resource}))).allowed===true
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
      const row:RecordEntry=Object.freeze({id:crypto.randomUUID(),tenantId:producer.tenantId,source:producer.source,code:d.code,severity:d.severity,occurrenceTime:d.occurrenceTime,receiptTime:t.instant,timeQuality:t.quality,correlation:d.correlation,attributes:Object.freeze(d.attributes??{}),policyRevision:p.revision,purpose:rule.purpose,category:rule.category??'technical',classification:'technical',retentionMs:purposeRetention.get(rule.purpose)??retention})
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
    const candidates=loaded.filter(x=>!('restricted' in x && x.restricted)&&matches(q,x.record)&&(!store||Date.parse(x.record.receiptTime)>lastTime-retentionFor(x.record)))
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
    if(!snapshot||!validQuery(snapshot)||!await authorised(event,'logging.read',snapshot)) return {status:'rejected' as const,records:[]}
    try {
      expire(time().ms)
      const offset=snapshot.offset??0
      const records=(await visibleEntries(snapshot)).slice(offset,offset+snapshot.limit).map(x=>x.record)
      return {status:'completed' as const,records,coverage:store?'durable diagnostic projection; pagination may shift':'process-local; incomplete; pagination may shift'}
    } catch {return {status:'unavailable' as const,records:[]}}
  }
  async function readDeliveryState(event:E,query:DiagnosticQuery) {
    const snapshot=snapshotQuery(query)
    if(!snapshot||!validQuery(snapshot)||!await authorised(event,'logging.delivery.read',snapshot)) return {status:'rejected' as const,records:[]}
    try {
      expire(time().ms)
      const offset=snapshot.offset??0
      return {status:'completed' as const,records:(await visibleEntries(snapshot)).slice(offset,offset+snapshot.limit).map(x=>({id:x.record.id,delivery:x.delivery.map(d=>({...d}))})),coverage:store?'durable diagnostic projection; unconfirmed delivery may be omitted':'process-local; incomplete; in-flight destinations may be omitted'}
    } catch {return {status:'unavailable' as const,records:[]}}
  }
  async function readEffectivePolicy(event:E,scope:{tenantId:string;source:string}) {
    if(!plain(scope)||!fields(scope,['tenantId','source'])||!token(scope.tenantId)||!token(scope.source))return {status:'rejected' as const}
    const {tenantId,source}=scope
    if(!await authorised(event,'logging.policy.read',{tenantId,source})) return {status:'rejected' as const}
    try {
      const p=await policy(tenantId,source)
      return p?{status:'completed' as const,policy:structuredClone(p)}:{status:'unavailable' as const}
    } catch {return {status:'unavailable' as const}}
  }
  async function lifecycleCaller(event:E,action:LifecycleAction,resource:AccessResource):Promise<Caller|null> {
    const {tenantId,source}=resource
    try {
      const caller=await options.resolveCaller(event)
      if(!caller||caller.tenantId!==tenantId||!token(tenantId)||!token(source)||!token(caller.subjectId))return null
      const snapshot=Object.freeze({...caller})
      return (await options.decide(snapshot,'logging.'+action,Object.freeze({...resource}))).allowed===true?snapshot:null
    } catch {return null}
  }
  // The approval port is an independent, attributable precondition, not a Logging receipt.
  async function approve(action:LifecycleAction,caller:Caller,requestId:string,target:LifecycleTarget|DiagnosticQuery|PolicyChange,destination?:string):Promise<LifecycleDecision> {
    if(!options.approveLifecycle)return {status:'unknown' as const}
    try {
      const result=await options.approveLifecycle({action,caller,requestId,destination,target:Object.freeze(structuredClone(target))})
      if(result?.status==='approved'&&token(result.evidenceRef)&&['clear','held','unknown'].includes(result.preservation))return {status:'approved',evidenceRef:result.evidenceRef,preservation:result.preservation}
      return {status:result?.status==='blocked'?'blocked' as const:'unknown' as const}
    } catch {return {status:'unknown' as const}}
  }
  async function exportDiagnostics(event:E,query:DiagnosticQuery,requestId:string):Promise<ExportResult> {
    const q=snapshotQuery(query)
    if(!q||!validQuery(q)||!token(requestId))return {status:'rejected',records:[]}
    const caller=await lifecycleCaller(event,'export',{...q,requestId})
    if(!caller)return {status:'rejected',records:[]}
    if(busy||lifecycleBusy||closing||closed)return {status:'unavailable',records:[]}
    beginLifecycle()
    try {
      const approval=await approve('export',caller,requestId,q)
      if(approval.status!=='approved')return {status:approval.status==='blocked'?'blocked':'unavailable',records:[]}
      if(closed||closing)return {status:'unavailable',records:[]}
      // Recheck after the external approval; no data is disclosed on a revoked decision.
      const current=await lifecycleCaller(event,'export',{...q,requestId})
      if(!current||current.subjectId!==caller.subjectId||closed||closing)return {status:'rejected',records:[]}
      expire(time().ms)
      const offset=q.offset??0
      const records=(await visibleEntries(q)).slice(offset,offset+q.limit).map(x=>x.record)
      return {status:'completed-local',requestId,evidenceRef:approval.evidenceRef,records,coverage:store?'durable diagnostic projection only; no provider export':'process-local only; incomplete; no provider export'}
    } catch {return {status:'unavailable',records:[]}}
    finally {endLifecycle()}
  }
  async function changeLocalRecord(event:E,action:'erase'|'restrict',target:LifecycleTarget,requestId:string):Promise<LifecycleResult> {
    if(!plain(target)||!fields(target,['tenantId','source','id'])||!token(target.tenantId)||!token(target.source)||!token(target.id)||!token(requestId))return {status:'rejected'}
    const q={tenantId:target.tenantId,source:target.source,id:target.id}
    const caller=await lifecycleCaller(event,action,{...q,requestId})
    if(!caller)return {status:'rejected'}
    if(busy||lifecycleBusy||closing||closed)return {status:'unavailable'}
    beginLifecycle()
    try {
      const approval=await approve(action,caller,requestId,q)
      if(approval.status!=='approved')return {status:approval.status==='blocked'?'blocked':'unavailable'}
      if(action==='erase'&&approval.preservation!=='clear')return {status:approval.preservation==='held'?'blocked':'unavailable'}
      if(closed||closing)return {status:'unavailable'}
      const current=await lifecycleCaller(event,action,{...q,requestId})
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
    finally {endLifecycle()}
  }
  async function runProviderLifecycle(event:E,request:Pick<ProviderLifecycleRequest,'action'|'target'|'destination'|'requestId'>):Promise<ProviderLifecycleResult> {
    if(!plain(request)||!fields(request,['action','target','destination','requestId']))return {status:'rejected'}
    const {action,destination,requestId,target}=request
    if(!['erase','restrict','recover'].includes(action)||!token(destination)||!token(requestId)||!plain(target)||!fields(target,['tenantId','source','id'])||!token(target.tenantId)||!token(target.source)||!token(target.id))return {status:'rejected'}
    const q={tenantId:target.tenantId,source:target.source,id:target.id}
    if(!options.providerLifecycle||busy||lifecycleBusy||closing||closed)return {status:'unavailable'}
    beginLifecycle()
    try {
      const caller=await lifecycleCaller(event,action,{...q,requestId,destination})
      if(!caller||(await options.decide(caller,'logging.provider.'+action,Object.freeze({...q,requestId,destination}))).allowed!==true)return {status:'rejected'}
      if(destination!=='local'&&!external.has(destination))return {status:'rejected'}
      const approval=await approve(action,caller,requestId,q,destination)
      if(approval.status!=='approved')return {status:approval.status==='blocked'?'blocked':'unavailable'}
      if((action==='erase'||action==='recover')&&approval.preservation!=='clear')return {status:approval.preservation==='held'?'blocked':'unavailable'}
      const current=await lifecycleCaller(event,action,{...q,requestId,destination})
      if(!current||current.subjectId!==caller.subjectId||(await options.decide(current,'logging.provider.'+action,Object.freeze({...q,requestId,destination}))).allowed!==true)return {status:'rejected'}
      if(closed||closing)return {status:'unavailable'}
      if(!journal)return {status:'unavailable'}
      if(action==='recover'&&await journal.visibility(q)!=='visible')return {status:'blocked'}
      if(closed||closing)return {status:'unavailable'}
      const controller=new AbortController()
      lifecycleAbort=controller
      let timer:ReturnType<typeof setTimeout>|undefined
      try {
        const result=await Promise.race([options.providerLifecycle({action,target:q,destination,requestId,subjectId:caller.subjectId!,evidenceRef:approval.evidenceRef},controller.signal),new Promise<ProviderLifecycleResult>(resolve=>{timer=setTimeout(()=>{controller.abort();resolve({status:'uncertain'})},timeout)})])
        if(result?.status==='confirmed'&&token(result.evidenceRef))return {status:'confirmed',evidenceRef:result.evidenceRef}
        if(result&&(result.status==='blocked'||result.status==='rejected'||result.status==='unavailable'||result.status==='uncertain'))return {status:result.status}
        return {status:'uncertain'}
      } catch {return {status:'uncertain'}}
      finally {if(timer!==undefined)clearTimeout(timer)}
    } catch {return {status:'unavailable'}}
    finally {endLifecycle()}
  }
  async function changePolicy(event:E,input:PolicyChange):Promise<PolicyChangeResult> {
    if(!plain(input)||!fields(input,['tenantId','source','expectedRevision','policy','requestId'])||!token(input.tenantId)||!token(input.source)||!token(input.expectedRevision)||!token(input.requestId)||!plain(input.policy)||!fields(input.policy,['revision','rules'])||!token(input.policy.revision)||input.policy.revision===input.expectedRevision)return {status:'rejected'}
    if(!options.commitPolicy||!options.resolvePolicy||busy||lifecycleBusy||closing||closed)return {status:'unavailable'}
    const {tenantId,source,expectedRevision,requestId}=input
    let change:PolicyChange
    try {change={tenantId,source,expectedRevision,requestId,policy:{revision:input.policy.revision,rules:effectiveRules(floors,input.policy.rules,source,new Set(external.keys()))}}}catch{return {status:'rejected'}}
    beginLifecycle()
    try {
      const caller=await lifecycleCaller(event,'policy.change',{tenantId,source,requestId,expectedRevision,revision:change.policy.revision})
      if(!caller)return {status:'rejected'}
      const approval=await approve('policy.change',caller,requestId,change)
      if(approval.status!=='approved')return {status:approval.status==='blocked'?'blocked':'unavailable'}
      const current=await lifecycleCaller(event,'policy.change',{tenantId,source,requestId,expectedRevision,revision:change.policy.revision})
      if(!current||current.subjectId!==caller.subjectId)return {status:'rejected'}
      if((await policy(tenantId,source))?.revision!==expectedRevision)return {status:'conflict'}
      if(closed||closing)return {status:'unavailable'}
      try {
        const result=await options.commitPolicy(change,current,approval.evidenceRef)
        if(result?.status==='committed'&&result.revision===change.policy.revision&&token(result.evidenceRef))return {status:'committed',revision:result.revision,evidenceRef:result.evidenceRef}
        if(result&&(result.status==='rejected'||result.status==='blocked'||result.status==='conflict'||result.status==='unavailable'||result.status==='uncertain'))return {status:result.status}
        return {status:'uncertain'}
      }catch{return {status:'uncertain'}}
    }catch{return {status:'unavailable'}}
    finally {endLifecycle()}
  }
  async function readPipelineHealth(event:E,query:DiagnosticQuery) {
    const q=snapshotQuery(query)
    if(!q||!validQuery(q)||!await authorised(event,'logging.health.read',q))return {status:'rejected' as const}
    try {
      expire(time().ms)
      const entries=await visibleEntries(q)
      const delivery=entries.flatMap(x=>x.delivery)
      return {status:'completed' as const,records:entries.length,delivered:delivery.filter(x=>x.status==='delivered').length,exhausted:delivery.filter(x=>x.status==='exhausted').length,uncertain:delivery.filter(x=>x.status==='uncertain').length,coverage:'retained scoped records only; excludes intake losses, restricted and expired records; pagination ignored' as const}
    } catch {return {status:'unavailable' as const}}
  }
  // Composition-root-only relay. Acknowledgement is after publisher confirmation; duplicates are possible.
  async function relayLifecycle(publish:LifecyclePublisher,limit=20) {
    if(!journal||relayBusy||lifecycleBusy||closing||closed||!bounded(limit,100))return {status:'unavailable' as const,published:0}
    beginRelay()
    let published=0
    try {
      const events=await journal.pending(limit)
      if(!Array.isArray(events)||events.length>limit)return {status:'unavailable' as const,published}
      for(const event of events) {
        if(closing||closed)return {status:'unavailable' as const,published}
        const controller=new AbortController()
        relayAbort=controller
        let timer:ReturnType<typeof setTimeout>|undefined
        try {
          const result=await Promise.race([Promise.resolve().then(()=>publish(event,controller.signal)),new Promise<Delivery>(resolve=>{timer=setTimeout(()=>{controller.abort();resolve('uncertain')},timeout)})])
          if(result!=='delivered')return {status:result==='refused'?'unavailable' as const:'uncertain' as const,published}
          const stamp=time()
          await journal.acknowledge(event.id,stamp.quality==='verified'?stamp.instant:undefined)
          published++
        } catch {return {status:'uncertain' as const,published}}
        finally {if(timer!==undefined)clearTimeout(timer)}
      }
      return {status:'completed' as const,published}
    } catch {return {status:'unavailable' as const,published}}
    finally {endRelay()}
  }
  // Explicit maintenance, restricted to composition root; unknown preservation never deletes.
  async function maintainRetention(limit=20) {
    if(!store||!options.retentionAuthority||busy||lifecycleBusy||closing||closed||!bounded(limit,100))return {status:'unavailable' as const,removed:0,held:0}
    beginLifecycle()
    let removed=0,held=0
    try {
      const t=time()
      if(t.quality!=='verified')return {status:'unavailable' as const,removed,held}
      const before=t.instant
      const targets=await store.expired(before,limit,retentionCursor)
      if(targets.length===0){retentionCursor=undefined;return {status:'completed' as const,removed,held}}
      if(targets.length>limit)return {status:'unavailable' as const,removed,held}
      for(const target of targets) {
        if(closing||closed)return {status:'unavailable' as const,removed,held}
        retentionCursor=target
        const record=await store.retentionRecord(target)
        if(!record||record.tenantId!==target.tenantId||record.source!==target.source||record.id!==target.id)continue
        const cutoff=new Date(t.ms-retentionFor(record)).toISOString()
        if(Date.parse(record.receiptTime)>Date.parse(cutoff))continue
        const decision=await options.retentionAuthority.resolvePreservation(target)
        if(decision!=='clear'){held++;continue}
        if(closing||closed)return {status:'unavailable' as const,removed,held}
        if(await store.expire(target,cutoff,t.instant)==='removed')removed++
      }
      if(targets.length<limit)retentionCursor=undefined
      return {status:'completed' as const,removed,held}
    } catch {return {status:'uncertain' as const,removed,held}}
    finally {endLifecycle()}
  }
  // Only acknowledged event payloads are eligible; request identities and tombstones are never pruned here.
  async function maintainJournal(limit=20) {
    const maintenance=journal?.maintenance
    if(!maintenance||!journalRetention||relayBusy||busy||lifecycleBusy||closing||closed||!bounded(limit,100))return {status:'unavailable' as const,removed:0,held:0}
    beginLifecycle()
    let removed=0,held=0
    try {
      const t=time()
      if(t.quality!=='verified')return {status:'unavailable' as const,removed,held}
      const before=new Date(t.ms-journalRetention.retentionMs).toISOString()
      const events=await maintenance.acknowledgedBefore(before,limit,journalCursor)
      if(!Array.isArray(events)||events.length>limit)return {status:'unavailable' as const,removed,held}
      for(const event of events) {
        if(closing||closed)return {status:'unavailable' as const,removed,held}
        if(!token(event.id)||!instant(event.committedAt))return {status:'unavailable' as const,removed,held}
        journalCursor=event.id
        if(await journalRetention.resolvePreservation(event)!=='clear'){held++;continue}
        if(closing||closed)return {status:'unavailable' as const,removed,held}
        if(await maintenance.removeAcknowledged(event.id,before)==='removed')removed++
      }
      if(events.length<limit)journalCursor=undefined
      return {status:'completed' as const,removed,held}
    } catch {return {status:'uncertain' as const,removed,held}}
    finally {endLifecycle()}
  }
  function close() {
    closing=true;closed=true;activeAbort?.abort();lifecycleAbort?.abort();relayAbort?.abort();rows.length=0;buckets.clear()
  }
  async function shutdown() {
    close()
    const pending=[active,lifecycleDone,relayDone].filter(p=>p!==undefined)
    if(!pending.length) return {status:'completed' as const}
    // The host must still bound its caller/policy provider latency.
    let timer:ReturnType<typeof setTimeout>|undefined
    try {
      return await Promise.race([Promise.allSettled(pending).then(()=>({status:'completed' as const})),new Promise<{status:'uncertain'}>(resolve=>{timer=setTimeout(()=>resolve({status:'uncertain'}),timeout)})])
    } finally {if(timer!==undefined)clearTimeout(timer)}
  }
  return {runProviderLifecycle,changePolicy,maintainJournal,maintainRetention,relayLifecycle,submit,readDiagnostics,readDeliveryState,readEffectivePolicy,readPipelineHealth,exportDiagnostics,eraseDiagnostic:(event:E,target:LifecycleTarget,requestId:string)=>changeLocalRecord(event,'erase',target,requestId),restrictDiagnostic:(event:E,target:LifecycleTarget,requestId:string)=>changeLocalRecord(event,'restrict',target,requestId),hostHealth:()=>({state:closed||quarantined.size?'degraded' as const:(busy||lifecycleBusy)?'busy' as const:'unknown' as const,lastDelivery,drops,uncertain,buffered:rows.length,quarantined:[...quarantined]}),close,shutdown}
}
