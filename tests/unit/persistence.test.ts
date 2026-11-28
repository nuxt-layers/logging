import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync,rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createSqliteLifecycleJournal } from '../../src/adapters/sqlite-lifecycle.mjs'
import { createLogging, type Options } from '../../src/index.ts'
const stamp='2026-11-26T00:00:00Z'
const event={tenantId:'a',subjectId:'operator',assurance:null,source:'worker'}
const diagnostic={version:1 as const,code:'worker.done',severity:'info' as const,occurrenceTime:stamp}
const q={tenantId:'a',source:'worker',from:stamp,to:stamp,limit:10}
test('persistent diagnostic recovery, restriction and hold-aware expiry',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'logging-store-'))
 const path=join(dir,'journal.sqlite')
 let store=createSqliteLifecycleJournal(path)
 let now=stamp
 let preservation:'clear'|'held'='held'
 let writes=0
 const options=():Options<typeof event>=>({resolveCaller:async e=>e,resolveProducer:async e=>e,decide:async()=>({allowed:true,reason:'test'}),now:()=>({instant:now,quality:'verified'}),localSink:{write:async()=>{writes++;return 'delivered'}},rules:[{source:'worker',code:'worker.done',purpose:'diagnostics',attributes:[]}],policyRevision:'v1',retentionMs:1000,diagnosticStore:store,retentionAuthority:{resolvePreservation:async()=>preservation},approveLifecycle:async()=>({status:'approved',evidenceRef:'evidence',preservation:'clear'})})
 try {
  let service=createLogging(options())
  const receipt=await service.submit(event,diagnostic)
  assert.equal(receipt.status,'accepted')
  service.close();store.close()
  store=createSqliteLifecycleJournal(path)
  service=createLogging(options())
  assert.equal((await service.readDiagnostics(event,q)).records.length,1)
  assert.equal(writes,1)
  assert.equal((await service.readDiagnostics({...event,tenantId:'b'},q)).status,'rejected')
  const target={tenantId:'a',source:'worker',id:receipt.id!}
  assert.equal((await service.restrictDiagnostic(event,target,'req')).status,'committed')
  assert.equal((await service.readDiagnostics(event,q)).records.length,0)
  now='2026-11-26T00:00:02Z'
  assert.equal((await service.maintainRetention()).held,1)
  preservation='clear'
  assert.equal((await service.maintainRetention()).removed,1)
  assert.equal(await store.visibility(target),'restricted')
  const events=await store.pending(10)
  assert.deepEqual(events.map(e=>e.type),['logging.restricted','logging.expired'])
  assert.equal(events[1]?.recordId,target.id)
  assert.equal(events[1]?.committedAt,now)
  assert.equal((await service.maintainRetention()).removed,0)
  assert.equal((await store.pending(10)).length,2)
  service.close()
 } finally {store.close();rmSync(dir,{recursive:true,force:true})}
})

test('manual delivery retry is authorised, bounded and cannot replay a completed request',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'logging-retry-'))
 const store=createSqliteLifecycleJournal(join(dir,'journal.sqlite'))
 let calls=0
 let allowed=true
 const service=createLogging<typeof event>({resolveCaller:async e=>e,resolveProducer:async e=>e,decide:async(_c,action)=>({allowed:action!=='logging.delivery.retry'||allowed,reason:'test'}),now:()=>({instant:stamp,quality:'verified'}),localSink:{write:async()=> 'delivered'},externalSinks:[{id:'remote',sink:{write:async()=>{calls++;return calls===1?'refused':'delivered'}}}],rules:[{source:'worker',code:'worker.done',purpose:'diagnostics',attributes:[],destinations:['remote']}],policyRevision:'v1',retentionMs:1000,diagnosticStore:store,approveLifecycle:async()=>({status:'approved',evidenceRef:'approval',preservation:'clear'})})
 try{
  const receipt=await service.submit(event,diagnostic)
  const request={target:{tenantId:'a',source:'worker',id:receipt.id!},destination:'remote',requestId:'retry-1',mode:'retry' as const}
  allowed=false
  assert.equal((await service.recoverDelivery(event,request)).status,'rejected')
  allowed=true
  assert.equal((await service.recoverDelivery({...event,tenantId:'b'},request)).status,'rejected')
  assert.equal((await service.recoverDelivery(event,request)).status,'delivered')
  assert.equal((await service.recoverDelivery(event,request)).status,'blocked')
  assert.equal(calls,2)
  assert.equal((await service.readDeliveryState(event,q)).records[0]?.delivery[1]?.attempts,2)
 }finally{service.close();store.close();rmSync(dir,{recursive:true,force:true})}
})
test('interrupted retry reservation survives reopen and requires reconciliation without writes',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'logging-reconcile-'))
 const path=join(dir,'journal.sqlite')
 let store=createSqliteLifecycleJournal(path)
 let writes=0
 const options=():Options<typeof event>=>({resolveCaller:async e=>e,resolveProducer:async e=>e,decide:async()=>({allowed:true,reason:'test'}),now:()=>({instant:stamp,quality:'verified'}),localSink:{write:async()=> 'delivered'},externalSinks:[{id:'remote',sink:{write:async()=>{writes++;return 'refused'}}}],rules:[{source:'worker',code:'worker.done',purpose:'diagnostics',attributes:[],destinations:['remote']}],policyRevision:'v1',retentionMs:1000,diagnosticStore:store,approveLifecycle:async()=>({status:'approved',evidenceRef:'approval',preservation:'clear'}),reconcileDelivery:async()=>({status:'delivered',evidenceRef:'provider-evidence'})})
 let service=createLogging(options())
 try{
  const receipt=await service.submit(event,diagnostic)
  const target={tenantId:'a',source:'worker',id:receipt.id!}
  const request={target,destination:'remote',requestId:'interrupted',mode:'retry' as const}
  const previous=(await store.load('a','worker',10))[0]!.delivery[1]!
  assert.equal(await store.reserveDeliveryRecovery!(request,previous,'operator','approval',stamp),'reserved')
  service.close();store.close()
  store=createSqliteLifecycleJournal(path);service=createLogging(options())
  assert.equal((await service.recoverDelivery(event,{...request,requestId:'unsafe-replay'})).status,'blocked')
  assert.equal((await service.recoverDelivery(event,{...request,requestId:'reconcile',mode:'reconcile'})).status,'delivered')
  assert.equal(writes,1)
  assert.equal((await service.readDeliveryState(event,q)).records[0]?.delivery[1]?.status,'delivered')
 }finally{service.close();store.close();rmSync(dir,{recursive:true,force:true})}
})
