import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createLogging, type Options } from '../../src/index.ts'
const stamp='2026-11-26T00:00:00Z'
const event={tenantId:'a',subjectId:'operator',assurance:null,source:'worker'}
const q={tenantId:'a',source:'worker',from:stamp,to:stamp,limit:10}
const diagnostic={version:1 as const,code:'worker.done',severity:'info' as const,occurrenceTime:stamp}
function setup(overrides:Partial<Options<typeof event>>={}) {
 return createLogging({resolveCaller:async e=>e,resolveProducer:async e=>e,decide:async()=>({allowed:true,reason:'test'}),now:()=>({instant:stamp,quality:'unknown'}),localSink:{write:async()=> 'delivered'},rules:[{source:'worker',code:'worker.done',purpose:'diagnostics',attributes:[]}],policyRevision:'v1',retentionMs:1000,...overrides})
}
const clear=async()=>({status:'approved' as const,evidenceRef:'evidence-1',preservation:'clear' as const})
test('export requires independent approval',async()=>{
 const service=setup()
 await service.submit(event,diagnostic)
 assert.equal((await service.exportDiagnostics(event,q,'req1')).status,'unavailable')
 const allowed=setup({approveLifecycle:clear})
 await allowed.submit(event,diagnostic)
 assert.equal((await allowed.exportDiagnostics(event,q,'req2')).records.length,1)
 assert.equal((await allowed.exportDiagnostics({...event,tenantId:'b'},q,'req3')).status,'rejected')
})
test('held and unknown preservation prevent local erase',async()=>{
 for(const preservation of ['held','unknown'] as const){
  const service=setup({approveLifecycle:async()=>({status:'approved',evidenceRef:'ref',preservation})})
  const receipt=await service.submit(event,diagnostic)
  const result=await service.eraseDiagnostic(event,{tenantId:'a',source:'worker',id:receipt.id!},'req')
  assert.equal(result.status,preservation==='held'?'blocked':'unavailable')
  assert.equal((await service.readDiagnostics(event,q)).records.length,1)
 }
})
test('restriction hides local reads and health; erasure never claims provider deletion',async()=>{
 const service=setup({approveLifecycle:clear})
 const receipt=await service.submit(event,diagnostic)
 const target={tenantId:'a',source:'worker',id:receipt.id!}
 assert.equal((await service.restrictDiagnostic(event,target,'r1')).status,'completed-local')
 assert.equal((await service.readDiagnostics(event,q)).records.length,0)
 assert.equal((await service.readDeliveryState(event,q)).records.length,0)
 assert.equal((await service.exportDiagnostics(event,q,'r2')).records.length,0)
 const health=await service.readPipelineHealth(event,q)
 assert.equal(health.status,'completed')
 if(health.status==='completed')assert.equal(health.records,0)
 const erased=await service.eraseDiagnostic(event,target,'r3')
 assert.equal(erased.status,'completed-local')
 if(erased.status==='completed-local')assert.equal(erased.providerState,'unknown')
})
test('permission revoked during approval blocks export',async()=>{
 let allowed=true
 const service=setup({decide:async()=>({allowed,reason:'test'}),approveLifecycle:async()=>{allowed=false;return clear()}})
 await service.submit(event,diagnostic)
 assert.equal((await service.exportDiagnostics(event,q,'r1')).status,'rejected')
})
test('scoped health requires separate permission and never exposes another tenant',async()=>{
 const service=setup()
 await service.submit({...event,tenantId:'b'},diagnostic)
 const result=await service.readPipelineHealth(event,q)
 assert.equal(result.status,'completed')
 if(result.status==='completed')assert.equal(result.records,0)
 assert.equal((await service.readPipelineHealth({...event,tenantId:'b'},q)).status,'rejected')
})

test('approval cannot be transferred to a changed caller',async()=>{
 let subject='operator'
 const service=setup({resolveCaller:async e=>({...e,subjectId:subject}),approveLifecycle:async()=>{subject='other';return clear()}})
 await service.submit(event,diagnostic)
 assert.equal((await service.exportDiagnostics(event,q,'req')).status,'rejected')
})

test('policy results disclose only contracted fields',async()=>{
 const service=setup({approveLifecycle:clear,resolvePolicy:async()=>({revision:'v1',rules:[]}),commitPolicy:async()=>({status:'committed',revision:'v2',evidenceRef:'ref',secret:'not public'})})
 assert.deepEqual(await service.changePolicy(event,{tenantId:'a',source:'worker',expectedRevision:'v1',requestId:'req',policy:{revision:'v2',rules:[]}}),{status:'committed',revision:'v2',evidenceRef:'ref'})
})
test('shutdown waits for in-flight policy mutation or reports uncertainty',async()=>{
 let release!:()=>void, entered!:()=>void
 const gate=new Promise<void>(r=>{release=r}), ready=new Promise<void>(r=>{entered=r})
 const service=setup({timeoutMs:20,approveLifecycle:clear,resolvePolicy:async()=>({revision:'v1',rules:[]}),commitPolicy:async()=>{entered();await gate;return {status:'committed',revision:'v2',evidenceRef:'ref'}}})
 const pending=service.changePolicy(event,{tenantId:'a',source:'worker',expectedRevision:'v1',requestId:'req',policy:{revision:'v2',rules:[]}})
 await ready
 assert.equal((await service.shutdown()).status,'uncertain')
 release()
 assert.equal((await pending).status,'committed')
 assert.equal((await service.shutdown()).status,'completed')
})

test('record and query permissions receive exact validated resource',async()=>{
 const service=setup({approveLifecycle:clear,decide:async(_c,action,resource)=>({allowed:action==='logging.submit'||resource.id==='allowed',reason:'test'})})
 assert.equal((await service.readDiagnostics(event,{...q,id:'denied'})).status,'rejected')
 assert.equal((await service.readDiagnostics(event,{...q,id:'allowed'})).status,'completed')
 assert.equal((await service.eraseDiagnostic(event,{tenantId:'a',source:'worker',id:'denied'},'req')).status,'rejected')
 assert.equal((await service.eraseDiagnostic(event,{tenantId:'a',source:'worker',id:'allowed'},'req')).status,'completed-local')
})
test('policy permission receives expected and proposed revision and request identity',async()=>{
 const service=setup({approveLifecycle:clear,resolvePolicy:async()=>({revision:'v1',rules:[]}),commitPolicy:async()=>({status:'committed',revision:'v2',evidenceRef:'ref'}),decide:async(_c,action,r)=>({allowed:action==='logging.policy.change'&&r.expectedRevision==='v1'&&r.revision==='v2'&&r.requestId==='req',reason:'test'})})
 assert.equal((await service.changePolicy(event,{tenantId:'a',source:'worker',expectedRevision:'v1',requestId:'denied',policy:{revision:'v2',rules:[]}})).status,'rejected')
 assert.equal((await service.changePolicy(event,{tenantId:'a',source:'worker',expectedRevision:'v1',requestId:'req',policy:{revision:'v2',rules:[]}})).status,'committed')
})

test('durable export fails closed unless release is committed',async()=>{
 for(const outcome of ['committed','conflict','uncertain'] as const){
  const service=setup({approveLifecycle:clear,lifecycleJournal:{commit:async()=> 'committed',visibility:async()=> 'visible',pending:async()=>[],acknowledge:async()=>{},recordExportRelease:async()=>outcome}})
  await service.submit(event,diagnostic)
  const result=await service.exportDiagnostics(event,q,'req')
  assert.equal(result.status,outcome==='committed'?'completed-local':outcome==='conflict'?'rejected':'uncertain')
  assert.equal(result.records.length,outcome==='committed'?1:0)
 }
})

test('durable policy confirmation requires a committed observation',async()=>{
 for(const outcome of ['committed','uncertain'] as const){
  const service=setup({approveLifecycle:clear,resolvePolicy:async()=>({revision:'v1',rules:[]}),commitPolicy:async()=>({status:'committed',revision:'v2',evidenceRef:'provider-ref'}),lifecycleJournal:{commit:async()=> 'committed',visibility:async()=> 'visible',pending:async()=>[],acknowledge:async()=>{},recordExternalObservation:async observation=>{assert.equal(observation.kind,'policy');assert.equal(observation.evidenceRef,'provider-ref');return outcome}}})
  assert.equal((await service.changePolicy(event,{tenantId:'a',source:'worker',expectedRevision:'v1',requestId:'req',policy:{revision:'v2',rules:[]}})).status,outcome==='committed'?'committed':'uncertain')
 }
})
