import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createLogging, type Options, type Diagnostic } from '../../src/index.ts'
const stamp='2026-11-26T00:00:00Z'
const event={tenantId:'a',subjectId:'service',assurance:null,source:'worker'}
const diagnostic: Diagnostic={version:1,code:'worker.done',severity:'info',occurrenceTime:stamp,attributes:{latency:3}}
function setup(overrides:Partial<Options<typeof event>>={}) {
 return createLogging({resolveCaller:async e=>e,resolveProducer:async e=>e,decide:async()=>({allowed:true,reason:'test'}),now:()=>({instant:stamp,quality:'unknown'}),localSink:{write:async()=> 'delivered'},rules:[{source:'worker',code:'worker.done',purpose:'diagnostics',attributes:['latency']}],policyRevision:'v1',retentionMs:1000,...overrides})
}
test('accepts and isolates local reads',async()=>{
 const service=setup()
 assert.equal((await service.submit(event,diagnostic)).status,'accepted')
 const q={tenantId:'a',source:'worker',from:stamp,to:stamp,limit:10}
 assert.equal((await service.readDiagnostics(event,q)).records.length,1)
 assert.equal((await service.readDiagnostics({...event,tenantId:'b'},q)).status,'rejected')
})
test('rejects unknown attributes before sink',async()=>{
 const service=setup({localSink:{write:async()=>{assert.fail('sink called')}}})
 assert.equal((await service.submit(event,{...diagnostic,attributes:{secret:1}})).status,'rejected')
})
test('fails closed on denied producer',async()=>{
 const service=setup({decide:async()=>({allowed:false,reason:'test'})})
 assert.equal((await service.submit(event,diagnostic)).status,'rejected')
})
test('capacity and rate are finite',async()=>{
 const service=setup({capacity:1})
 assert.equal((await service.submit(event,diagnostic)).status,'accepted')
 assert.equal((await service.submit(event,diagnostic)).status,'unavailable')
 const rated=setup({ratePerMinute:1})
 await rated.submit(event,diagnostic)
 assert.equal((await rated.submit(event,diagnostic)).status,'throttled')
})
test('sink refusal and uncertainty are distinct',async()=>{
 assert.equal((await setup({localSink:{write:async()=> 'refused'}}).submit(event,diagnostic)).status,'unavailable')
 const service=setup({localSink:{write:async()=> 'uncertain'}})
 assert.equal((await service.submit(event,diagnostic)).status,'uncertain')
 assert.equal((await service.submit(event,diagnostic)).status,'unavailable')
})
test('expiry removes history',async()=>{
 let now=stamp
 const service=setup({now:()=>({instant:now,quality:'unknown'})})
 await service.submit(event,diagnostic)
 now='2026-11-26T00:00:02Z'
 assert.equal((await service.readDiagnostics(event,{tenantId:'a',source:'worker',from:stamp,to:now,limit:10})).records.length,0)
})

test('strict envelope refuses unexpected fields',async()=>{
 const service=setup()
 assert.equal((await service.submit(event,{...diagnostic,raw:'never log'} as Diagnostic)).status,'rejected')
})
test('policy may narrow but never widen deployment rules',async()=>{
 const narrower=setup({resolvePolicy:async()=>({revision:'v2',rules:[{source:'worker',code:'worker.done',purpose:'diagnostics',attributes:[],minimumSeverity:'warn'}]})})
 assert.equal((await narrower.submit(event,diagnostic)).status,'rejected')
 const wider=setup({resolvePolicy:async()=>({revision:'v2',rules:[{source:'worker',code:'worker.done',purpose:'diagnostics',attributes:['extra']}]})})
 assert.equal((await wider.submit(event,diagnostic)).status,'unavailable')
})
test('external refusal retries; uncertainty quarantines; local intake stays accepted',async()=>{
 let calls=0
 const service=setup({rules:[{source:'worker',code:'worker.done',purpose:'diagnostics',attributes:['latency'],destinations:['remote']}],externalSinks:[{id:'remote',maxAttempts:3,sink:{write:async()=>{calls++;return calls===1?'refused':'uncertain'}}}]})
 assert.equal((await service.submit(event,diagnostic)).status,'accepted')
 assert.equal((await service.submit(event,diagnostic)).status,'accepted')
 assert.equal(calls,2)
 const q={tenantId:'a',source:'worker',from:stamp,to:stamp,limit:10}
 const result=await service.readDeliveryState(event,q)
 assert.equal(result.records[0]?.delivery[1]?.status,'uncertain')
 assert.equal(result.records[1]?.delivery[1]?.attempts,0)
 assert.equal((await service.readDeliveryState({...event,tenantId:'b'},q)).status,'rejected')
})
test('policy and delivery reads require distinct permissions',async()=>{
 const service=setup({decide:async(_c,action)=>({allowed:action==='logging.submit'||action==='logging.read',reason:'test'})})
 await service.submit(event,diagnostic)
 const q={tenantId:'a',source:'worker',from:stamp,to:stamp,limit:10}
 assert.equal((await service.readDiagnostics(event,q)).status,'completed')
 assert.equal((await service.readDeliveryState(event,q)).status,'rejected')
 assert.equal((await service.readEffectivePolicy(event,q)).status,'rejected')
})
test('query filters and shutdown',async()=>{
 const service=setup()
 const r=await service.submit(event,diagnostic)
 const q={tenantId:'a',source:'worker',from:stamp,to:stamp,limit:10}
 assert.equal((await service.readDiagnostics(event,{...q,id:r.id})).records.length,1)
 assert.equal((await service.readDiagnostics(event,{...q,severity:'fatal'})).records.length,0)
 assert.equal((await service.shutdown()).status,'completed')
 assert.equal((await service.submit(event,diagnostic)).status,'unavailable')
})
test('missing policy fails closed and invalid calendar dates reject',async()=>{
 assert.equal((await setup({resolvePolicy:async()=>null}).submit(event,diagnostic)).status,'unavailable')
 assert.equal((await setup().submit(event,{...diagnostic,occurrenceTime:'2026-02-30T00:00:00Z'})).status,'rejected')
})
