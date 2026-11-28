import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createLogging } from '../../src/index.ts'
const stamp='2026-11-26T00:00:00Z'
const event={tenantId:'a',subjectId:'operator',assurance:null,source:'worker'}
test('purpose retention narrows global limit and rejects unapproved purposes',async()=>{
 let now=stamp
 const options={resolveCaller:async(e:typeof event)=>e,resolveProducer:async(e:typeof event)=>e,decide:async()=>({allowed:true,reason:'test'}),now:()=>({instant:now,quality:'verified' as const}),localSink:{write:async()=> 'delivered' as const},rules:[{source:'worker',code:'short',purpose:'short',attributes:[]},{source:'worker',code:'long',purpose:'long',attributes:[]}],policyRevision:'v1',retentionMs:10000,purposeRetention:{short:1000}}
 assert.throws(()=>createLogging({...options,purposeRetention:{unknown:1000}}))
 assert.throws(()=>createLogging({...options,purposeRetention:{short:10001}}))
 const service=createLogging(options)
 for(const code of ['short','long'])assert.equal((await service.submit(event,{version:1,code,severity:'info',occurrenceTime:stamp})).status,'accepted')
 now='2026-11-26T00:00:02Z'
 const result=await service.readDiagnostics(event,{tenantId:'a',source:'worker',from:stamp,to:now,limit:10})
 assert.deepEqual(result.records.map(r=>r.code),['long'])
})
