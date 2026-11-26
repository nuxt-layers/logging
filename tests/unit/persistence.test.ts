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
  service.close()
 } finally {store.close();rmSync(dir,{recursive:true,force:true})}
})
