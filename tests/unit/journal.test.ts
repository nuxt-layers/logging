import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createSqliteLifecycleJournal } from '../../src/adapters/sqlite-lifecycle.mjs'
const command={action:'restrict' as const,target:{tenantId:'a',source:'worker',id:'record'},requestId:'req',subjectId:'operator',evidenceRef:'evidence',committedAt:'2026-11-26T00:00:00Z'}
test('journal persists state and event atomically across reopen and deduplicates',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'logging-'))
 const path=join(dir,'journal.sqlite')
 let journal=createSqliteLifecycleJournal(path)
 try {
  assert.equal(await journal.commit(command),'committed')
  const event=(await journal.pending(10))[0]!
  assert.equal(event.type,'logging.restricted')
  journal.close()
  journal=createSqliteLifecycleJournal(path)
  assert.equal(await journal.visibility(command.target),'restricted')
  assert.equal(await journal.visibility({...command.target,tenantId:'b'}),'visible')
  assert.equal(await journal.commit(command),'duplicate')
  assert.equal(await journal.commit({...command,action:'erase'}),'conflict')
  assert.equal((await journal.pending(10)).length,1)
  assert.equal((await journal.pending(10))[0]?.id,event.id)
  await journal.acknowledge(event.id)
  assert.equal((await journal.pending(10)).length,0)
  assert.equal(await journal.commit({...command,action:'erase',requestId:'req2'}),'committed')
  assert.equal(await journal.commit({...command,requestId:'req3'}),'conflict')
  assert.equal(await journal.visibility(command.target),'erased')
 } finally {journal.close();rmSync(dir,{recursive:true,force:true})}
})

test('maintenance removes only aged acknowledged events, retaining replay identities and tombstones',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'logging-maintenance-'))
 const journal=createSqliteLifecycleJournal(join(dir,'journal.sqlite'))
 try {
  assert.equal(await journal.commit(command),'committed')
  const first=(await journal.pending(10))[0]!
  await journal.acknowledge(first.id,'2026-11-26T00:00:00Z')
  assert.equal(await journal.commit({...command,action:'erase',requestId:'req2'}),'committed')
  const pending=(await journal.pending(10))[0]!
  const cutoff='2026-11-27T00:00:00Z'
  assert.deepEqual((await journal.maintenance!.acknowledgedBefore(cutoff,10)).map(e=>e.id),[first.id])
  assert.equal(await journal.maintenance!.removeAcknowledged(pending.id,cutoff),'absent')
  assert.equal(await journal.maintenance!.removeAcknowledged(first.id,'2026-11-25T00:00:00Z'),'absent')
  assert.equal(await journal.maintenance!.removeAcknowledged(first.id,cutoff),'removed')
  assert.equal(await journal.commit(command),'duplicate')
  assert.equal(await journal.visibility(command.target),'erased')
  assert.equal((await journal.pending(10))[0]?.id,pending.id)
 } finally {journal.close();rmSync(dir,{recursive:true,force:true})}
})

test('expiry commits its event once and survives reopening',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'logging-expiry-'))
 const path=join(dir,'journal.sqlite')
 let journal=createSqliteLifecycleJournal(path)
 const stamp=command.committedAt
 const record={id:'record',tenantId:'a',source:'worker',code:'worker.done',severity:'info' as const,occurrenceTime:stamp,receiptTime:stamp,timeQuality:'verified' as const,attributes:{},policyRevision:'v1',purpose:'diagnostics',category:'technical',classification:'technical' as const}
 try {
  assert.equal(await journal.save({record,delivery:[]},10),'stored')
  assert.equal(await journal.expire(command.target,'2026-11-25T00:00:00Z',stamp),'absent')
  assert.equal((await journal.pending(10)).length,0)
  assert.equal(await journal.expire(command.target,stamp,stamp),'removed')
  journal.close();journal=createSqliteLifecycleJournal(path)
  assert.equal((await journal.load('a','worker',10)).length,0)
  assert.equal(await journal.expire(command.target,stamp,stamp),'absent')
  const events=await journal.pending(10)
  assert.equal(events.length,1)
  assert.equal(events[0]?.type,'logging.expired')
  assert.equal(events[0]?.recordId,'record')
 } finally {journal.close();rmSync(dir,{recursive:true,force:true})}
})
