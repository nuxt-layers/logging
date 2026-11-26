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
