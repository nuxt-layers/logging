import { DatabaseSync } from 'node:sqlite'
import { randomUUID } from 'node:crypto'

// Host owns file path, filesystem permissions, backups, retention and lifecycle.
// One Logging-owned database per deployment, not a shared application table.
export function createSqliteLifecycleJournal(path) {
  if (typeof path !== 'string' || !path || path === ':memory:') throw new Error('LOG_DURABLE_PATH')
  const db = new DatabaseSync(path)
  try {
    db.exec(`
      PRAGMA journal_mode=WAL;
      PRAGMA synchronous=FULL;
      PRAGMA busy_timeout=1000;
      CREATE TABLE IF NOT EXISTS logging_lifecycle (
        tenant TEXT NOT NULL, source TEXT NOT NULL, record TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('restricted','erased')),
        PRIMARY KEY(tenant,source,record)
      );
      CREATE TABLE IF NOT EXISTS logging_requests (
        tenant TEXT NOT NULL, source TEXT NOT NULL, request TEXT NOT NULL,
        fingerprint TEXT NOT NULL, PRIMARY KEY(tenant,source,request)
      );
      CREATE TABLE IF NOT EXISTS logging_outbox (
        seq INTEGER PRIMARY KEY, id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
        acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0,1))
      );
    `)
  } catch (error) { db.close(); throw error }
  const lookup = db.prepare('SELECT fingerprint FROM logging_requests WHERE tenant=? AND source=? AND request=?')
  const insertRequest = db.prepare('INSERT INTO logging_requests VALUES(?,?,?,?)')
  const mutate = db.prepare(`INSERT INTO logging_lifecycle VALUES(?,?,?,?)
    ON CONFLICT(tenant,source,record) DO UPDATE SET state=CASE
    WHEN state='erased' THEN 'erased' ELSE excluded.state END`)
  const state = db.prepare('SELECT state FROM logging_lifecycle WHERE tenant=? AND source=? AND record=?')
  const insertEvent = db.prepare('INSERT INTO logging_outbox(id,payload) VALUES(?,?)')
  const pending = db.prepare('SELECT payload FROM logging_outbox WHERE acknowledged=0 ORDER BY seq LIMIT ?')
  const acknowledge = db.prepare('UPDATE logging_outbox SET acknowledged=1 WHERE id=?')
  return {
    async commit(command) {
      const {action,target,requestId,subjectId,evidenceRef,committedAt}=command
      const {tenantId,source,id}=target
      if(action!=='erase'&&action!=='restrict')return 'conflict'
      const fingerprint=JSON.stringify([action,id,subjectId,evidenceRef])
      try {
        db.exec('BEGIN IMMEDIATE')
        const previous=lookup.get(tenantId,source,requestId)
        if(previous) {
          db.exec('ROLLBACK')
          return previous.fingerprint===fingerprint?'duplicate':'conflict'
        }
        if(action==='restrict'&&state.get(tenantId,source,id)?.state==='erased') {
          db.exec('ROLLBACK')
          return 'conflict'
        }
        const event={version:1,id:randomUUID(),type:action==='erase'?'logging.erased':'logging.restricted',tenantId,source,recordId:id,requestId,subjectId,evidenceRef,committedAt}
        mutate.run(tenantId,source,id,action==='erase'?'erased':'restricted')
        insertRequest.run(tenantId,source,requestId,fingerprint)
        insertEvent.run(event.id,JSON.stringify(event))
        db.exec('COMMIT')
        return 'committed'
      } catch {
        try {db.exec('ROLLBACK')} catch {}
        return 'uncertain'
      }
    },
    async visibility({tenantId,source,id}) {
      return state.get(tenantId,source,id)?.state??'visible'
    },
    async pending(limit) {
      if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw new Error('LOG_INVALID_LIMIT')
      return pending.all(limit).map(row=>JSON.parse(row.payload))
    },
    async acknowledge(eventId) { acknowledge.run(eventId) },
    close() { db.close() }
  }
}
