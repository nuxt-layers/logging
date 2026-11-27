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
      CREATE TABLE IF NOT EXISTS logging_diagnostics (
        tenant TEXT NOT NULL, source TEXT NOT NULL, record TEXT NOT NULL,
        receipt INTEGER NOT NULL, payload TEXT NOT NULL, delivery TEXT NOT NULL,
        PRIMARY KEY(tenant,source,record)
      );
      CREATE INDEX IF NOT EXISTS logging_diagnostics_expiry ON logging_diagnostics(receipt);
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
    async save(entry,capacity) {
      if(!Number.isSafeInteger(capacity)||capacity<1||capacity>10000)return 'uncertain'
      const r=entry.record
      try {
        db.exec('BEGIN IMMEDIATE')
        if(db.prepare('SELECT COUNT(*) AS n FROM logging_diagnostics').get().n>=capacity) {
          db.exec('ROLLBACK');return 'full'
        }
        if(state.get(r.tenantId,r.source,r.id)){db.exec('ROLLBACK');return 'uncertain'}
        db.prepare('INSERT INTO logging_diagnostics VALUES(?,?,?,?,?,?)').run(r.tenantId,r.source,r.id,Date.parse(r.receiptTime),JSON.stringify(r),JSON.stringify(entry.delivery))
        db.exec('COMMIT');return 'stored'
      } catch {try{db.exec('ROLLBACK')}catch{}return 'uncertain'}
    },
    async load(tenantId,source,limit) {
      if(!Number.isSafeInteger(limit)||limit<1||limit>10000)throw new Error('LOG_INVALID_LIMIT')
      return db.prepare(`SELECT d.payload,d.delivery FROM logging_diagnostics d
        LEFT JOIN logging_lifecycle l ON d.tenant=l.tenant AND d.source=l.source AND d.record=l.record
        WHERE d.tenant=? AND d.source=? AND l.state IS NULL ORDER BY d.receipt,d.record LIMIT ?`).all(tenantId,source,limit).map(row=>({record:JSON.parse(row.payload),delivery:JSON.parse(row.delivery)}))
    },
    async updateDelivery(target,delivery) {
      db.prepare('UPDATE logging_diagnostics SET delivery=? WHERE tenant=? AND source=? AND record=?').run(JSON.stringify(delivery),target.tenantId,target.source,target.id)
    },
    async expired(before,limit,after) {
      if(!Number.isSafeInteger(limit)||limit<1||limit>100||!Number.isFinite(Date.parse(before)))throw new Error('LOG_INVALID_LIMIT')
      return after ? db.prepare('SELECT tenant AS tenantId,source,record AS id FROM logging_diagnostics WHERE receipt<=? AND (tenant,source,record)>(?,?,?) ORDER BY tenant,source,record LIMIT ?').all(Date.parse(before),after.tenantId,after.source,after.id,limit) : db.prepare('SELECT tenant AS tenantId,source,record AS id FROM logging_diagnostics WHERE receipt<=? ORDER BY tenant,source,record LIMIT ?').all(Date.parse(before),limit)
    },
    async retentionRecord(target) {
      const row=db.prepare('SELECT payload FROM logging_diagnostics WHERE tenant=? AND source=? AND record=?').get(target.tenantId,target.source,target.id)
      return row?JSON.parse(row.payload):null
    },
    async expire(target,before) {
      const result=db.prepare('DELETE FROM logging_diagnostics WHERE tenant=? AND source=? AND record=? AND receipt<=?').run(target.tenantId,target.source,target.id,Date.parse(before))
      return result.changes?'removed':'absent'
    },
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
        if(action==='erase')db.prepare('DELETE FROM logging_diagnostics WHERE tenant=? AND source=? AND record=?').run(tenantId,source,id)
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
