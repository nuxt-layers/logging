import { DatabaseSync } from 'node:sqlite'
import { randomUUID } from 'node:crypto'

// Host owns file path, filesystem permissions, backups, retention and lifecycle.
// One Logging-owned database per deployment, not a shared application table.
export function createSqliteLifecycleJournal(path, capacity = 100000) {
  if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 1000000) throw new Error('LOG_JOURNAL_CAPACITY')
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
    // Additive migration: legacy acknowledgements have unknown time and cannot be compacted.
    if(!db.prepare('PRAGMA table_info(logging_outbox)').all().some(c=>c.name==='acknowledged_at'))db.exec('ALTER TABLE logging_outbox ADD COLUMN acknowledged_at INTEGER')
  } catch (error) { db.close(); throw error }
  const lookup = db.prepare('SELECT fingerprint FROM logging_requests WHERE tenant=? AND source=? AND request=?')
  const insertRequest = db.prepare('INSERT INTO logging_requests VALUES(?,?,?,?)')
  const mutate = db.prepare(`INSERT INTO logging_lifecycle VALUES(?,?,?,?)
    ON CONFLICT(tenant,source,record) DO UPDATE SET state=CASE
    WHEN state='erased' THEN 'erased' ELSE excluded.state END`)
  const state = db.prepare('SELECT state FROM logging_lifecycle WHERE tenant=? AND source=? AND record=?')
  const insertEvent = db.prepare('INSERT INTO logging_outbox(id,payload) VALUES(?,?)')
  const pending = db.prepare('SELECT payload FROM logging_outbox WHERE acknowledged=0 ORDER BY seq LIMIT ?')
  const acknowledge = db.prepare('UPDATE logging_outbox SET acknowledged=1, acknowledged_at=? WHERE id=? AND acknowledged=0')
  const requestCount=db.prepare('SELECT COUNT(*) AS n FROM logging_requests')
  const markerCount=db.prepare('SELECT COUNT(*) AS n FROM logging_lifecycle')
  const eventCount=db.prepare('SELECT COUNT(*) AS n FROM logging_outbox')
  const reservations=db.prepare("SELECT COUNT(*) AS n FROM logging_requests WHERE substr(fingerprint,1,20)='[\"external-pending\",'")
  const pendingCount=db.prepare('SELECT COUNT(*) AS n FROM logging_outbox WHERE acknowledged=0')
  const oldestPending=db.prepare("SELECT json_extract(payload,'$.committedAt') AS stamp FROM logging_outbox WHERE acknowledged=0 ORDER BY seq LIMIT 1")
  const eventFull=()=>eventCount.get().n+reservations.get().n>=capacity
  const requestFull=()=>requestCount.get().n>=capacity
  const detailOf=x=>x.kind==='provider'?{kind:'provider',action:x.action,recordId:x.recordId,destination:x.destination}:x.kind==='policy'?{kind:'policy',expectedRevision:x.expectedRevision,revision:x.revision}:null
  const reservation=(subjectId,detail)=>JSON.stringify(['external-pending',subjectId,detail])
  return {
    async health() {
      return {capacity,requests:requestCount.get().n,markers:markerCount.get().n,events:eventCount.get().n,reservations:reservations.get().n,pending:pendingCount.get().n,oldestPendingAt:oldestPending.get()?.stamp??null}
    },
    async reserveExternal(intent) {
      const {tenantId,source,requestId,subjectId}=intent
      const detail=detailOf(intent)
      if(!detail)return 'conflict'
      const fingerprint=reservation(subjectId,detail)
      try {
        db.exec('BEGIN IMMEDIATE')
        const previous=lookup.get(tenantId,source,requestId)
        if(previous){db.exec('ROLLBACK');return previous.fingerprint===fingerprint?'uncertain':'conflict'}
        if(requestFull()||eventFull()){db.exec('ROLLBACK');return 'full'}
        insertRequest.run(tenantId,source,requestId,fingerprint)
        db.exec('COMMIT');return 'reserved'
      } catch {try{db.exec('ROLLBACK')}catch{}return 'uncertain'}
    },
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
    async expire(target,before,committedAt) {
      if(!Number.isFinite(Date.parse(before))||!Number.isFinite(Date.parse(committedAt)))throw new Error('LOG_INVALID_LIMIT')
      try {
        db.exec('BEGIN IMMEDIATE')
        if(eventFull())throw new Error('LOG_JOURNAL_CAPACITY')
        const result=db.prepare('DELETE FROM logging_diagnostics WHERE tenant=? AND source=? AND record=? AND receipt<=?').run(target.tenantId,target.source,target.id,Date.parse(before))
        if(!result.changes){db.exec('ROLLBACK');return 'absent'}
        const event={version:1,id:randomUUID(),type:'logging.expired',tenantId:target.tenantId,source:target.source,recordId:target.id,committedAt}
        insertEvent.run(event.id,JSON.stringify(event))
        db.exec('COMMIT')
        return 'removed'
      } catch(error){try{db.exec('ROLLBACK')}catch{}throw error}
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
        if(requestFull()||eventFull()||(!state.get(tenantId,source,id)&&markerCount.get().n>=capacity)){db.exec('ROLLBACK');return 'full'}
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
    async recordExternalObservation(observation) {
      const {tenantId,source,requestId,subjectId,evidenceRef,committedAt}=observation
      const detail=detailOf(observation)
      if(!detail||!Number.isFinite(Date.parse(committedAt)))return 'conflict'
      const fingerprint=JSON.stringify(['external',subjectId,evidenceRef,detail])
      try {
        db.exec('BEGIN IMMEDIATE')
        const previous=lookup.get(tenantId,source,requestId)
        const reserved=previous?.fingerprint===reservation(subjectId,detail)
        if(previous&&!reserved){db.exec('ROLLBACK');return previous.fingerprint===fingerprint?'duplicate':'conflict'}
        if(!reserved&&(requestFull()||eventFull())){db.exec('ROLLBACK');return 'full'}
        const event={version:1,id:randomUUID(),type:'logging.external.confirmation.observed',tenantId,source,requestId,subjectId,evidenceRef,committedAt,...detail}
        if(reserved)db.prepare('UPDATE logging_requests SET fingerprint=? WHERE tenant=? AND source=? AND request=?').run(fingerprint,tenantId,source,requestId)
        else insertRequest.run(tenantId,source,requestId,fingerprint)
        insertEvent.run(event.id,JSON.stringify(event))
        db.exec('COMMIT')
        return 'committed'
      } catch {try{db.exec('ROLLBACK')}catch{}return 'uncertain'}
    },
    async recordExportRelease(release) {
      const {tenantId,source,requestId,subjectId,evidenceRef,committedAt,count}=release
      if(!Number.isSafeInteger(count)||count<0||count>100||!Number.isFinite(Date.parse(committedAt)))return 'conflict'
      try {
        db.exec('BEGIN IMMEDIATE')
        if(lookup.get(tenantId,source,requestId)){db.exec('ROLLBACK');return 'conflict'}
        if(requestFull()||eventFull()){db.exec('ROLLBACK');return 'full'}
        const event={version:1,id:randomUUID(),type:'logging.export.released',tenantId,source,requestId,subjectId,evidenceRef,committedAt,count}
        insertRequest.run(tenantId,source,requestId,JSON.stringify(['export',subjectId,evidenceRef]))
        insertEvent.run(event.id,JSON.stringify(event))
        db.exec('COMMIT')
        return 'committed'
      } catch {try{db.exec('ROLLBACK')}catch{}return 'uncertain'}
    },
    async visibility({tenantId,source,id}) {
      return state.get(tenantId,source,id)?.state??'visible'
    },
    async pending(limit) {
      if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw new Error('LOG_INVALID_LIMIT')
      return pending.all(limit).map(row=>JSON.parse(row.payload))
    },
    async acknowledge(eventId,acknowledgedAt) {
      const ms=Date.parse(acknowledgedAt??'')
      acknowledge.run(Number.isFinite(ms)?ms:null,eventId)
    },
    maintenance: {
      async acknowledgedBefore(before,limit,after) {
        const ms=Date.parse(before)
        if(!Number.isFinite(ms)||!Number.isSafeInteger(limit)||limit<1||limit>100)throw new Error('LOG_INVALID_LIMIT')
        return db.prepare('SELECT payload FROM logging_outbox WHERE acknowledged=1 AND acknowledged_at<=? AND id>? ORDER BY id LIMIT ?').all(ms,after??'',limit).map(row=>JSON.parse(row.payload))
      },
      async removeAcknowledged(eventId,before) {
        const ms=Date.parse(before)
        if(!Number.isFinite(ms))throw new Error('LOG_INVALID_LIMIT')
        return db.prepare('DELETE FROM logging_outbox WHERE id=? AND acknowledged=1 AND acknowledged_at<=?').run(eventId,ms).changes?'removed':'absent'
      }
    },
    close() { db.close() }
  }
}
