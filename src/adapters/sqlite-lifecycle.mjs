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
      CREATE TABLE IF NOT EXISTS logging_delivery_recovery (
        tenant TEXT NOT NULL, source TEXT NOT NULL, request TEXT NOT NULL,
        record TEXT NOT NULL, destination TEXT NOT NULL, mode TEXT NOT NULL,
        subject TEXT NOT NULL, approval TEXT NOT NULL, state TEXT NOT NULL,
        evidence TEXT, stamp TEXT NOT NULL,
        PRIMARY KEY(tenant,source,request)
      );
      CREATE TABLE IF NOT EXISTS logging_sink_state (
        tenant TEXT NOT NULL, source TEXT NOT NULL, destination TEXT NOT NULL,
        state TEXT NOT NULL, observed_at TEXT NOT NULL,
        PRIMARY KEY(tenant,source,destination)
      );
      CREATE TABLE IF NOT EXISTS logging_outbox (
        seq INTEGER PRIMARY KEY, id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
        acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0,1))
      );
    `)
    // Additive migration: legacy acknowledgements have unknown time and cannot be compacted.
    if(!db.prepare('PRAGMA table_info(logging_outbox)').all().some(c=>c.name==='acknowledged_at'))db.exec('ALTER TABLE logging_outbox ADD COLUMN acknowledged_at INTEGER')
    // A fresh opaque version on every insert/update prevents stale retirement approvals.
    for(const table of ['logging_requests','logging_lifecycle','logging_delivery_recovery','logging_sink_state']){
      if(!db.prepare('PRAGMA table_info('+table+')').all().some(c=>c.name==='retirement_version')){
        db.exec('ALTER TABLE '+table+" ADD COLUMN retirement_version TEXT NOT NULL DEFAULT ''")
      }
      db.exec("UPDATE "+table+" SET retirement_version=lower(hex(randomblob(16))) WHERE retirement_version=''")
    }
  } catch (error) { db.close(); throw error }
  const lookup = db.prepare("SELECT fingerprint FROM logging_requests WHERE tenant=?1 AND source=?2 AND request=?3 UNION ALL SELECT 'delivery-recovery' FROM logging_delivery_recovery WHERE tenant=?1 AND source=?2 AND request=?3 LIMIT 1")
  const insertRequest = db.prepare('INSERT INTO logging_requests(tenant,source,request,fingerprint,retirement_version) VALUES(?,?,?,?,lower(hex(randomblob(16))))')
  const mutate = db.prepare(`INSERT INTO logging_lifecycle(tenant,source,record,state,retirement_version) VALUES(?,?,?,?,lower(hex(randomblob(16))))
    ON CONFLICT(tenant,source,record) DO UPDATE SET state=CASE
    WHEN state='erased' THEN 'erased' ELSE excluded.state END, retirement_version=excluded.retirement_version`)
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
  // Conservative eligibility: no retained outbox reference, no unresolved reservation,
  // and no diagnostic or outstanding request in the marker's scope.
  const eligible = `
    SELECT 'request' AS kind,r.tenant AS tenantId,r.source,r.request AS id,r.retirement_version AS version
    FROM logging_requests r
    WHERE json_extract(r.fingerprint,'$[0]')!='external-pending'
      AND NOT EXISTS(SELECT 1 FROM logging_outbox o WHERE json_extract(o.payload,'$.tenantId')=r.tenant AND json_extract(o.payload,'$.source')=r.source AND json_extract(o.payload,'$.requestId')=r.request)
    UNION ALL
    SELECT 'marker' AS kind,l.tenant AS tenantId,l.source,l.record AS id,l.retirement_version AS version
    FROM logging_lifecycle l
    WHERE NOT EXISTS(SELECT 1 FROM logging_diagnostics d WHERE d.tenant=l.tenant AND d.source=l.source AND d.record=l.record)
      AND NOT EXISTS(SELECT 1 FROM logging_requests r WHERE r.tenant=l.tenant AND r.source=l.source)
      AND NOT EXISTS(SELECT 1 FROM logging_delivery_recovery r WHERE r.tenant=l.tenant AND r.source=l.source AND r.record=l.record)
      AND NOT EXISTS(SELECT 1 FROM logging_outbox o WHERE json_extract(o.payload,'$.tenantId')=l.tenant AND json_extract(o.payload,'$.source')=l.source AND json_extract(o.payload,'$.recordId')=l.record)
    UNION ALL
    SELECT 'recovery',r.tenant,r.source,r.request,r.retirement_version
    FROM logging_delivery_recovery r
    WHERE r.state IN ('delivered','refused')
      AND NOT EXISTS(SELECT 1 FROM logging_diagnostics d WHERE d.tenant=r.tenant AND d.source=r.source AND d.record=r.record)
    UNION ALL
    SELECT 'sink',s.tenant,s.source,s.destination,s.retirement_version
    FROM logging_sink_state s
    WHERE NOT EXISTS(SELECT 1 FROM logging_outbox o WHERE json_extract(o.payload,'$.tenantId')=s.tenant AND json_extract(o.payload,'$.source')=s.source AND json_extract(o.payload,'$.destination')=s.destination)
      AND NOT EXISTS(SELECT 1 FROM logging_diagnostics d WHERE d.tenant=s.tenant AND d.source=s.source)
  `
  return {
    async retirementCandidates(limit,after) {
      if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw new Error('LOG_INVALID_LIMIT')
      return db.prepare('SELECT * FROM ('+eligible+') WHERE (kind,tenantId,source,id)>(?,?,?,?) ORDER BY kind,tenantId,source,id LIMIT ?').all(after?.kind??'',after?.tenantId??'',after?.source??'',after?.id??'',limit)
    },
    async retire(candidate) {
      const {kind,tenantId,source,id,version}=candidate
      if(!['request','marker','recovery','sink'].includes(kind))return 'blocked'
      try {
        db.exec('BEGIN IMMEDIATE')
        const current=db.prepare('SELECT * FROM ('+eligible+') WHERE kind=? AND tenantId=? AND source=? AND id=?').get(kind,tenantId,source,id)
        if(!current||current.version!==version){db.exec('ROLLBACK');return 'blocked'}
        const sql=({request:'DELETE FROM logging_requests WHERE tenant=? AND source=? AND request=? AND retirement_version=?',marker:'DELETE FROM logging_lifecycle WHERE tenant=? AND source=? AND record=? AND retirement_version=?',recovery:'DELETE FROM logging_delivery_recovery WHERE tenant=? AND source=? AND request=? AND retirement_version=?',sink:'DELETE FROM logging_sink_state WHERE tenant=? AND source=? AND destination=? AND retirement_version=?'})[kind]
        const result=db.prepare(sql).run(tenantId,source,id,version)
        db.exec('COMMIT');return result.changes?'removed':'absent'
      }catch(error){try{db.exec('ROLLBACK')}catch{}throw error}
    },
    async health() {
      return {capacity,requests:requestCount.get().n,markers:markerCount.get().n,events:eventCount.get().n,reservations:reservations.get().n,pending:pendingCount.get().n,oldestPendingAt:oldestPending.get()?.stamp??null}
    },
    async observeSink(observation) {
      const {tenantId,source,destination,state,committedAt,timeQuality}=observation
      if(![tenantId,source,destination].every(v=>typeof v==='string'&&/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}$/.test(v))||
         !['ready','degraded'].includes(state)||!['verified','degraded','unknown'].includes(timeQuality)||
         !Number.isFinite(Date.parse(committedAt)))return 'uncertain'
      try {
        db.exec('BEGIN IMMEDIATE')
        const previous=db.prepare('SELECT state,observed_at FROM logging_sink_state WHERE tenant=? AND source=? AND destination=?').get(tenantId,source,destination)
        if(previous&&Date.parse(previous.observed_at)>Date.parse(committedAt)){db.exec('ROLLBACK');return 'uncertain'}
        const transition=previous?previous.state!==state:state==='degraded'
        if((!previous&&db.prepare('SELECT COUNT(*) AS n FROM logging_sink_state').get().n>=capacity)||(transition&&eventFull())){
          db.exec('ROLLBACK');return 'full'
        }
        db.prepare('INSERT INTO logging_sink_state(tenant,source,destination,state,observed_at,retirement_version) VALUES(?,?,?,?,?,lower(hex(randomblob(16)))) ON CONFLICT(tenant,source,destination) DO UPDATE SET state=excluded.state,observed_at=excluded.observed_at,retirement_version=excluded.retirement_version').run(tenantId,source,destination,state,committedAt)
        if(transition){
          const event={version:1,id:randomUUID(),type:state==='degraded'?'logging.sink.degraded':'logging.sink.recovered',tenantId,source,destination,state,committedAt,timeQuality}
          insertEvent.run(event.id,JSON.stringify(event))
        }
        db.exec('COMMIT');return transition?'committed':'unchanged'
      }catch{try{db.exec('ROLLBACK')}catch{}return 'uncertain'}
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
    async reserveDeliveryRecovery(request,expected,subjectId,evidenceRef,stamp) {
      const {target,destination,requestId,mode}=request
      const {tenantId,source,id}=target
      try {
        db.exec('BEGIN IMMEDIATE')
        const previous=lookup.get(tenantId,source,requestId)
        const recovery=db.prepare('SELECT 1 FROM logging_delivery_recovery WHERE tenant=? AND source=? AND request=?').get(tenantId,source,requestId)
        const row=db.prepare('SELECT delivery FROM logging_diagnostics WHERE tenant=? AND source=? AND record=?').get(tenantId,source,id)
        const current=row?JSON.parse(row.delivery).find(d=>d.target===destination):undefined
        const eligible=current&&current.status===expected.status&&current.attempts===expected.attempts&&current.time===expected.time&&
          (mode==='retry'?current.status==='exhausted'&&current.attempts>0&&current.attempts<3:mode==='reconcile'&&current.status==='uncertain')
        const pending=db.prepare("SELECT 1 FROM logging_delivery_recovery WHERE tenant=? AND source=? AND record=? AND destination=? AND state='pending'").get(tenantId,source,id,destination)
        if(previous||recovery||!eligible||state.get(tenantId,source,id)||(mode==='retry'&&pending)){
          db.exec('ROLLBACK');return 'blocked'
        }
        if(db.prepare('SELECT COUNT(*) AS n FROM logging_delivery_recovery').get().n>=capacity){db.exec('ROLLBACK');return 'full'}
        db.prepare('INSERT INTO logging_delivery_recovery(tenant,source,request,record,destination,mode,subject,approval,state,evidence,stamp,retirement_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,lower(hex(randomblob(16))))').run(tenantId,source,requestId,id,destination,mode,subjectId,evidenceRef,'pending',null,stamp)
        // Reserve before the external call. A crash can never make this a definite refusal.
        if(mode==='retry'){
          const delivery=JSON.parse(row.delivery).map(d=>d.target===destination?{...d,status:'uncertain',attempts:d.attempts+1,time:stamp}:d)
          db.prepare('UPDATE logging_diagnostics SET delivery=? WHERE tenant=? AND source=? AND record=?').run(JSON.stringify(delivery),tenantId,source,id)
        }
        db.exec('COMMIT');return 'reserved'
      }catch{try{db.exec('ROLLBACK')}catch{}return 'uncertain'}
    },
    async finishDeliveryRecovery(request,result,stamp) {
      const {target,destination,requestId,mode}=request
      const {tenantId,source,id}=target
      try {
        db.exec('BEGIN IMMEDIATE')
        const reserved=db.prepare("SELECT * FROM logging_delivery_recovery WHERE tenant=? AND source=? AND request=? AND state='pending'").get(tenantId,source,requestId)
        const row=db.prepare('SELECT delivery FROM logging_diagnostics WHERE tenant=? AND source=? AND record=?').get(tenantId,source,id)
        if(!reserved||reserved.record!==id||reserved.destination!==destination||reserved.mode!==mode||!row||state.get(tenantId,source,id)){
          db.exec('ROLLBACK');return 'uncertain'
        }
        const delivery=JSON.parse(row.delivery)
        const current=delivery.find(d=>d.target===destination)
        if(!current||current.status!=='uncertain'){db.exec('ROLLBACK');return 'uncertain'}
        const status=result.status==='refused'?'exhausted':result.status
        db.prepare('UPDATE logging_diagnostics SET delivery=? WHERE tenant=? AND source=? AND record=?').run(JSON.stringify(delivery.map(d=>d.target===destination?{...d,status,time:stamp}:d)),tenantId,source,id)
        db.prepare('UPDATE logging_delivery_recovery SET state=?,evidence=?,stamp=?,retirement_version=lower(hex(randomblob(16))) WHERE tenant=? AND source=? AND request=?').run(result.status,result.evidenceRef??null,stamp,tenantId,source,requestId)
        if(mode==='reconcile'&&result.status!=='uncertain'){
          db.prepare("UPDATE logging_delivery_recovery SET state=?,evidence=?,stamp=?,retirement_version=lower(hex(randomblob(16))) WHERE tenant=? AND source=? AND record=? AND destination=? AND state IN ('pending','uncertain')").run(result.status,result.evidenceRef,stamp,tenantId,source,id,destination)
        }
        db.exec('COMMIT');return 'committed'
      }catch{try{db.exec('ROLLBACK')}catch{}return 'uncertain'}
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
        if(reserved)db.prepare('UPDATE logging_requests SET fingerprint=?, retirement_version=lower(hex(randomblob(16))) WHERE tenant=? AND source=? AND request=?').run(fingerprint,tenantId,source,requestId)
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
