import { token, instant, bounded, levels, compileRules, effectiveRules, validateDiagnostic, diagnosticCode, plain, fields } from "../policy/validation.js";
import { deliver } from "../sinks/delivery.js";
export function createLogging(options) {
    const capacity = options.capacity ?? 1000;
    const rate = options.ratePerMinute ?? 120;
    const timeout = options.timeoutMs ?? 1000;
    const retention = options.retentionMs;
    const revision = options.policyRevision;
    // Snapshot provider references; callers must not mutate a composed trust boundary.
    options = { ...options };
    const localSink = options.localSink;
    const store = options.diagnosticStore;
    if (store && options.lifecycleJournal && store !== options.lifecycleJournal)
        throw new Error('LOG_INVALID_CONFIG');
    const journal = store ?? options.lifecycleJournal;
    let relayBusy = false;
    let journalCursor;
    let retirementCursor;
    let sinkObservationFailures = 0;
    const journalRetention = options.journalRetention ? { ...options.journalRetention } : undefined;
    if (journalRetention && (!journal?.maintenance || !bounded(journalRetention.retentionMs, 3650 * 86400000) || typeof journalRetention.resolvePreservation !== 'function'))
        throw new Error('LOG_INVALID_CONFIG');
    if (!bounded(capacity, 10000) || !bounded(rate, 10000) || !bounded(timeout, 30000) || !bounded(retention, 86400000 * 30) || !token(revision) || !localSink || typeof localSink.write !== 'function')
        throw new Error('LOG_INVALID_CONFIG');
    const external = new Map();
    for (const target of options.externalSinks ?? []) {
        if (!token(target.id) || target.id === 'local' || external.has(target.id) || external.size >= 8 || !bounded(target.maxAttempts ?? 1, 3) || typeof target.sink?.write !== 'function')
            throw new Error('LOG_INVALID_SINK');
        external.set(target.id, { sink: target.sink, maxAttempts: target.maxAttempts ?? 1 });
    }
    const floors = compileRules(options.rules, new Set(external.keys()));
    const purposeRetention = new Map(Object.entries(options.purposeRetention ?? {}));
    for (const [purpose, ms] of purposeRetention)
        if (!token(purpose) || !bounded(ms, retention) || ![...floors.values()].some(r => r.purpose === purpose))
            throw new Error('LOG_INVALID_CONFIG');
    function retentionFor(record) {
        return Math.min(retention, purposeRetention.get(record.purpose) ?? retention, record.retentionMs ?? retention);
    }
    let retentionCursor;
    const rows = [];
    // Bounded process-lifetime scope counters; never attribute unauthenticated or busy intake.
    const scopedIntake = new Map();
    let scopeCoverageLimited = false;
    function countScoped(key, receipt) {
        let counts = scopedIntake.get(key);
        if (!counts) {
            if (scopedIntake.size >= capacity) {
                scopeCoverageLimited = true;
                return receipt;
            }
            counts = { accepted: 0, rejected: 0, throttled: 0, unavailable: 0, uncertain: 0 };
            scopedIntake.set(key, counts);
        }
        counts[receipt.status] = Math.min(Number.MAX_SAFE_INTEGER, counts[receipt.status] + 1);
        return receipt;
    }
    const buckets = new Map();
    // Uncertain external sinks remain quarantined until recomposition, so ignored aborts cannot accumulate.
    const quarantined = new Set();
    let lifecycleBusy = false, lifecycleGeneration = 0;
    let lifecycleAbort;
    let relayAbort;
    let lifecycleDone, relayDone;
    let finishLifecycle, finishRelay;
    function beginLifecycle() { lifecycleGeneration++; lifecycleBusy = true; lifecycleDone = new Promise(resolve => { finishLifecycle = resolve; }); }
    function endLifecycle() { lifecycleGeneration++; lifecycleBusy = false; finishLifecycle?.(); lifecycleDone = undefined; finishLifecycle = undefined; lifecycleAbort = undefined; }
    function beginRelay() { relayBusy = true; relayDone = new Promise(resolve => { finishRelay = resolve; }); }
    function endRelay() { relayBusy = false; finishRelay?.(); relayDone = undefined; finishRelay = undefined; relayAbort = undefined; }
    let busy = false, closed = false, closing = false, drops = 0, uncertain = 0;
    let lastDelivery = 'unknown';
    let activeSince = null;
    const intake = { accepted: 0, rejected: 0, throttled: 0, unavailable: 0, uncertain: 0 };
    const countReceipt = (receipt) => { intake[receipt.status] = Math.min(Number.MAX_SAFE_INTEGER, intake[receipt.status] + 1); return receipt; };
    let lastTime = -Infinity;
    let active;
    let activeAbort;
    function time() {
        const t = options.now();
        if (!instant(t.instant) || !['verified', 'degraded', 'unknown'].includes(t.quality))
            throw new Error('LOG_CLOCK');
        const ms = Date.parse(t.instant);
        if (ms < lastTime)
            throw new Error('LOG_CLOCK');
        lastTime = ms;
        return { ...t, ms };
    }
    function expire(ms) {
        for (let i = rows.length - 1; i >= 0; i--) {
            const row = rows[i];
            if (row && ms - Date.parse(row.record.receiptTime) >= retentionFor(row.record))
                rows.splice(i, 1);
        }
    }
    async function authorised(event, action, resource) {
        const { tenantId, source } = resource;
        try {
            const caller = await options.resolveCaller(event);
            return !!caller && caller.tenantId === tenantId && token(tenantId) && token(source) && (await options.decide(caller, action, Object.freeze({ ...resource }))).allowed === true;
        }
        catch {
            return false;
        }
    }
    async function policy(tenantId, source) {
        if (!options.resolvePolicy)
            return { revision, rules: [...floors.values()].filter(r => r.source === source) };
        const p = await options.resolvePolicy({ tenantId, source });
        if (!p || !token(p.revision))
            return null;
        return { revision: p.revision, rules: effectiveRules(floors, p.rules, source, new Set(external.keys())) };
    }
    async function observeSink(record, destination, state) {
        if (!journal?.observeSink)
            return;
        try {
            const stamp = time();
            const result = await journal.observeSink({ tenantId: record.tenantId, source: record.source, destination, state, committedAt: stamp.instant, timeQuality: stamp.quality });
            if (result === 'committed' || result === 'unchanged')
                return;
        }
        catch { }
        sinkObservationFailures = Math.min(Number.MAX_SAFE_INTEGER, sinkObservationFailures + 1);
    }
    async function perform(event, input, scope) {
        try {
            const producer = await options.resolveProducer(event);
            if (!producer || !token(producer.tenantId) || !token(producer.source) || (await options.decide(producer, 'logging.submit', { tenantId: producer.tenantId, source: producer.source })).allowed !== true)
                return { status: 'rejected', code: 'LOG_DENIED' };
            scope(JSON.stringify([producer.tenantId, producer.source]));
            const p = await policy(producer.tenantId, producer.source);
            if (!p)
                return { status: 'unavailable', code: 'LOG_POLICY_UNAVAILABLE' };
            const code = diagnosticCode(input);
            if (!code)
                return { status: 'rejected', code: 'LOG_INVALID' };
            const rule = p.rules.find(r => r.source === producer.source && r.code === code);
            if (!rule)
                return { status: 'rejected', code: 'LOG_RULE' };
            const d = validateDiagnostic(input, rule);
            if (!d)
                return { status: 'rejected', code: 'LOG_INVALID' };
            const t = time();
            expire(t.ms);
            const minute = Math.floor(t.ms / 60000);
            for (const [key, b] of buckets)
                if (b.minute !== minute)
                    buckets.delete(key);
            const key = JSON.stringify([producer.tenantId, producer.source]);
            const b = buckets.get(key) ?? { count: 0, minute };
            if (b.count >= rate || (!buckets.has(key) && buckets.size >= capacity)) {
                drops++;
                return { status: 'throttled', code: 'LOG_RATE' };
            }
            b.count++;
            buckets.set(key, b);
            if (levels.indexOf(d.severity) < levels.indexOf(rule.minimumSeverity ?? 'debug') || (b.count - 1) % (rule.sampleEvery ?? 1) !== 0) {
                drops++;
                return { status: 'rejected', code: 'LOG_FILTERED' };
            }
            if ((!store && rows.length >= capacity) || closing || closed) {
                drops++;
                return { status: 'unavailable', code: 'LOG_CAPACITY' };
            }
            const row = Object.freeze({ id: crypto.randomUUID(), tenantId: producer.tenantId, source: producer.source, code: d.code, severity: d.severity, occurrenceTime: d.occurrenceTime, receiptTime: t.instant, timeQuality: t.quality, correlation: d.correlation, attributes: Object.freeze(d.attributes ?? {}), policyRevision: p.revision, purpose: rule.purpose, category: rule.category ?? 'technical', classification: 'technical', retentionMs: purposeRetention.get(rule.purpose) ?? retention });
            // Persist the sanitised record before invoking any sink. Recovery never replays writes.
            if (store) {
                let saved;
                try {
                    saved = await store.save({ record: row, delivery: [] }, capacity);
                }
                catch {
                    saved = 'uncertain';
                }
                if (saved === 'full') {
                    drops++;
                    return { status: 'unavailable', code: 'LOG_CAPACITY' };
                }
                if (saved !== 'stored') {
                    close();
                    return { status: 'uncertain', code: 'LOG_STORE_UNCERTAIN', id: row.id };
                }
                if (closing || closed)
                    return { status: 'uncertain', code: 'LOG_STORE_UNCERTAIN', id: row.id };
            }
            const controller = new AbortController();
            activeAbort = controller;
            const local = await deliver(localSink, row, timeout, 1, controller.signal);
            lastDelivery = local.outcome;
            const localState = { target: 'local', status: local.outcome === 'refused' ? 'exhausted' : local.outcome, attempts: local.attempts, time: time().instant };
            if (store) {
                try {
                    await store.updateDelivery({ tenantId: row.tenantId, source: row.source, id: row.id }, [localState]);
                }
                catch {
                    close();
                    return { status: 'uncertain', code: 'LOG_STORE_UNCERTAIN', id: row.id };
                }
            }
            await observeSink(row, 'local', local.outcome === 'delivered' ? 'ready' : 'degraded');
            if (local.outcome === 'refused') {
                drops++;
                return { status: 'unavailable', code: 'LOG_SINK_REFUSED' };
            }
            if (local.outcome !== 'delivered') {
                uncertain++;
                closed = true;
                return { status: 'uncertain', code: 'LOG_SINK_UNCERTAIN', id: row.id };
            }
            const item = { record: row, restricted: false, delivery: [{ target: 'local', status: 'delivered', attempts: local.attempts, time: localState.time }] };
            if (!store && !closing && !closed)
                rows.push(item);
            for (const target of rule.destinations ?? []) {
                const ext = external.get(target);
                if (!ext || quarantined.has(target) || controller.signal.aborted) {
                    item.delivery.push({ target, status: 'exhausted', attempts: 0, time: time().instant });
                    drops++;
                    await observeSink(row, target, 'degraded');
                    continue;
                }
                const result = await deliver(ext.sink, row, timeout, ext.maxAttempts, controller.signal);
                const status = result.outcome === 'refused' ? 'exhausted' : result.outcome;
                item.delivery.push({ target, status, attempts: result.attempts, time: time().instant });
                await observeSink(row, target, status === 'delivered' ? 'ready' : 'degraded');
                if (status === 'uncertain') {
                    uncertain++;
                    quarantined.add(target);
                }
                if (status === 'exhausted')
                    drops++;
            }
            if (store) {
                try {
                    await store.updateDelivery({ tenantId: row.tenantId, source: row.source, id: row.id }, item.delivery);
                }
                catch {
                    close();
                    return { status: 'uncertain', code: 'LOG_STORE_UNCERTAIN', id: row.id };
                }
            }
            return { status: 'accepted', code: 'LOG_ACCEPTED', id: row.id };
        }
        catch {
            return { status: 'unavailable', code: 'LOG_PROVIDER' };
        }
        finally {
            activeAbort = undefined;
        }
    }
    function submit(event, input) {
        if (closed || closing || busy || lifecycleBusy) {
            drops++;
            return Promise.resolve(countReceipt({ status: 'unavailable', code: 'LOG_BUSY' }));
        }
        busy = true;
        activeSince = performance.now();
        let scopeKey;
        const task = perform(event, input, key => { scopeKey = key; }).then(receipt => countReceipt(scopeKey === undefined ? receipt : countScoped(scopeKey, receipt))).finally(() => { busy = false; active = undefined; activeSince = null; });
        active = task;
        return task;
    }
    function snapshotQuery(q) {
        if (!plain(q) || !fields(q, ['tenantId', 'source', 'from', 'to', 'limit', 'offset', 'severity', 'code', 'correlation', 'id']))
            return null;
        return { ...q };
    }
    function validQuery(q) {
        return !!q && token(q.tenantId) && token(q.source) && instant(q.from) && instant(q.to) && bounded(q.limit, 100) && Number.isSafeInteger(q.offset ?? 0) && (q.offset ?? 0) >= 0 && Date.parse(q.to) >= Date.parse(q.from) && Date.parse(q.to) - Date.parse(q.from) <= 86400000 * 30 && (q.severity === undefined || levels.includes(q.severity)) && (q.code === undefined || token(q.code)) && (q.correlation === undefined || token(q.correlation)) && (q.id === undefined || token(q.id));
    }
    function matches(q, r) {
        return r.tenantId === q.tenantId && r.source === q.source && Date.parse(r.receiptTime) >= Date.parse(q.from) && Date.parse(r.receiptTime) <= Date.parse(q.to) && (q.severity === undefined || r.severity === q.severity) && (q.code === undefined || r.code === q.code) && (q.correlation === undefined || r.correlation === q.correlation) && (q.id === undefined || r.id === q.id);
    }
    async function visibleEntries(q) {
        const loaded = store ? await store.load(q.tenantId, q.source, 10000) : rows;
        if (store && loaded.length > 10000)
            throw new Error('LOG_JOURNAL');
        const candidates = loaded.filter(x => !('restricted' in x && x.restricted) && matches(q, x.record) && (!store || Date.parse(x.record.receiptTime) > lastTime - retentionFor(x.record)));
        if (closing || closed)
            throw new Error('LOG_JOURNAL');
        if (!journal)
            return candidates;
        const result = [];
        for (const x of candidates) {
            const { tenantId, source, id } = x.record;
            const state = await journal.visibility({ tenantId, source, id });
            if (state === 'visible')
                result.push(x);
            else if (state !== 'restricted' && state !== 'erased')
                throw new Error('LOG_JOURNAL');
        }
        if (closing || closed)
            throw new Error('LOG_JOURNAL');
        return store ? result : result.filter(x => rows.some(r => r === x && !r.restricted));
    }
    async function checkRead(event, action, q, generation) {
        if (!await authorised(event, action, q) || lifecycleBusy || generation !== lifecycleGeneration || closing || closed)
            throw new Error('LOG_READ_INVALIDATED');
    }
    async function readDiagnostics(event, query) {
        const snapshot = snapshotQuery(query);
        if (!snapshot || !validQuery(snapshot) || !await authorised(event, 'logging.read', snapshot))
            return { status: 'rejected', records: [] };
        try {
            const generation = lifecycleGeneration;
            if (lifecycleBusy)
                throw new Error('LOG_READ_INVALIDATED');
            expire(time().ms);
            const offset = snapshot.offset ?? 0;
            const records = (await visibleEntries(snapshot)).slice(offset, offset + snapshot.limit).map(x => structuredClone(x.record));
            await checkRead(event, 'logging.read', snapshot, generation);
            return { status: 'completed', records, coverage: store ? 'durable diagnostic projection; pagination may shift' : 'process-local; incomplete; pagination may shift' };
        }
        catch {
            return { status: 'unavailable', records: [] };
        }
    }
    async function readDeliveryState(event, query) {
        const snapshot = snapshotQuery(query);
        if (!snapshot || !validQuery(snapshot) || !await authorised(event, 'logging.delivery.read', snapshot))
            return { status: 'rejected', records: [] };
        try {
            const generation = lifecycleGeneration;
            if (lifecycleBusy)
                throw new Error('LOG_READ_INVALIDATED');
            expire(time().ms);
            const offset = snapshot.offset ?? 0;
            const records = (await visibleEntries(snapshot)).slice(offset, offset + snapshot.limit).map(x => ({ id: x.record.id, delivery: x.delivery.map(d => ({ ...d })) }));
            await checkRead(event, 'logging.delivery.read', snapshot, generation);
            return { status: 'completed', records, coverage: store ? 'durable diagnostic projection; unconfirmed delivery may be omitted' : 'process-local; incomplete; in-flight destinations may be omitted' };
        }
        catch {
            return { status: 'unavailable', records: [] };
        }
    }
    async function readEffectivePolicy(event, scope) {
        if (!plain(scope) || !fields(scope, ['tenantId', 'source']) || !token(scope.tenantId) || !token(scope.source))
            return { status: 'rejected' };
        const { tenantId, source } = scope;
        if (!await authorised(event, 'logging.policy.read', { tenantId, source }))
            return { status: 'rejected' };
        try {
            const p = await policy(tenantId, source);
            return p ? { status: 'completed', policy: structuredClone(p) } : { status: 'unavailable' };
        }
        catch {
            return { status: 'unavailable' };
        }
    }
    async function lifecycleCaller(event, action, resource) {
        const { tenantId, source } = resource;
        try {
            const caller = await options.resolveCaller(event);
            if (!caller || caller.tenantId !== tenantId || !token(tenantId) || !token(source) || !token(caller.subjectId))
                return null;
            const snapshot = Object.freeze({ ...caller });
            return (await options.decide(snapshot, 'logging.' + action, Object.freeze({ ...resource }))).allowed === true ? snapshot : null;
        }
        catch {
            return null;
        }
    }
    // The approval port is an independent, attributable precondition, not a Logging receipt.
    async function approve(action, caller, requestId, target, destination) {
        if (!options.approveLifecycle)
            return { status: 'unknown' };
        try {
            const result = await options.approveLifecycle({ action, caller, requestId, destination, target: Object.freeze(structuredClone(target)) });
            if (result?.status === 'approved' && token(result.evidenceRef) && ['clear', 'held', 'unknown'].includes(result.preservation))
                return { status: 'approved', evidenceRef: result.evidenceRef, preservation: result.preservation };
            return { status: result?.status === 'blocked' ? 'blocked' : 'unknown' };
        }
        catch {
            return { status: 'unknown' };
        }
    }
    async function exportDiagnostics(event, query, requestId) {
        const q = snapshotQuery(query);
        if (!q || !validQuery(q) || !token(requestId))
            return { status: 'rejected', records: [] };
        const caller = await lifecycleCaller(event, 'export', { ...q, requestId });
        if (!caller)
            return { status: 'rejected', records: [] };
        if (busy || lifecycleBusy || closing || closed)
            return { status: 'unavailable', records: [] };
        beginLifecycle();
        try {
            const approval = await approve('export', caller, requestId, q);
            if (approval.status !== 'approved')
                return { status: approval.status === 'blocked' ? 'blocked' : 'unavailable', records: [] };
            if (closed || closing)
                return { status: 'unavailable', records: [] };
            // Recheck after the external approval; no data is disclosed on a revoked decision.
            const current = await lifecycleCaller(event, 'export', { ...q, requestId });
            if (!current || current.subjectId !== caller.subjectId || closed || closing)
                return { status: 'rejected', records: [] };
            expire(time().ms);
            const offset = q.offset ?? 0;
            const records = (await visibleEntries(q)).slice(offset, offset + q.limit).map(x => x.record);
            if (journal) {
                if (!journal.recordExportRelease)
                    return { status: 'unavailable', records: [] };
                if (closing || closed)
                    return { status: 'unavailable', records: [] };
                let outcome;
                try {
                    outcome = await journal.recordExportRelease({ tenantId: q.tenantId, source: q.source, requestId, subjectId: caller.subjectId, evidenceRef: approval.evidenceRef, committedAt: time().instant, count: records.length });
                }
                catch {
                    outcome = 'uncertain';
                }
                if (outcome === 'conflict')
                    return { status: 'rejected', records: [] };
                if (outcome === 'full')
                    return { status: 'unavailable', records: [] };
                if (outcome !== 'committed') {
                    close();
                    return { status: 'uncertain', records: [] };
                }
                if (closing || closed)
                    return { status: 'unavailable', records: [] };
            }
            const releaseCaller = await lifecycleCaller(event, 'export', { ...q, requestId });
            if (!releaseCaller || releaseCaller.subjectId !== caller.subjectId || closed || closing)
                return { status: 'rejected', records: [] };
            return { status: 'completed-local', requestId, evidenceRef: approval.evidenceRef, records: structuredClone(records), coverage: store ? 'durable diagnostic projection only; no provider export' : 'process-local only; incomplete; no provider export' };
        }
        catch {
            return { status: 'unavailable', records: [] };
        }
        finally {
            endLifecycle();
        }
    }
    async function changeLocalRecord(event, action, target, requestId) {
        if (!plain(target) || !fields(target, ['tenantId', 'source', 'id']) || !token(target.tenantId) || !token(target.source) || !token(target.id) || !token(requestId))
            return { status: 'rejected' };
        const q = { tenantId: target.tenantId, source: target.source, id: target.id };
        const caller = await lifecycleCaller(event, action, { ...q, requestId });
        if (!caller)
            return { status: 'rejected' };
        if (busy || lifecycleBusy || closing || closed)
            return { status: 'unavailable' };
        beginLifecycle();
        try {
            const approval = await approve(action, caller, requestId, q);
            if (approval.status !== 'approved')
                return { status: approval.status === 'blocked' ? 'blocked' : 'unavailable' };
            if (action === 'erase' && approval.preservation !== 'clear')
                return { status: approval.preservation === 'held' ? 'blocked' : 'unavailable' };
            if (closed || closing)
                return { status: 'unavailable' };
            const current = await lifecycleCaller(event, action, { ...q, requestId });
            if (!current || current.subjectId !== caller.subjectId)
                return { status: 'rejected' };
            if (closed || closing)
                return { status: 'unavailable' };
            if (journal) {
                // State and event must commit atomically in the Logging-owned journal.
                let outcome;
                try {
                    outcome = await journal.commit({ action, target: q, requestId, subjectId: caller.subjectId, evidenceRef: approval.evidenceRef, committedAt: time().instant });
                }
                catch {
                    outcome = 'uncertain';
                }
                if (outcome === 'conflict')
                    return { status: 'rejected' };
                if (outcome === 'full')
                    return { status: 'unavailable' };
                if (outcome !== 'committed' && outcome !== 'duplicate') {
                    close();
                    return { status: 'uncertain' };
                }
            }
            // A missing record and a previously removed record give the same local result.
            const index = rows.findIndex(x => x.record.tenantId === q.tenantId && x.record.source === q.source && x.record.id === q.id);
            if (index >= 0) {
                if (action === 'erase')
                    rows.splice(index, 1);
                else {
                    const row = rows[index];
                    if (row)
                        row.restricted = true;
                }
            }
            return { status: journal ? 'committed' : 'completed-local', requestId, evidenceRef: approval.evidenceRef, providerState: 'unknown' };
        }
        catch {
            return { status: 'unavailable' };
        }
        finally {
            endLifecycle();
        }
    }
    async function reserveExternal(intent) {
        if (!journal?.reserveExternal)
            return 'unavailable';
        try {
            const result = await journal.reserveExternal(intent);
            return result === 'reserved' ? 'reserved' : result === 'conflict' ? 'rejected' : result === 'full' ? 'unavailable' : 'uncertain';
        }
        catch {
            return 'uncertain';
        }
    }
    async function observeExternal(observation) {
        if (!journal)
            return true;
        if (!journal.recordExternalObservation)
            return false;
        try {
            const result = await journal.recordExternalObservation({ ...observation, committedAt: time().instant });
            return result === 'committed' || result === 'duplicate';
        }
        catch {
            return false;
        }
    }
    async function runProviderLifecycle(event, request) {
        if (!plain(request) || !fields(request, ['action', 'target', 'destination', 'requestId']))
            return { status: 'rejected' };
        const { action, destination, requestId, target } = request;
        if (!['erase', 'restrict', 'recover'].includes(action) || !token(destination) || !token(requestId) || !plain(target) || !fields(target, ['tenantId', 'source', 'id']) || !token(target.tenantId) || !token(target.source) || !token(target.id))
            return { status: 'rejected' };
        const q = { tenantId: target.tenantId, source: target.source, id: target.id };
        if (!options.providerLifecycle || busy || lifecycleBusy || closing || closed)
            return { status: 'unavailable' };
        beginLifecycle();
        try {
            const caller = await lifecycleCaller(event, action, { ...q, requestId, destination });
            if (!caller || (await options.decide(caller, 'logging.provider.' + action, Object.freeze({ ...q, requestId, destination }))).allowed !== true)
                return { status: 'rejected' };
            if (destination !== 'local' && !external.has(destination))
                return { status: 'rejected' };
            const approval = await approve(action, caller, requestId, q, destination);
            if (approval.status !== 'approved')
                return { status: approval.status === 'blocked' ? 'blocked' : 'unavailable' };
            if ((action === 'erase' || action === 'recover') && approval.preservation !== 'clear')
                return { status: approval.preservation === 'held' ? 'blocked' : 'unavailable' };
            const current = await lifecycleCaller(event, action, { ...q, requestId, destination });
            if (!current || current.subjectId !== caller.subjectId || (await options.decide(current, 'logging.provider.' + action, Object.freeze({ ...q, requestId, destination }))).allowed !== true)
                return { status: 'rejected' };
            if (closed || closing)
                return { status: 'unavailable' };
            if (!journal?.recordExternalObservation || !journal.reserveExternal)
                return { status: 'unavailable' };
            if (action === 'recover' && await journal.visibility(q) !== 'visible')
                return { status: 'blocked' };
            if (closed || closing)
                return { status: 'unavailable' };
            const reservation = await reserveExternal({ kind: 'provider', tenantId: q.tenantId, source: q.source, recordId: q.id, action, destination, requestId, subjectId: caller.subjectId });
            if (reservation !== 'reserved')
                return { status: reservation };
            if (closed || closing)
                return { status: 'unavailable' };
            const controller = new AbortController();
            lifecycleAbort = controller;
            let timer;
            try {
                const result = await Promise.race([options.providerLifecycle({ action, target: q, destination, requestId, subjectId: caller.subjectId, evidenceRef: approval.evidenceRef }, controller.signal), new Promise(resolve => { timer = setTimeout(() => { controller.abort(); resolve({ status: 'uncertain' }); }, timeout); })]);
                if (result?.status === 'confirmed' && token(result.evidenceRef)) {
                    const observed = await observeExternal({ kind: 'provider', tenantId: q.tenantId, source: q.source, recordId: q.id, action, destination, requestId, subjectId: caller.subjectId, evidenceRef: result.evidenceRef });
                    return observed ? { status: 'confirmed', evidenceRef: result.evidenceRef } : { status: 'uncertain' };
                }
                if (result && (result.status === 'blocked' || result.status === 'rejected' || result.status === 'unavailable' || result.status === 'uncertain'))
                    return { status: result.status };
                return { status: 'uncertain' };
            }
            catch {
                return { status: 'uncertain' };
            }
            finally {
                if (timer !== undefined)
                    clearTimeout(timer);
            }
        }
        catch {
            return { status: 'unavailable' };
        }
        finally {
            endLifecycle();
        }
    }
    async function changePolicy(event, input) {
        if (!plain(input) || !fields(input, ['tenantId', 'source', 'expectedRevision', 'policy', 'requestId']) || !token(input.tenantId) || !token(input.source) || !token(input.expectedRevision) || !token(input.requestId) || !plain(input.policy) || !fields(input.policy, ['revision', 'rules']) || !token(input.policy.revision) || input.policy.revision === input.expectedRevision)
            return { status: 'rejected' };
        if (!options.commitPolicy || !options.resolvePolicy || (journal && (!journal.recordExternalObservation || !journal.reserveExternal)) || busy || lifecycleBusy || closing || closed)
            return { status: 'unavailable' };
        const { tenantId, source, expectedRevision, requestId } = input;
        let change;
        try {
            change = { tenantId, source, expectedRevision, requestId, policy: { revision: input.policy.revision, rules: effectiveRules(floors, input.policy.rules, source, new Set(external.keys())) } };
        }
        catch {
            return { status: 'rejected' };
        }
        beginLifecycle();
        try {
            const caller = await lifecycleCaller(event, 'policy.change', { tenantId, source, requestId, expectedRevision, revision: change.policy.revision });
            if (!caller)
                return { status: 'rejected' };
            const approval = await approve('policy.change', caller, requestId, change);
            if (approval.status !== 'approved')
                return { status: approval.status === 'blocked' ? 'blocked' : 'unavailable' };
            const current = await lifecycleCaller(event, 'policy.change', { tenantId, source, requestId, expectedRevision, revision: change.policy.revision });
            if (!current || current.subjectId !== caller.subjectId)
                return { status: 'rejected' };
            if ((await policy(tenantId, source))?.revision !== expectedRevision)
                return { status: 'conflict' };
            if (closed || closing)
                return { status: 'unavailable' };
            if (journal) {
                const reservation = await reserveExternal({ kind: 'policy', tenantId, source, requestId, subjectId: caller.subjectId, expectedRevision, revision: change.policy.revision });
                if (reservation !== 'reserved')
                    return { status: reservation === 'rejected' ? 'conflict' : reservation };
            }
            if (closed || closing)
                return { status: 'unavailable' };
            try {
                const result = await options.commitPolicy(change, current, approval.evidenceRef);
                if (result?.status === 'committed' && result.revision === change.policy.revision && token(result.evidenceRef)) {
                    const observed = await observeExternal({ kind: 'policy', tenantId, source, requestId, subjectId: caller.subjectId, expectedRevision, revision: result.revision, evidenceRef: result.evidenceRef });
                    return observed ? { status: 'committed', revision: result.revision, evidenceRef: result.evidenceRef } : { status: 'uncertain' };
                }
                if (result && (result.status === 'rejected' || result.status === 'blocked' || result.status === 'conflict' || result.status === 'unavailable' || result.status === 'uncertain'))
                    return { status: result.status };
                return { status: 'uncertain' };
            }
            catch {
                return { status: 'uncertain' };
            }
        }
        catch {
            return { status: 'unavailable' };
        }
        finally {
            endLifecycle();
        }
    }
    // Manual recovery is single-record, single-destination and requires durable reservation.
    // Reconciliation asks for evidence only; it never invokes a sink write.
    async function recoverDelivery(event, input) {
        if (!plain(input) || !fields(input, ['target', 'destination', 'requestId', 'mode']) ||
            !plain(input.target) || !fields(input.target, ['tenantId', 'source', 'id']) ||
            ![input.target.tenantId, input.target.source, input.target.id, input.destination, input.requestId].every(token) ||
            !['retry', 'reconcile'].includes(input.mode))
            return { status: 'rejected' };
        const request = structuredClone(input);
        const { target, destination, requestId, mode } = request;
        const action = mode === 'retry' ? 'delivery.retry' : 'delivery.reconcile';
        const resource = { ...target, destination, requestId };
        if (!store?.reserveDeliveryRecovery || !store.finishDeliveryRecovery || busy || lifecycleBusy || closing || closed)
            return { status: 'unavailable' };
        if (destination !== 'local' && !external.has(destination))
            return { status: 'rejected' };
        if (mode === 'reconcile' && !options.reconcileDelivery)
            return { status: 'unavailable' };
        beginLifecycle();
        try {
            const caller = await lifecycleCaller(event, action, resource);
            if (!caller)
                return { status: 'rejected' };
            const approval = await approve(action, caller, requestId, target, destination);
            if (approval.status !== 'approved')
                return { status: approval.status === 'blocked' ? 'blocked' : 'unavailable' };
            if (approval.preservation !== 'clear')
                return { status: approval.preservation === 'held' ? 'blocked' : 'unavailable' };
            const current = await lifecycleCaller(event, action, resource);
            if (!current || current.subjectId !== caller.subjectId)
                return { status: 'rejected' };
            const stamp = time();
            if (stamp.quality !== 'verified' || closing || closed)
                return { status: 'unavailable' };
            const record = (await store.load(target.tenantId, target.source, 10000)).find(x => x.record.tenantId === target.tenantId && x.record.source === target.source && x.record.id === target.id);
            if (!record || stamp.ms - Date.parse(record.record.receiptTime) >= retentionFor(record.record) || await store.visibility(target) !== 'visible')
                return { status: 'blocked' };
            const previous = record.delivery.find(d => d.target === destination);
            if (!previous)
                return { status: 'blocked' };
            if (mode === 'retry') {
                // Never retry a skipped or uncertain attempt. Recheck current routing and classification.
                if (previous.status !== 'exhausted' || previous.attempts < 1 || previous.attempts >= 3 || quarantined.has(destination))
                    return { status: 'blocked' };
                const p = await policy(target.tenantId, target.source);
                const rule = p?.rules.find(r => r.source === target.source && r.code === record.record.code);
                if (!rule || p?.revision !== record.record.policyRevision || rule.purpose !== record.record.purpose ||
                    (destination !== 'local' && !rule.destinations?.includes(destination)))
                    return { status: 'blocked' };
            }
            else if (previous.status !== 'uncertain')
                return { status: 'blocked' };
            const finalCaller = await lifecycleCaller(event, action, resource);
            if (!finalCaller || finalCaller.subjectId !== caller.subjectId || closing || closed)
                return { status: 'rejected' };
            const reserved = await store.reserveDeliveryRecovery(request, previous, caller.subjectId, approval.evidenceRef, stamp.instant);
            if (reserved !== 'reserved')
                return { status: reserved === 'full' ? 'unavailable' : reserved };
            if (closing || closed)
                return { status: 'uncertain' };
            const controller = new AbortController();
            lifecycleAbort = controller;
            let outcome = { status: 'uncertain' };
            let timer;
            try {
                if (mode === 'retry') {
                    const sink = destination === 'local' ? localSink : external.get(destination).sink;
                    const result = await deliver(sink, record.record, timeout, 1, controller.signal);
                    if (result.outcome === 'delivered' || result.outcome === 'refused')
                        outcome = { status: result.outcome, evidenceRef: approval.evidenceRef };
                }
                else {
                    const result = await Promise.race([options.reconcileDelivery(Object.freeze({ ...request, target: Object.freeze({ ...target }) }), controller.signal), new Promise(resolve => { timer = setTimeout(() => { controller.abort(); resolve({ status: 'uncertain' }); }, timeout); })]);
                    if ((result?.status === 'delivered' || result?.status === 'refused') && token(result.evidenceRef))
                        outcome = { status: result.status, evidenceRef: result.evidenceRef };
                }
            }
            catch { }
            finally {
                if (timer !== undefined)
                    clearTimeout(timer);
            }
            if (mode === 'retry' && outcome.status === 'uncertain')
                quarantined.add(destination);
            const finished = await store.finishDeliveryRecovery(request, outcome, time().instant);
            if (finished !== 'committed')
                return { status: 'uncertain' };
            if (mode === 'retry')
                await observeSink(record.record, destination, outcome.status === 'delivered' ? 'ready' : 'degraded');
            return { status: outcome.status };
        }
        catch {
            return { status: 'uncertain' };
        }
        finally {
            endLifecycle();
        }
    }
    async function readPipelineHealth(event, query) {
        const q = snapshotQuery(query);
        if (!q || !validQuery(q) || !await authorised(event, 'logging.health.read', q))
            return { status: 'rejected' };
        try {
            const generation = lifecycleGeneration;
            if (lifecycleBusy)
                throw new Error('LOG_READ_INVALIDATED');
            const observed = time();
            expire(observed.ms);
            const entries = await visibleEntries(q);
            const delivery = entries.flatMap(x => x.delivery);
            const withoutDelivery = entries.filter(x => x.delivery.length === 0).length;
            const backlog = entries.filter(x => x.delivery.length === 0 || x.delivery.some(d => d.status !== 'delivered'));
            const oldestBacklogAt = backlog.length ? backlog.map(x => x.record.receiptTime).sort((a, b) => Date.parse(a) - Date.parse(b))[0] : null;
            const oldestBacklogAgeMs = observed.quality === 'verified' && oldestBacklogAt && Date.parse(oldestBacklogAt) <= observed.ms ? observed.ms - Date.parse(oldestBacklogAt) : null;
            const severityDistribution = { debug: 0, info: 0, warn: 0, error: 0, fatal: 0 };
            for (const entry of entries)
                severityDistribution[entry.record.severity]++;
            const destinations = [...new Set(delivery.map(d => d.target))].sort().map(target => {
                const states = delivery.filter(d => d.target === target);
                const last = states.reduce((a, b) => Date.parse(b.time) >= Date.parse(a.time) ? b : a);
                return { target, state: last.status === 'delivered' ? 'ready' : 'degraded', observedAt: last.time, exhausted: states.filter(d => d.status === 'exhausted').length, uncertain: states.filter(d => d.status === 'uncertain').length };
            });
            // Scope-wide counters must not bypass a resource-filtered read permission.
            const scopeOnly = { tenantId: q.tenantId, source: q.source };
            const canReadScope = await authorised(event, 'logging.health.scope.read', scopeOnly);
            await checkRead(event, 'logging.health.read', q, generation);
            const scoped = canReadScope ? scopedIntake.get(JSON.stringify([q.tenantId, q.source])) : undefined;
            return { status: 'completed', observedAt: observed.instant, timeQuality: observed.quality, storage: store ? 'durable' : 'process-local', withoutDelivery, backlogRecords: backlog.length, oldestBacklogAt, oldestBacklogAgeMs, severityDistribution, destinations, scopeIntake: scoped ? { ...scoped } : null, scopeIntakeCoverage: canReadScope ? (scopeCoverageLimited ? 'bounded-process-lifetime; some scopes omitted' : 'process-lifetime; excludes unauthorised and busy intake') : 'not-authorised', records: entries.length, delivered: delivery.filter(x => x.status === 'delivered').length, exhausted: delivery.filter(x => x.status === 'exhausted').length, uncertain: delivery.filter(x => x.status === 'uncertain').length, coverage: 'retained scoped records only; excludes intake losses, restricted and expired records; pagination ignored' };
        }
        catch {
            return { status: 'unavailable' };
        }
    }
    // Deployment-wide durable metadata is deliberately not exposed through tenant HTTP queries.
    async function journalHealth() {
        if (!journal?.health)
            return { status: 'unavailable' };
        try {
            const value = await journal.health();
            const observed = time();
            const counts = [value.requests, value.markers, value.events, value.reservations, value.pending];
            if (!Number.isSafeInteger(value.capacity) || value.capacity < 1 || counts.some(n => !Number.isSafeInteger(n) || n < 0) || value.pending > value.events || value.reservations > value.requests || (value.oldestPendingAt !== null && !instant(value.oldestPendingAt)))
                return { status: 'unavailable' };
            const oldestPendingAgeMs = observed.quality === 'verified' && value.oldestPendingAt && Date.parse(value.oldestPendingAt) <= observed.ms ? observed.ms - Date.parse(value.oldestPendingAt) : null;
            return { status: 'completed', capacity: value.capacity, requests: value.requests, markers: value.markers, events: value.events, reservations: value.reservations, pending: value.pending, oldestPendingAt: value.oldestPendingAt, oldestPendingAgeMs, observedAt: observed.instant, timeQuality: observed.quality };
        }
        catch {
            return { status: 'unavailable' };
        }
    }
    // Composition-root-only relay. Acknowledgement is after publisher confirmation; duplicates are possible.
    async function relayLifecycle(publish, limit = 20) {
        if (!journal || relayBusy || lifecycleBusy || closing || closed || !bounded(limit, 100))
            return { status: 'unavailable', published: 0 };
        beginRelay();
        let published = 0;
        try {
            const events = await journal.pending(limit);
            if (!Array.isArray(events) || events.length > limit)
                return { status: 'unavailable', published };
            for (const event of events) {
                if (closing || closed)
                    return { status: 'unavailable', published };
                const controller = new AbortController();
                relayAbort = controller;
                let timer;
                try {
                    const result = await Promise.race([Promise.resolve().then(() => publish(event, controller.signal)), new Promise(resolve => { timer = setTimeout(() => { controller.abort(); resolve('uncertain'); }, timeout); })]);
                    if (result !== 'delivered')
                        return { status: result === 'refused' ? 'unavailable' : 'uncertain', published };
                    const stamp = time();
                    await journal.acknowledge(event.id, stamp.quality === 'verified' ? stamp.instant : undefined);
                    published++;
                }
                catch {
                    return { status: 'uncertain', published };
                }
                finally {
                    if (timer !== undefined)
                        clearTimeout(timer);
                }
            }
            return { status: 'completed', published };
        }
        catch {
            return { status: 'unavailable', published };
        }
        finally {
            endRelay();
        }
    }
    // Explicit maintenance, restricted to composition root; unknown preservation never deletes.
    async function maintainRetention(limit = 20) {
        if (!store || !options.retentionAuthority || busy || lifecycleBusy || closing || closed || !bounded(limit, 100))
            return { status: 'unavailable', removed: 0, held: 0 };
        beginLifecycle();
        let removed = 0, held = 0;
        try {
            const t = time();
            if (t.quality !== 'verified')
                return { status: 'unavailable', removed, held };
            const before = t.instant;
            const targets = await store.expired(before, limit, retentionCursor);
            if (targets.length === 0) {
                retentionCursor = undefined;
                return { status: 'completed', removed, held };
            }
            if (targets.length > limit)
                return { status: 'unavailable', removed, held };
            for (const target of targets) {
                if (closing || closed)
                    return { status: 'unavailable', removed, held };
                retentionCursor = target;
                const record = await store.retentionRecord(target);
                if (!record || record.tenantId !== target.tenantId || record.source !== target.source || record.id !== target.id)
                    continue;
                const cutoff = new Date(t.ms - retentionFor(record)).toISOString();
                if (Date.parse(record.receiptTime) > Date.parse(cutoff))
                    continue;
                const decision = await options.retentionAuthority.resolvePreservation(target);
                if (decision !== 'clear') {
                    held++;
                    continue;
                }
                if (closing || closed)
                    return { status: 'unavailable', removed, held };
                if (await store.expire(target, cutoff, t.instant) === 'removed')
                    removed++;
            }
            if (targets.length < limit)
                retentionCursor = undefined;
            return { status: 'completed', removed, held };
        }
        catch {
            return { status: 'uncertain', removed, held };
        }
        finally {
            endLifecycle();
        }
    }
    // Only acknowledged event payloads are eligible; request identities and tombstones are never pruned here.
    async function maintainJournal(limit = 20) {
        const maintenance = journal?.maintenance;
        if (!maintenance || !journalRetention || relayBusy || busy || lifecycleBusy || closing || closed || !bounded(limit, 100))
            return { status: 'unavailable', removed: 0, held: 0 };
        beginLifecycle();
        let removed = 0, held = 0;
        try {
            const t = time();
            if (t.quality !== 'verified')
                return { status: 'unavailable', removed, held };
            const before = new Date(t.ms - journalRetention.retentionMs).toISOString();
            const events = await maintenance.acknowledgedBefore(before, limit, journalCursor);
            if (!Array.isArray(events) || events.length > limit)
                return { status: 'unavailable', removed, held };
            for (const event of events) {
                if (closing || closed)
                    return { status: 'unavailable', removed, held };
                if (!token(event.id) || !instant(event.committedAt))
                    return { status: 'unavailable', removed, held };
                journalCursor = event.id;
                if (await journalRetention.resolvePreservation(event) !== 'clear') {
                    held++;
                    continue;
                }
                if (closing || closed)
                    return { status: 'unavailable', removed, held };
                if (await maintenance.removeAcknowledged(event.id, before) === 'removed')
                    removed++;
            }
            if (events.length < limit)
                journalCursor = undefined;
            return { status: 'completed', removed, held };
        }
        catch {
            return { status: 'uncertain', removed, held };
        }
        finally {
            endLifecycle();
        }
    }
    // Host authority must establish expiry of replay rights and preservation obligations.
    async function maintainRetirement(limit = 20) {
        if (!store || !journal?.retirementCandidates || !journal.retire || !options.retirementAuthority || relayBusy || busy || lifecycleBusy || closing || closed || !bounded(limit, 100))
            return { status: 'unavailable', removed: 0, held: 0 };
        beginLifecycle();
        let removed = 0, held = 0;
        try {
            if (time().quality !== 'verified')
                return { status: 'unavailable', removed, held };
            const candidates = await journal.retirementCandidates(limit, retirementCursor);
            if (!Array.isArray(candidates) || candidates.length > limit)
                return { status: 'unavailable', removed, held };
            for (const candidate of candidates) {
                if (closing || closed)
                    return { status: 'unavailable', removed, held };
                if (!['request', 'marker'].includes(candidate.kind) || ![candidate.tenantId, candidate.source, candidate.id, candidate.version].every(token))
                    return { status: 'unavailable', removed, held };
                const snapshot = Object.freeze({ ...candidate });
                retirementCursor = snapshot;
                if (await options.retirementAuthority.authorise(snapshot) !== 'clear') {
                    held++;
                    continue;
                }
                if (closing || closed || time().quality !== 'verified')
                    return { status: 'unavailable', removed, held };
                if (await journal.retire(snapshot) === 'removed')
                    removed++;
                else
                    held++;
            }
            if (candidates.length < limit)
                retirementCursor = undefined;
            return { status: 'completed', removed, held };
        }
        catch {
            return { status: 'uncertain', removed, held };
        }
        finally {
            endLifecycle();
        }
    }
    function close() {
        closing = true;
        closed = true;
        activeAbort?.abort();
        lifecycleAbort?.abort();
        relayAbort?.abort();
        rows.length = 0;
        buckets.clear();
    }
    async function shutdown() {
        close();
        const pending = [active, lifecycleDone, relayDone].filter(p => p !== undefined);
        if (!pending.length)
            return { status: 'completed' };
        // The host must still bound its caller/policy provider latency.
        let timer;
        try {
            return await Promise.race([Promise.allSettled(pending).then(() => ({ status: 'completed' })), new Promise(resolve => { timer = setTimeout(() => resolve({ status: 'uncertain' }), timeout); })]);
        }
        finally {
            if (timer !== undefined)
                clearTimeout(timer);
        }
    }
    return { recoverDelivery, maintainRetirement, journalHealth, runProviderLifecycle, changePolicy, maintainJournal, maintainRetention, relayLifecycle, submit, readDiagnostics, readDeliveryState, readEffectivePolicy, readPipelineHealth, exportDiagnostics, eraseDiagnostic: (event, target, requestId) => changeLocalRecord(event, 'erase', target, requestId), restrictDiagnostic: (event, target, requestId) => changeLocalRecord(event, 'restrict', target, requestId), hostHealth: () => ({ state: closed || quarantined.size ? 'degraded' : (busy || lifecycleBusy || relayBusy) ? 'busy' : 'unknown', lastDelivery, drops, uncertain, sinkObservationFailures, buffered: rows.length, intake: { ...intake }, activeIntake: busy ? 1 : 0, activeAgeMs: activeSince === null ? null : Math.max(0, performance.now() - activeSince), retainedProcessLocal: rows.length, storage: store ? 'durable' : 'process-local', quarantined: [...quarantined] }), close, shutdown };
}
