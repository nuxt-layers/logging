// Retry only an explicit refusal (provider guarantees no write); uncertainty is never replayed.
export async function deliver(sink, record, timeout, maxAttempts, parent) {
    let outcome = 'refused', attempts = 0;
    while (attempts < maxAttempts && !parent.aborted) {
        attempts++;
        const controller = new AbortController();
        const abort = () => controller.abort();
        parent.addEventListener('abort', abort, { once: true });
        let timer;
        try {
            const result = await Promise.race([Promise.resolve().then(() => controller.signal.aborted ? 'refused' : sink.write(record, controller.signal)), new Promise(resolve => { timer = setTimeout(() => { controller.abort(); resolve('uncertain'); }, timeout); })]);
            outcome = result === 'delivered' || result === 'refused' ? result : 'uncertain';
        }
        catch {
            outcome = 'uncertain';
        }
        finally {
            if (timer !== undefined)
                clearTimeout(timer);
            parent.removeEventListener('abort', abort);
        }
        if (outcome !== 'refused')
            break;
    }
    return { outcome, attempts, time: new Date().toISOString() };
}
