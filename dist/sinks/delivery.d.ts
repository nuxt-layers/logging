import type { Sink, RecordEntry, Delivery } from '../contracts/types.ts';
export declare function deliver(sink: Sink, record: RecordEntry, timeout: number, maxAttempts: number, parent: AbortSignal): Promise<{
    outcome: Delivery;
    attempts: number;
    time: string;
}>;
