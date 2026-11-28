import type { Options, Delivery, Diagnostic, Receipt, DiagnosticQuery, Policy, LifecycleTarget, LifecycleResult, ExportResult, LifecyclePublisher, ProviderLifecycleRequest, ProviderLifecycleResult, PolicyChange, PolicyChangeResult } from '../contracts/types.ts';
export declare function createLogging<E>(options: Options<E>): {
    maintainRetirement: (limit?: number) => Promise<{
        status: "unavailable";
        removed: number;
        held: number;
    } | {
        status: "completed";
        removed: number;
        held: number;
    } | {
        status: "uncertain";
        removed: number;
        held: number;
    }>;
    journalHealth: () => Promise<{
        status: "unavailable";
        capacity?: undefined;
        requests?: undefined;
        markers?: undefined;
        events?: undefined;
        reservations?: undefined;
        pending?: undefined;
        oldestPendingAt?: undefined;
        oldestPendingAgeMs?: undefined;
        observedAt?: undefined;
        timeQuality?: undefined;
    } | {
        status: "completed";
        capacity: number;
        requests: number;
        markers: number;
        events: number;
        reservations: number;
        pending: number;
        oldestPendingAt: string | null;
        oldestPendingAgeMs: number | null;
        observedAt: string;
        timeQuality: "verified" | "degraded" | "unknown";
    }>;
    runProviderLifecycle: (event: E, request: Pick<ProviderLifecycleRequest, "action" | "target" | "destination" | "requestId">) => Promise<ProviderLifecycleResult>;
    changePolicy: (event: E, input: PolicyChange) => Promise<PolicyChangeResult>;
    maintainJournal: (limit?: number) => Promise<{
        status: "unavailable";
        removed: number;
        held: number;
    } | {
        status: "completed";
        removed: number;
        held: number;
    } | {
        status: "uncertain";
        removed: number;
        held: number;
    }>;
    maintainRetention: (limit?: number) => Promise<{
        status: "unavailable";
        removed: number;
        held: number;
    } | {
        status: "completed";
        removed: number;
        held: number;
    } | {
        status: "uncertain";
        removed: number;
        held: number;
    }>;
    relayLifecycle: (publish: LifecyclePublisher, limit?: number) => Promise<{
        status: "uncertain" | "unavailable";
        published: number;
    } | {
        status: "completed";
        published: number;
    }>;
    submit: (event: E, input: Diagnostic) => Promise<Receipt>;
    readDiagnostics: (event: E, query: DiagnosticQuery) => Promise<{
        status: "rejected";
        records: never[];
        coverage?: undefined;
    } | {
        status: "completed";
        records: Readonly<{
            id: string;
            tenantId: string;
            source: string;
            code: string;
            severity: import("../contracts/types.ts").Severity;
            occurrenceTime: string;
            receiptTime: string;
            timeQuality: "verified" | "degraded" | "unknown";
            correlation?: string;
            attributes: Readonly<Record<string, number | boolean>>;
            policyRevision: string;
            purpose: string;
            category: string;
            classification: "technical";
            retentionMs?: number;
        }>[];
        coverage: string;
    } | {
        status: "unavailable";
        records: never[];
        coverage?: undefined;
    }>;
    readDeliveryState: (event: E, query: DiagnosticQuery) => Promise<{
        status: "rejected";
        records: never[];
        coverage?: undefined;
    } | {
        status: "completed";
        records: {
            id: string;
            delivery: {
                target: string;
                status: "delivered" | "exhausted" | "uncertain";
                attempts: number;
                time: string;
            }[];
        }[];
        coverage: string;
    } | {
        status: "unavailable";
        records: never[];
        coverage?: undefined;
    }>;
    readEffectivePolicy: (event: E, scope: {
        tenantId: string;
        source: string;
    }) => Promise<{
        status: "rejected";
        policy?: undefined;
    } | {
        status: "completed";
        policy: Policy;
    } | {
        status: "unavailable";
        policy?: undefined;
    }>;
    readPipelineHealth: (event: E, query: DiagnosticQuery) => Promise<{
        status: "rejected";
        observedAt?: undefined;
        timeQuality?: undefined;
        storage?: undefined;
        withoutDelivery?: undefined;
        severityDistribution?: undefined;
        records?: undefined;
        delivered?: undefined;
        exhausted?: undefined;
        uncertain?: undefined;
        coverage?: undefined;
    } | {
        status: "completed";
        observedAt: string;
        timeQuality: "verified" | "degraded" | "unknown";
        storage: "durable" | "process-local";
        withoutDelivery: number;
        severityDistribution: {
            debug: number;
            info: number;
            warn: number;
            error: number;
            fatal: number;
        };
        records: number;
        delivered: number;
        exhausted: number;
        uncertain: number;
        coverage: "retained scoped records only; excludes intake losses, restricted and expired records; pagination ignored";
    } | {
        status: "unavailable";
        observedAt?: undefined;
        timeQuality?: undefined;
        storage?: undefined;
        withoutDelivery?: undefined;
        severityDistribution?: undefined;
        records?: undefined;
        delivered?: undefined;
        exhausted?: undefined;
        uncertain?: undefined;
        coverage?: undefined;
    }>;
    exportDiagnostics: (event: E, query: DiagnosticQuery, requestId: string) => Promise<ExportResult>;
    eraseDiagnostic: (event: E, target: LifecycleTarget, requestId: string) => Promise<LifecycleResult>;
    restrictDiagnostic: (event: E, target: LifecycleTarget, requestId: string) => Promise<LifecycleResult>;
    hostHealth: () => {
        state: "degraded" | "unknown" | "busy";
        lastDelivery: "unknown" | Delivery;
        drops: number;
        uncertain: number;
        sinkObservationFailures: number;
        buffered: number;
        intake: {
            accepted: number;
            rejected: number;
            throttled: number;
            unavailable: number;
            uncertain: number;
        };
        activeIntake: number;
        activeAgeMs: number | null;
        retainedProcessLocal: number;
        storage: "durable" | "process-local";
        quarantined: string[];
    };
    close: () => void;
    shutdown: () => Promise<{
        status: "uncertain";
    } | {
        status: "completed";
    }>;
};
