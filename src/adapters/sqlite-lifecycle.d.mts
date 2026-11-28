import type { DiagnosticStore } from '../contracts/types.ts'
export declare function createSqliteLifecycleJournal(path:string):DiagnosticStore & {close():void}
