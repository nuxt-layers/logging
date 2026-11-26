import type { LifecycleJournal } from '../contracts/types.ts'
export declare function createSqliteLifecycleJournal(path:string):LifecycleJournal & {close():void}
