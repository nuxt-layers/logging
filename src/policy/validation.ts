import type { Rule, Diagnostic, Severity } from '../contracts/types.ts'
export const token=(v:unknown):v is string=>typeof v==='string'&&/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}$/.test(v)
export const instant=(v:unknown):v is string=>{
  if(typeof v!=='string'||!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,3})?Z$/.test(v)||!Number.isFinite(Date.parse(v)))return false
  return new Date(v).toISOString().slice(0,19)===v.slice(0,19)
}
export const levels:readonly Severity[]=['debug','info','warn','error','fatal']
export const bounded=(v:number,max:number)=>Number.isSafeInteger(v)&&v>0&&v<=max
const forbidden=/password|secret|token|credential|session|payment|key|email|phone|address|__proto__|constructor|prototype/i
const key=(r:Rule)=>JSON.stringify([r.source,r.code])
export function compileRules(input:readonly Rule[],targets:Set<string>) {
  if(!Array.isArray(input)||input.length>10000)throw new Error('LOG_INVALID_RULE')
  const result=new Map<string,Rule>()
  for(const r of input) {
    if(!r||!token(r.source)||!token(r.code)||!token(r.purpose)||!Array.isArray(r.attributes)||r.attributes.length>16||r.attributes.some(k=>!token(k)||forbidden.test(k))||new Set(r.attributes).size!==r.attributes.length||(r.category!==undefined&&!token(r.category))||(r.minimumSeverity!==undefined&&!levels.includes(r.minimumSeverity))||!bounded(r.sampleEvery??1,10000)||!Array.isArray(r.destinations??[])||(r.destinations??[]).length>8||(r.destinations??[]).some(t=>!targets.has(t))||new Set(r.destinations??[]).size!==(r.destinations??[]).length)throw new Error('LOG_INVALID_RULE')
    if(result.has(key(r)))throw new Error('LOG_DUPLICATE_RULE')
    result.set(key(r),Object.freeze({source:r.source,code:r.code,purpose:r.purpose,category:r.category??'technical',minimumSeverity:r.minimumSeverity??'debug',sampleEvery:r.sampleEvery??1,attributes:Object.freeze([...r.attributes]),destinations:Object.freeze([...(r.destinations??[])])}))
  }
  return result
}
// Deployment rules are immutable maxima; configuration may only narrow them.
export function effectiveRules(floors:Map<string,Rule>,input:readonly Rule[],source:string,targets:Set<string>) {
  const rules=compileRules(input,targets)
  for(const [k,r] of rules) {
    const f=floors.get(k)
    if(!f||r.source!==source||r.purpose!==f.purpose||r.category!==f.category||r.attributes.some(a=>!f.attributes.includes(a))||(r.destinations??[]).some(t=>!f.destinations?.includes(t))||levels.indexOf(r.minimumSeverity??'debug')<levels.indexOf(f.minimumSeverity??'debug')||(r.sampleEvery??1)<(f.sampleEvery??1)||(r.sampleEvery??1)%(f.sampleEvery??1)!==0)throw new Error('LOG_POLICY_FLOOR')
  }
  return [...rules.values()]
}
export function plain(v:unknown):v is Record<string,unknown> {
  return !!v&&typeof v==='object'&&(Object.getPrototypeOf(v)===Object.prototype||Object.getPrototypeOf(v)===null)
}
export function fields(v:Record<string,unknown>,allow:readonly string[]) {
  const keys=Reflect.ownKeys(v)
  return keys.length<=allow.length&&keys.every(k=>typeof k==='string'&&allow.includes(k)&&Object.hasOwn(Object.getOwnPropertyDescriptor(v,k)??{},'value'))
}
export function diagnosticCode(input:unknown):string|null {
  if(!plain(input)||!fields(input,['version','code','severity','occurrenceTime','correlation','attributes'])||!token(input.code))return null
  return input.code
}
export function validateDiagnostic(input:unknown,rule:Rule):Diagnostic|null {
  if(!plain(input)||!fields(input,['version','code','severity','occurrenceTime','correlation','attributes'])||input.version!==1||!token(input.code)||!levels.includes(input.severity as Severity)||!instant(input.occurrenceTime)||(input.correlation!==undefined&&!token(input.correlation)))return null
  const attrs:Record<string,number|boolean>=Object.create(null)
  if(input.attributes!==undefined) {
    if(!plain(input.attributes)||!fields(input.attributes,rule.attributes))return null
    for(const k of Object.keys(input.attributes)) {
      const v=input.attributes[k]
      if(typeof v!=='boolean'&&!(typeof v==='number'&&Number.isFinite(v)&&Math.abs(v)<=1e12))return null
      attrs[k]=v
    }
  }
  return {version:1,code:input.code,severity:input.severity as Severity,occurrenceTime:input.occurrenceTime,correlation:input.correlation as string|undefined,attributes:attrs}
}
