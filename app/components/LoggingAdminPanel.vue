<script setup lang="ts">
import { computed, ref, watch } from 'vue'
const props=defineProps<{section:'overview'|'records'|'detail'|'delivery'|'settings'|'data'}>()
const route=useRoute()
const { t }=useI18n()
const tenantId=ref(''), source=ref(''), from=ref(''), to=ref(''), recordId=ref('')
const offset=ref(0), busy=ref(false), result=ref<unknown>(null), notice=ref('')
const requestId=ref(''), destination=ref(''), action=ref('restrict'), policy=ref('')
const confirmed=ref(false)
const severity=ref(''), code=ref(''), correlation=ref('')
let generation=0
const intent=ref('')
const routes=[['overview','/logging'],['records','/logging/records'],['delivery','/logging/delivery'],['settings','/logging/settings'],['data','/logging/data-management']]
const operation=computed(()=>({overview:'health',records:'records',detail:'records',delivery:'delivery',settings:'policy',data:'records'}[props.section]))
const detailId=computed(()=>props.section==='detail'?String(route.params.id??''):recordId.value)
const outcome=computed(()=>{const r=result.value as {status?:string}|null;return typeof r?.status==='string'&&['completed','completed-local','committed','confirmed','blocked','rejected','unavailable','uncertain','conflict','delivered','refused'].includes(r.status)?r.status:'unknown'})
const health=computed(()=>{const r=result.value as {records?:number;delivered?:number;exhausted?:number;uncertain?:number;withoutDelivery?:number;backlogRecords?:number;coverage?:string}|null;return props.section==='overview'&&r&&typeof r.records==='number'?r:null})
const rows=computed(()=>{const r=result.value as {records?:unknown[]}|null;return Array.isArray(r?.records)?r.records:[]})
function query(){
 return {tenantId:tenantId.value,source:source.value,from:new Date(from.value).toISOString(),to:new Date(to.value).toISOString(),limit:20,offset:offset.value,...(detailId.value?{id:detailId.value}:{}),...(severity.value?{severity:severity.value}:{}),...(code.value?{code:code.value}:{}),...(correlation.value?{correlation:correlation.value}:{})}
}
async function call(op:string=operation.value){
 if(busy.value)return
 const requestGeneration=++generation
 result.value=null;notice.value='';busy.value=true
 try {
  const target={tenantId:tenantId.value,source:source.value,id:detailId.value}
  const mutating=['erase','restrict','provider','policy.change','export','delivery.retry','delivery.reconcile'].includes(op)
  if(mutating&&(!confirmed.value||intent.value!==op)){notice.value=t('logging.confirmRequired');return}
  let input:unknown
  if(op==='policy')input={tenantId:tenantId.value,source:source.value}
  else if(op==='erase'||op==='restrict')input=target
  else if(op==='delivery.retry'||op==='delivery.reconcile')input={target,destination:destination.value,requestId:requestId.value,mode:op==='delivery.retry'?'retry':'reconcile'}
  else if(op==='provider')input={action:action.value,target,destination:destination.value,requestId:requestId.value}
  else if(op==='policy.change')input={...JSON.parse(policy.value),tenantId:tenantId.value,source:source.value,requestId:requestId.value}
  else input=query()
  confirmed.value=false
  const response=await $fetch('/api/logging/operation',{method:'POST',headers:{'x-logging-request':'1'},body:{operation:op,input,requestId:requestId.value}})
  if(requestGeneration!==generation)return
  result.value=response
  notice.value=t('logging.outcome.'+outcome.value)
 } catch {if(requestGeneration===generation)notice.value=t('logging.failed')}
 finally {busy.value=false;confirmed.value=false}
}
function clear(){generation++;result.value=null;notice.value='';offset.value=0;confirmed.value=false}
watch([tenantId,source,from,to,detailId,severity,code,correlation,requestId,destination,action,policy,intent,()=>route.fullPath],clear,{flush:'sync'})
watch(()=>route.fullPath,()=>{tenantId.value='';source.value='';recordId.value='';requestId.value='';destination.value='';policy.value='';intent.value=''},{flush:'sync'})
</script>

<template>
 <section class="logging-panel bg-fill-base-default text-pen-base-default p-step-md" :aria-labelledby="'logging-title-'+section">
  <h1 :id="'logging-title-'+section">{{ t('logging.'+section) }}</h1>
  <nav :aria-label="t('logging.navigation')"><NuxtLink class="text-pen-link-default hover:bg-fill-link-hover hover:text-pen-link-hover focus-visible:outline-focus focus-visible:outline-edge-link-focus" v-for="[key,path] in routes" :key="key" :to="path">{{ t('logging.'+key) }}</NuxtLink></nav>
  <form @submit.prevent="offset=0;call()">
   <label>{{ t('logging.tenant') }}<input class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="tenantId" required maxlength="96" @input="clear"></label>
   <label>{{ t('logging.source') }}<input class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="source" required maxlength="96" @input="clear"></label>
   <template v-if="section!=='settings'">
    <label>{{ t('logging.from') }}<input class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="from" required type="datetime-local" @input="clear"></label>
    <label>{{ t('logging.to') }}<input class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="to" required type="datetime-local" @input="clear"></label>
    <label v-if="section==='data'||section==='delivery'">{{ t('logging.recordId') }}<input class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="recordId" maxlength="96" @input="clear"></label>
    <label v-if="section!=='detail'">{{ t('logging.severity') }}<select class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="severity"><option value="">{{ t('logging.allSeverities') }}</option><option v-for="level in ['debug','info','warn','error','fatal']" :key="level" :value="level">{{ t('logging.severity.'+level) }}</option></select></label>
    <label v-if="section!=='detail'">{{ t('logging.code') }}<input class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="code" maxlength="96"></label>
    <label v-if="section!=='detail'">{{ t('logging.correlation') }}<input class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="correlation" maxlength="96"></label>
   </template>
   <button class="rounded-control border border-edge-primary-default bg-fill-primary-default text-pen-primary-default hover:bg-fill-primary-hover hover:text-pen-primary-hover hover:border-edge-primary-hover focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-primary-focus disabled:bg-fill-primary-disabled disabled:text-pen-primary-disabled disabled:border-edge-primary-disabled" type="submit" :disabled="busy">{{ t('logging.load') }}</button>
  </form>
  <p role="status" aria-live="polite">{{ busy?t('logging.loading'):notice }}</p>
  <template v-if="result">
   <h2>{{ t('logging.result') }}</h2>
   <p>{{ t('logging.outcome.'+outcome) }}</p>
   <dl v-if="health"><template v-for="key in (['records','delivered','exhausted','uncertain','withoutDelivery','backlogRecords'] as const)" :key="key"><dt>{{ t('logging.health.'+key) }}</dt><dd>{{ health[key] }}</dd></template></dl>
   <p v-if="health">{{ t('logging.health.coverage') }}: {{ health.coverage }}</p>
   <ul v-if="section==='records'&&rows.length"><li v-for="row in rows" :key="(row as any).id"><NuxtLink class="text-pen-link-default hover:bg-fill-link-hover hover:text-pen-link-hover focus-visible:outline-focus focus-visible:outline-edge-link-focus" :to="'/logging/records/'+encodeURIComponent((row as any).id)">{{ (row as any).code }} — {{ (row as any).id }}</NuxtLink></li></ul>
   <details><summary>{{ t('logging.technicalDetails') }}</summary><pre tabindex="0" class="border border-edge-base-default bg-fill-base-default text-pen-base-default focus-visible:outline-focus focus-visible:outline-edge-base-focus">{{ JSON.stringify(result,null,2) }}</pre></details>
   <div v-if="section==='records'||section==='delivery'">
    <button class="rounded-control border border-edge-primary-default bg-fill-primary-default text-pen-primary-default hover:bg-fill-primary-hover hover:text-pen-primary-hover hover:border-edge-primary-hover focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-primary-focus disabled:bg-fill-primary-disabled disabled:text-pen-primary-disabled disabled:border-edge-primary-disabled" :disabled="busy||offset===0" @click="offset=Math.max(0,offset-20);call()">{{ t('logging.previous') }}</button>
    <button class="rounded-control border border-edge-primary-default bg-fill-primary-default text-pen-primary-default hover:bg-fill-primary-hover hover:text-pen-primary-hover hover:border-edge-primary-hover focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-primary-focus disabled:bg-fill-primary-disabled disabled:text-pen-primary-disabled disabled:border-edge-primary-disabled" :disabled="busy||rows.length<20" @click="offset+=20;call()">{{ t('logging.next') }}</button>
   </div>
  </template>
  <fieldset v-if="section==='delivery'" class="border border-edge-base-default p-step-sm">
   <legend>{{ t('logging.deliveryRecovery') }}</legend>
   <label>{{ t('logging.requestId') }}<input v-model="requestId" maxlength="96" class="border border-edge-input-default bg-fill-input-default text-pen-input-default"></label>
   <label>{{ t('logging.destination') }}<input v-model="destination" maxlength="96" class="border border-edge-input-default bg-fill-input-default text-pen-input-default"></label>
   <label>{{ t('logging.intent') }}<select v-model="intent" class="border border-edge-input-default bg-fill-input-default text-pen-input-default"><option value="">{{ t('logging.chooseOperation') }}</option><option value="delivery.retry">{{ t('logging.retryDelivery') }}</option><option value="delivery.reconcile">{{ t('logging.reconcileDelivery') }}</option></select></label>
   <label><input v-model="confirmed" type="checkbox">{{ t('logging.confirm') }}</label>
   <button type="button" class="rounded-control border border-edge-primary-default bg-fill-primary-default text-pen-primary-default focus-visible:outline-focus focus-visible:outline-edge-primary-focus" :disabled="busy||!confirmed||!tenantId||!source||!recordId||!requestId||!destination||!['delivery.retry','delivery.reconcile'].includes(intent)" @click="call(intent)">{{ t('logging.executeRecovery') }}</button>
  </fieldset>
  <fieldset class="border border-edge-base-default p-step-sm" v-if="section==='settings'||section==='data'">
   <legend>{{ t('logging.protected') }}</legend>
   <label>{{ t('logging.requestId') }}<input class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="requestId" maxlength="96"></label>
   <label>{{ t('logging.intent') }}<select class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="intent"><option value="">{{ t('logging.chooseOperation') }}</option><option v-if="section==='settings'" value="policy.change">{{ t('logging.changePolicy') }}</option><template v-else><option v-for="op in ['export','restrict','erase','provider']" :key="op" :value="op">{{ t('logging.'+op) }}</option></template></select></label>
   <label><input class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="confirmed" type="checkbox">{{ t('logging.confirm') }}</label>
   <template v-if="section==='settings'">
    <label>{{ t('logging.policyJson') }}<textarea class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="policy" rows="6" maxlength="16000"/></label>
    <button class="rounded-control border border-edge-primary-default bg-fill-primary-default text-pen-primary-default hover:bg-fill-primary-hover hover:text-pen-primary-hover hover:border-edge-primary-hover focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-primary-focus disabled:bg-fill-primary-disabled disabled:text-pen-primary-disabled disabled:border-edge-primary-disabled" :disabled="busy||!confirmed||!requestId||intent!=='policy.change'" @click="call('policy.change')">{{ t('logging.changePolicy') }}</button>
   </template>
   <template v-else>
    <button class="rounded-control border border-edge-primary-default bg-fill-primary-default text-pen-primary-default hover:bg-fill-primary-hover hover:text-pen-primary-hover hover:border-edge-primary-hover focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-primary-focus disabled:bg-fill-primary-disabled disabled:text-pen-primary-disabled disabled:border-edge-primary-disabled" :disabled="busy||!confirmed||!requestId||intent!=='export'" @click="call('export')">{{ t('logging.export') }}</button>
    <button class="rounded-control border border-edge-primary-default bg-fill-primary-default text-pen-primary-default hover:bg-fill-primary-hover hover:text-pen-primary-hover hover:border-edge-primary-hover focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-primary-focus disabled:bg-fill-primary-disabled disabled:text-pen-primary-disabled disabled:border-edge-primary-disabled" :disabled="busy||!confirmed||!requestId||!recordId||intent!=='restrict'" @click="call('restrict')">{{ t('logging.restrict') }}</button>
    <button class="rounded-control border border-edge-primary-default bg-fill-primary-default text-pen-primary-default hover:bg-fill-primary-hover hover:text-pen-primary-hover hover:border-edge-primary-hover focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-primary-focus disabled:bg-fill-primary-disabled disabled:text-pen-primary-disabled disabled:border-edge-primary-disabled" :disabled="busy||!confirmed||!requestId||!recordId||intent!=='erase'" @click="call('erase')">{{ t('logging.erase') }}</button>
    <label>{{ t('logging.destination') }}<input class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="destination" maxlength="96"></label>
    <label>{{ t('logging.action') }}<select class="rounded-control border border-edge-input-default bg-fill-input-default text-pen-input-default focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-input-focus" v-model="action"><option value="restrict">{{t('logging.restrict')}}</option><option value="erase">{{t('logging.erase')}}</option><option value="recover">{{t('logging.recover')}}</option></select></label>
    <button class="rounded-control border border-edge-primary-default bg-fill-primary-default text-pen-primary-default hover:bg-fill-primary-hover hover:text-pen-primary-hover hover:border-edge-primary-hover focus-visible:outline-focus focus-visible:outline-offset-focus focus-visible:outline-edge-primary-focus disabled:bg-fill-primary-disabled disabled:text-pen-primary-disabled disabled:border-edge-primary-disabled" :disabled="busy||!confirmed||!requestId||!recordId||!destination||intent!=='provider'" @click="call('provider')">{{t('logging.provider')}}</button>
   </template>
  </fieldset>
 </section>
</template>
<style scoped>
.logging-panel{display:grid;gap:1rem;min-width:0}
nav,form,fieldset{display:flex;flex-wrap:wrap;gap:1rem;align-items:end}
label{display:grid;gap:.25rem;min-width:0}
input,textarea,select,button{font:inherit;padding:.5rem;max-width:100%}

pre{white-space:pre-wrap;overflow-wrap:anywhere;max-height:32rem;overflow:auto;padding:1rem}
</style>
