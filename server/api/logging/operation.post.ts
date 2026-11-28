import { defineEventHandler, getHeader, readRawBody, setResponseHeader, createError } from 'h3'
import type { H3Event } from 'h3'
import type { DiagnosticQuery, LifecycleTarget, PolicyChange, ProviderLifecycleRequest, DeliveryRecoveryRequest } from '../../../src/index'
import { getLogging } from '../../utils/logging'

export default defineEventHandler(async (event: H3Event) => {
  setResponseHeader(event, 'Cache-Control', 'no-store, private')
  setResponseHeader(event, 'Referrer-Policy', 'no-referrer')
  setResponseHeader(event, 'X-Content-Type-Options', 'nosniff')
  setResponseHeader(event, 'X-Frame-Options', 'DENY')
  const config = useRuntimeConfig(event).logging
  if (!config?.presentation) throw createError({ statusCode: 404 })
  // Host must configure its canonical public origin; never trust forwarded host headers.
  if (!config.origin || getHeader(event,'origin') !== config.origin ||
      getHeader(event,'sec-fetch-site') === 'cross-site' ||
      !getHeader(event,'content-type')?.match(/^application\/json(?:;|$)/i) ||
      getHeader(event,'x-logging-request') !== '1') throw createError({statusCode:403})
  const size = Number(getHeader(event,'content-length'))
  if(!Number.isSafeInteger(size)||size<1||size>32768||getHeader(event,'transfer-encoding'))throw createError({statusCode:413})
  const raw=await readRawBody(event)
  if(!raw||new TextEncoder().encode(raw).length>32768)throw createError({statusCode:413})
  let body
  try {body=JSON.parse(raw)}catch{throw createError({statusCode:400})}
  if(!body||typeof body!=='object'||Array.isArray(body))throw createError({statusCode:400})
  const service=getLogging(useNitroApp())
  if(!service)throw createError({statusCode:503})
  const input=body.input
  // No generic method dispatch, no host health, no relay, no browser diagnostic intake.
  switch(body.operation) {
    case 'records': return service.readDiagnostics(event,input as DiagnosticQuery)
    case 'delivery': return service.readDeliveryState(event,input as DiagnosticQuery)
    case 'health': return service.readPipelineHealth(event,input as DiagnosticQuery)
    case 'policy': return service.readEffectivePolicy(event,input)
    case 'export': return service.exportDiagnostics(event,input as DiagnosticQuery,body.requestId)
    case 'erase': return service.eraseDiagnostic(event,input as LifecycleTarget,body.requestId)
    case 'restrict': return service.restrictDiagnostic(event,input as LifecycleTarget,body.requestId)
    case 'provider': return service.runProviderLifecycle(event,input as Pick<ProviderLifecycleRequest,'action'|'target'|'destination'|'requestId'>)
    case 'delivery.retry':
    case 'delivery.reconcile': {
      if(!input||input.mode!==(body.operation==='delivery.retry'?'retry':'reconcile'))throw createError({statusCode:400})
      return service.recoverDelivery(event,input as DeliveryRecoveryRequest)
    }
    case 'policy.change': return service.changePolicy(event,input as PolicyChange)
    default: throw createError({statusCode:400})
  }
})
