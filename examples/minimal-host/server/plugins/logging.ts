import { provideLogging, closeLogging } from '@nuxt-layers/logging/nuxt/server'
export default defineNitroPlugin(app => {
  // Deny-all example: no fabricated production identity or permission.
  provideLogging(app, {
    resolveCaller: async () => null,
    resolveProducer: async () => null,
    decide: async () => ({allowed:false,reason:'not-configured'}),
    now: () => ({instant:new Date().toISOString(),quality:'unknown'}),
    localSink: {write:async () => 'refused'},
    rules: [], policyRevision:'example',retentionMs:1000,
  })
  app.hooks.hook('close', () => closeLogging(app))
})
