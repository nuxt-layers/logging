import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createLogging } from '@nuxt-layers/logging'
test('public package composes without Nuxt and fails closed', async () => {
  const service = createLogging({
    resolveCaller: async () => null, resolveProducer: async () => null,
    decide: async () => ({allowed:false,reason:'denied'}),
    now: () => ({instant:'2026-11-26T00:00:00Z',quality:'unknown'}),
    localSink: {write: async () => 'refused'}, rules: [],
    policyRevision:'v1', retentionMs:1000,
  })
  assert.equal((await service.submit(null,{version:1,code:'test',severity:'info',occurrenceTime:'2026-11-26T00:00:00Z'})).status,'rejected')
})
