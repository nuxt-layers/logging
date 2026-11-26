export default defineNuxtConfig({
  // The layer contributes server integration only until presentation is accepted.
  runtimeConfig: { logging: { presentation: false } },
})
