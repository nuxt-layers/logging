export default defineNuxtConfig({
 runtimeConfig: { logging: { presentation: false, origin: '' }, public: { loggingPresentation: false } },
 routeRules: { '/logging/**': { headers: { 'Cache-Control':'no-store, private','Referrer-Policy':'no-referrer','X-Frame-Options':'DENY' } }, '/logging': { headers: { 'Cache-Control':'no-store, private','Referrer-Policy':'no-referrer','X-Frame-Options':'DENY' } } },
})
