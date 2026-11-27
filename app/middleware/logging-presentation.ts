export default defineNuxtRouteMiddleware(() => {
 // Public flag is only a presentation switch; every API independently authorises.
 if(!useRuntimeConfig().public.loggingPresentation) return abortNavigation(createError({statusCode:404}))
})
