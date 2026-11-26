import { readFileSync } from 'node:fs'
import assert from 'node:assert/strict'
for(const paths of [['examples/minimal-consumer/index.ts'],['examples/minimal-host/nuxt.config.ts','examples/minimal-host/server/plugins/logging.ts']]){
 const lines=paths.reduce((n,p)=>n+readFileSync(p,'utf8').split('\n').filter(l=>l.trim()).length,0)
 console.log(paths.join(', ')+': '+lines+' nonblank lines')
 assert.ok(lines<=50)
}
