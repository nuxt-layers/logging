import { readFileSync } from 'node:fs'
import assert from 'node:assert/strict'
const doc=readFileSync('CONTRACT.md','utf8')
const types=readFileSync('src/contracts/types.ts','utf8').trim()
assert.deepEqual([...doc.matchAll(/^## (.+)$/gm)].map(x=>x[1]),['Install','Settings','Host passes','Client and server functions','Permissions','Screens','Headless surface','You will hit this'])
assert.ok(doc.includes(types),'Exported types must match the contract')
assert.ok(doc.includes('Public inputs supplied by the wider application'))
assert.ok(doc.includes('Public outputs supplied to the wider application'))
const service=readFileSync('src/services/logging.ts','utf8')
for(const code of new Set(service.match(/LOG_[A-Z_]+/g))) assert.ok(doc.includes(code), 'Missing code '+code)
