import type { H3Event } from 'h3'
import { createLogging, type Options } from '../../dist/index.js'
type Service = ReturnType<typeof createLogging<H3Event>>
const instances = new WeakMap<object, Service>()
/** Compose once per Nitro application. Never expose this registry to client code. */
export function provideLogging(app: object, options: Options<H3Event>): Service {
  if (instances.has(app)) throw new Error('LOG_ALREADY_COMPOSED')
  const service = createLogging(options)
  instances.set(app, service)
  return service
}
export function getLogging(app: object): Service | undefined {
  return instances.get(app)
}
export function closeLogging(app: object): void {
  instances.get(app)?.close()
  instances.delete(app)
}
