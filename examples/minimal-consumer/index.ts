import { createLogging } from '@nuxt-layers/logging'
import type { Options } from '@nuxt-layers/logging'
export function compose<E>(host: Options<E>) {
  return createLogging(host)
}
