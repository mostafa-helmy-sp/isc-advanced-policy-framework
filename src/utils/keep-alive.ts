import { KEEP_ALIVE_INTERVAL_MS } from '../config/defaults'

/**
 * Runs `fn` while periodically calling `res.keepAlive()`. SaaS commands time out after 3 minutes without output,
 * which long 429 retry waits would otherwise exceed.
 */
export async function withKeepAlive<T>(res: { keepAlive(): void }, fn: () => Promise<T>): Promise<T> {
    const timer = setInterval(() => res.keepAlive(), KEEP_ALIVE_INTERVAL_MS)
    try {
        return await fn()
    } finally {
        clearInterval(timer)
    }
}
