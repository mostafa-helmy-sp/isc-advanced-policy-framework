import { AsyncLocalStorage } from 'async_hooks'
import { logger as sdkLogger } from '@sailpoint/connector-sdk'

type LogFn = (detailsOrMessage: unknown, message?: string) => void
type LogLevel = 'debug' | 'info' | 'warn' | 'error'
type LevelLogger = Record<LogLevel, LogFn>

const policyLoggerStorage = new AsyncLocalStorage<LevelLogger>()

/** Runs `fn` so that every line logged within it (including after awaits) carries `policyName`. */
export function runWithPolicyLogger<T>(policyName: string, fn: () => Promise<T>): Promise<T> {
    return policyLoggerStorage.run(sdkLogger.child({ policyName }), fn)
}

function logMethod(level: LogLevel): LogFn {
    return (detailsOrMessage, message) => {
        const target: LevelLogger = policyLoggerStorage.getStore() ?? sdkLogger
        if (message === undefined) {
            target[level](detailsOrMessage)
        } else {
            target[level](detailsOrMessage, message)
        }
    }
}

/** Connector logger. Adds `policyName` to lines logged while a policy is being processed. */
export const logger: LevelLogger = {
    debug: logMethod('debug'),
    info: logMethod('info'),
    warn: logMethod('warn'),
    error: logMethod('error'),
}
