import { logger } from '@sailpoint/connector-sdk'
import { API_RETRY_BASE_DELAY_MS, API_RETRY_MAX_ATTEMPTS } from '../config/defaults'

export type ApiResult<T> = { ok: true; data: T } | { ok: false; error: string }

export interface SearchItemsResult<T> {
    items: T[]
    error?: string
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

function getErrorStatus(error: unknown): number | undefined {
    if (typeof error !== 'object' || error === null || !('response' in error)) {
        return undefined
    }
    const response = (error as { response?: { status?: number } }).response
    return response?.status
}

export function isRateLimitError(error: unknown): boolean {
    return getErrorStatus(error) === 429
}

export function getRetryDelayMs(error: unknown, attempt: number): number {
    if (typeof error === 'object' && error !== null && 'response' in error) {
        const headers = (error as { response?: { headers?: Record<string, unknown> } }).response?.headers
        const retryAfter = headers?.['retry-after'] ?? headers?.['Retry-After']
        if (retryAfter !== undefined && retryAfter !== null) {
            const seconds = Number(retryAfter)
            if (!Number.isNaN(seconds) && seconds >= 0) {
                return Math.max(seconds * 1000, 0)
            }
        }
    }
    return API_RETRY_BASE_DELAY_MS * Math.pow(2, attempt)
}

function formatErrorMessage(context: string, error: unknown): string {
    return `${context}: ${error instanceof Error ? error.message : error}`
}

/** Retries the call on HTTP 429, honoring Retry-After when present. */
export async function executeWithRetry<T>(fn: () => Promise<T>, context: string): Promise<T> {
    let attempt = 0
    for (;;) {
        try {
            return await fn()
        } catch (error) {
            if (!isRateLimitError(error) || attempt >= API_RETRY_MAX_ATTEMPTS) {
                throw error
            }
            const delayMs = getRetryDelayMs(error, attempt)
            logger.info(
                `Retrying API [${context}] after HTTP 429. Attempt ${attempt + 1}/${API_RETRY_MAX_ATTEMPTS}, waiting ${delayMs}ms`
            )
            await sleep(delayMs)
            attempt += 1
        }
    }
}

/** Executes an API call with 429 retry and returns a typed success/failure result. */
export async function wrapApiCallResult<T>(
    fn: () => Promise<T>,
    context: string,
    requestDetails?: unknown
): Promise<ApiResult<T>> {
    try {
        const data = await executeWithRetry(fn, context)
        return { ok: true, data }
    } catch (error) {
        const errorMessage = formatErrorMessage(context, error)
        logger.error(requestDetails, errorMessage)
        logger.debug(error, `Failed API request: ${context}`)
        return { ok: false, error: errorMessage }
    }
}

/** Executes an API call and returns undefined on failure after logging the error. */
export async function wrapApiCall<T>(
    fn: () => Promise<T>,
    context: string,
    requestDetails?: unknown
): Promise<T | undefined> {
    const result = await wrapApiCallResult(fn, context, requestDetails)
    return result.ok ? result.data : undefined
}

/** Executes an API call and returns an error message string on failure. */
export async function wrapApiMutation(fn: () => Promise<void>, context: string, requestDetails?: unknown): Promise<string> {
    const result = await wrapApiCallResult(fn, context, requestDetails)
    return result.ok ? '' : result.error
}

export function mergeUnique<T>(items1: T[], items2: T[]): T[] {
    return [...new Set([...items1, ...items2])]
}

export function chunkArray<T>(items: T[], size: number): T[][] {
    if (size <= 0 || items.length === 0) {
        return items.length === 0 ? [] : [items]
    }

    const chunks: T[][] = []
    for (let index = 0; index < items.length; index += size) {
        chunks.push(items.slice(index, index + size))
    }
    return chunks
}

export function buildBatchedIdQueries(
    ids: string[],
    batchSize: number,
    itemPrefix: string,
    joiner: string,
    prefix?: string,
    suffix?: string
): string[] {
    if (ids.length === 0) {
        return []
    }
    if (ids.length <= batchSize) {
        return [buildIdQuery(ids, itemPrefix, joiner, prefix, suffix)]
    }
    return chunkArray(ids, batchSize).map((batch) => buildIdQuery(batch, itemPrefix, joiner, prefix, suffix))
}

export function deduplicateById<T extends { id?: string }>(items: T[]): T[] {
    return [...new Map(items.filter((item) => item.id).map((item) => [item.id, item])).values()]
}

export function buildIdArray(items: Array<{ id?: string }>): string[] {
    return items.map((item) => item.id).filter((id): id is string => !!id)
}

export function buildIdQuery(ids: string[], itemPrefix: string, joiner: string, prefix?: string, suffix?: string): string {
    let query = prefix ?? ''
    ids.forEach((id, index) => {
        if (index > 0) {
            query += joiner
        }
        query += itemPrefix + id
    })
    if (suffix) {
        query += suffix
    }
    return query
}

export function buildIdQueryFromItems(
    items: Array<{ id?: string }>,
    itemPrefix: string,
    joiner: string,
    prefix?: string,
    suffix?: string
): string {
    return buildIdQuery(buildIdArray(items), itemPrefix, joiner, prefix, suffix)
}

export function buildNameArray(items: Array<{ name?: string }>): string[] {
    return items.map((item) => item.name).filter((name): name is string => !!name)
}

export function buildEntitlementNameArray(items: Array<{ name?: string; schema?: string; source?: { name?: string } }>): string[] {
    return items.map(
        (item) => `Source: ${item.source?.name ?? 'N/A'}, Type: ${item.schema ?? 'N/A'}, Name: ${item.name ?? 'N/A'}`
    )
}
