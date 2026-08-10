import { logger } from '@sailpoint/connector-sdk'

/** Executes an API call and returns undefined on failure after logging the error. */
export async function wrapApiCall<T>(
    fn: () => Promise<T>,
    context: string,
    requestDetails?: unknown
): Promise<T | undefined> {
    try {
        return await fn()
    } catch (error) {
        const errorMessage = `${context}: ${error instanceof Error ? error.message : error}`
        logger.error(requestDetails, errorMessage)
        logger.debug(error, `Failed API request: ${context}`)
        return undefined
    }
}

/** Executes an API call and returns an error message string on failure. */
export async function wrapApiMutation(fn: () => Promise<void>, context: string, requestDetails?: unknown): Promise<string> {
    try {
        await fn()
        return ''
    } catch (error) {
        const errorMessage = `${context}: ${error instanceof Error ? error.message : error}`
        logger.error(requestDetails, errorMessage)
        logger.debug(error, `Failed API request: ${context}`)
        return errorMessage
    }
}

export function mergeUnique<T>(items1: T[], items2: T[]): T[] {
    return [...new Set([...items1, ...items2])]
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
