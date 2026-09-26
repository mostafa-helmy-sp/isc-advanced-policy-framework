import axios, { AxiosError, AxiosInstance, AxiosResponse, InternalAxiosRequestConfig } from 'axios'
import { parseRetryAfter } from '@sailpoint/connector-sdk'
import {
    API_429_MAX_RETRIES,
    API_5XX_MAX_RETRIES,
    API_REQUEST_TIMEOUT_MS,
    API_RETRY_BASE_DELAY_MS,
    API_RETRY_MAX_DELAY_MS,
} from '../config/defaults'
import { logger } from '../utils/logger'

interface RetryableRequestConfig extends InternalAxiosRequestConfig {
    __retryCount?: number
    __startedAt?: number
}

const NON_RETRYABLE_NETWORK_CODES = new Set(['ECONNABORTED', 'ENOTFOUND', 'ERR_CANCELED'])
const IDEMPOTENT_METHODS = new Set(['get', 'head', 'options', 'put', 'delete'])
const SEARCH_API_PATH = /\/search\/v\d+(\/|$)/
const COOLDOWN_JITTER_MS = 1000

// ISC rate limits per client_id, so after one 429 every request in this process is over the limit until Retry-After passes.
let rateLimitedUntil = 0

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

/** Clears the shared 429 cooldown. Intended for tests. */
export function resetRateLimitCooldown(): void {
    rateLimitedUntil = 0
}

/**
 * Delay before retry `attempt` (1-based): exponential backoff whose jitter window widens with every attempt and
 * never undercuts Retry-After, so requests that keep colliding spread out across later rate-limit windows.
 */
export function computeRetryDelayMs(attempt: number, retryAfterMs?: number): number {
    const backoffMs = Math.min(API_RETRY_MAX_DELAY_MS, API_RETRY_BASE_DELAY_MS * 2 ** (attempt - 1))
    const floorMs = retryAfterMs !== undefined ? Math.min(retryAfterMs, API_RETRY_MAX_DELAY_MS) : backoffMs / 2
    return Math.round(Math.min(API_RETRY_MAX_DELAY_MS, floorMs + Math.random() * backoffMs))
}

async function waitForRateLimitCooldown(): Promise<void> {
    for (let waitMs = rateLimitedUntil - Date.now(); waitMs > 0; waitMs = rateLimitedUntil - Date.now()) {
        await sleep(waitMs + Math.random() * COOLDOWN_JITTER_MS)
    }
}

function requestPath(config: InternalAxiosRequestConfig): string {
    try {
        return new URL(config.url ?? '', config.baseURL).pathname
    } catch {
        return config.url ?? ''
    }
}

function describeRequest(config: InternalAxiosRequestConfig): string {
    return `${(config.method ?? 'get').toUpperCase()} ${requestPath(config)}`
}

function elapsedMs(config: RetryableRequestConfig): number {
    return config.__startedAt ? Date.now() - config.__startedAt : 0
}

function isSafeToRepeat(config: InternalAxiosRequestConfig): boolean {
    const method = (config.method ?? 'get').toLowerCase()
    return IDEMPOTENT_METHODS.has(method) || (method === 'post' && SEARCH_API_PATH.test(requestPath(config)))
}

function getMaxRetries(error: AxiosError, config: InternalAxiosRequestConfig): number {
    const status = error.response?.status
    if (status === 429) {
        return API_429_MAX_RETRIES
    }
    if (!isSafeToRepeat(config)) {
        return 0
    }
    if (status === undefined) {
        return error.code && NON_RETRYABLE_NETWORK_CODES.has(error.code) ? 0 : API_5XX_MAX_RETRIES
    }
    return status >= 500 && status <= 599 ? API_5XX_MAX_RETRIES : 0
}

function getRetryAfterMs(error: AxiosError): number | undefined {
    const header = error.response?.headers?.['retry-after']
    return parseRetryAfter(header == null ? undefined : String(header))
}

function firstText(list: unknown): string | undefined {
    const text = Array.isArray(list) ? (list[0] as { text?: unknown } | undefined)?.text : undefined
    return typeof text === 'string' ? text : undefined
}

function stringField(value: unknown): string | undefined {
    return typeof value === 'string' && value ? value : undefined
}

function extractErrorDetail(data: unknown): string | undefined {
    if (typeof data !== 'object' || data === null) {
        return undefined
    }
    const body = data as Record<string, unknown>
    const oauthError = stringField(body.error)
    if (oauthError) {
        const description = stringField(body.error_description)
        return description ? `${oauthError}: ${description}` : oauthError
    }
    return (
        stringField(body.message) ??
        firstText(body.causes) ??
        firstText(body.messages) ??
        stringField(body.formatted_msg) ??
        stringField(body.errorMessage) ??
        stringField(body.detailMessage)
    )
}

/** Builds a plain Error (no `response`), so no request config or token can leak and the SDK passes it through unchanged. */
function toReadableError(error: AxiosError, retryCount: number): Error {
    const response = error.response
    let message: string
    if (response) {
        const statusLabel = response.statusText ? `${response.status} ${response.statusText}` : `${response.status}`
        message = `HTTP ${statusLabel}: ${extractErrorDetail(response.data) ?? error.message}`
        const trackingId = stringField((response.data as { trackingId?: unknown } | null | undefined)?.trackingId)
        if (trackingId) {
            message += ` (trackingId ${trackingId})`
        }
    } else {
        message = error.code ? `${error.code}: ${error.message}` : error.message
    }
    if (retryCount > 0) {
        message += ` (gave up after ${retryCount} ${retryCount === 1 ? 'retry' : 'retries'})`
    }
    return new Error(message)
}

async function onRequest(config: RetryableRequestConfig): Promise<InternalAxiosRequestConfig> {
    await waitForRateLimitCooldown()
    config.__startedAt = Date.now()
    return config
}

function onResponse(response: AxiosResponse): AxiosResponse {
    logger.debug(`HTTP ${describeRequest(response.config)} ${response.status} (${elapsedMs(response.config)}ms)`)
    return response
}

async function onErrorResponse(error: unknown, instance: AxiosInstance): Promise<AxiosResponse> {
    if (!axios.isAxiosError(error) || !error.config) {
        throw error
    }
    const config: RetryableRequestConfig = error.config
    const status = error.response?.status
    const outcome = status !== undefined ? `HTTP ${status}` : (error.code ?? 'network error')
    logger.debug(`HTTP ${describeRequest(config)} ${outcome} (${elapsedMs(config)}ms)`)

    const retryAfterMs = status === 429 ? getRetryAfterMs(error) : undefined
    if (status === 429) {
        const cooldownMs = Math.min(retryAfterMs ?? API_RETRY_BASE_DELAY_MS, API_RETRY_MAX_DELAY_MS)
        rateLimitedUntil = Math.max(rateLimitedUntil, Date.now() + cooldownMs)
    }

    const retryCount = config.__retryCount ?? 0
    const maxRetries = getMaxRetries(error, config)
    if (retryCount >= maxRetries) {
        throw toReadableError(error, retryCount)
    }

    const attempt = retryCount + 1
    const waitMs = computeRetryDelayMs(attempt, retryAfterMs)
    logger.info(
        `Retrying ${describeRequest(config)} after ${outcome} (attempt ${attempt}/${maxRetries}, waiting ${waitMs}ms)`
    )
    config.__retryCount = attempt
    await sleep(waitMs)
    return instance(config)
}

/**
 * Creates the axios instance every SDK API class uses: retries 429s (any method) and transient 5xx/network errors
 * (read-only or idempotent requests only), then maps final failures to readable ISC error messages.
 */
export function createApiAxiosInstance(): AxiosInstance {
    const instance = axios.create({ timeout: API_REQUEST_TIMEOUT_MS })
    instance.interceptors.request.use(onRequest)
    instance.interceptors.response.use(onResponse, (error) => onErrorResponse(error, instance))
    return instance
}
