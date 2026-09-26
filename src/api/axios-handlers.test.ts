import { AxiosError, AxiosResponse, InternalAxiosRequestConfig } from 'axios'
import {
    AccountsApi,
    CertificationCampaignsApi,
    Configuration,
    EntitlementsApi,
    GovernanceGroupsApi,
    Search,
    SearchApi,
    SearchIndex,
    SODPoliciesApi,
    SourcesApi,
} from '../types/sailpoint-api'
import { API_429_MAX_RETRIES, API_5XX_MAX_RETRIES } from '../config/defaults'
import { logger } from '../utils/logger'
import { computeRetryDelayMs, createApiAxiosInstance, resetRateLimitCooldown } from './axios-handlers'

jest.mock('../utils/logger', () => ({
    logger: { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() },
}))

const STATUS_TEXT: Record<number, string> = {
    200: 'OK',
    400: 'Bad Request',
    429: 'Too Many Requests',
    503: 'Service Unavailable',
}

type StubReply = { status: number; data?: unknown; headers?: Record<string, string> } | { networkError: string }

const search: Search = { indices: [SearchIndex.Entitlements], query: { query: '*' }, sort: ['id'] }
const rateLimited: StubReply = {
    status: 429,
    data: { message: 'Rate Limit Exceeded' },
    headers: { 'retry-after': '0' },
}

/** A real SDK Configuration wired the same way as createApiConfig, minus the network token fetch. */
function createTestConfig(): Configuration {
    const apiConfig = new Configuration({ baseurl: 'https://tenant.api.identitynow.com', accessToken: 'test-token' })
    apiConfig.axiosInstance = createApiAxiosInstance()
    return apiConfig
}

/** Replaces the HTTP transport of the config's axios instance; the last reply repeats once the list is exhausted. */
function stubTransport(apiConfig: Configuration, replies: StubReply[]): jest.Mock {
    let call = 0
    const adapter = jest.fn(async (config: InternalAxiosRequestConfig): Promise<AxiosResponse> => {
        const reply = replies[Math.min(call++, replies.length - 1)]
        if ('networkError' in reply) {
            throw new AxiosError('socket hang up', reply.networkError, config)
        }
        const response: AxiosResponse = {
            status: reply.status,
            statusText: STATUS_TEXT[reply.status] ?? '',
            data: reply.data ?? {},
            headers: reply.headers ?? {},
            config,
        }
        if (reply.status >= 400) {
            const code = reply.status >= 500 ? AxiosError.ERR_BAD_RESPONSE : AxiosError.ERR_BAD_REQUEST
            throw new AxiosError(`Request failed with status code ${reply.status}`, code, config, {}, response)
        }
        return response
    })
    Object.assign(apiConfig.axiosInstance.defaults, { adapter })
    return adapter
}

describe('API axios handlers', () => {
    beforeEach(() => {
        resetRateLimitCooldown()
        jest.spyOn(Math, 'random').mockReturnValue(0)
    })

    afterEach(() => {
        jest.useRealTimers()
        jest.restoreAllMocks()
    })

    it('is the axios instance every SDK API class uses', () => {
        const apiConfig = createTestConfig()
        const apiClasses = [
            AccountsApi,
            CertificationCampaignsApi,
            EntitlementsApi,
            GovernanceGroupsApi,
            SearchApi,
            SODPoliciesApi,
            SourcesApi,
        ]
        for (const ApiClass of apiClasses) {
            expect((new ApiClass(apiConfig) as unknown as { axios: unknown }).axios).toBe(apiConfig.axiosInstance)
        }
    })

    it('retries a 429 after Retry-After and then succeeds', async () => {
        jest.useFakeTimers()
        const apiConfig = createTestConfig()
        const transport = stubTransport(apiConfig, [
            { ...rateLimited, headers: { 'retry-after': '3' } },
            { status: 200, data: [{ id: 'e1' }] },
        ])

        const result = new SearchApi(apiConfig).searchPostV1({ search })
        await jest.advanceTimersByTimeAsync(2999)
        expect(transport).toHaveBeenCalledTimes(1)

        await jest.advanceTimersByTimeAsync(1)
        await expect(result).resolves.toMatchObject({ data: [{ id: 'e1' }] })
        expect(transport).toHaveBeenCalledTimes(2)
        expect(logger.info).toHaveBeenCalledWith(
            'Retrying POST /search/v1 after HTTP 429 (attempt 1/15, waiting 3000ms)'
        )
    })

    it('gives up after the 429 retry limit with a readable error and no response attached', async () => {
        const apiConfig = createTestConfig()
        const transport = stubTransport(apiConfig, [rateLimited])

        const error = await new SearchApi(apiConfig).searchPostV1({ search }).catch((e: unknown) => e)

        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toBe(
            `HTTP 429 Too Many Requests: Rate Limit Exceeded (gave up after ${API_429_MAX_RETRIES} retries)`
        )
        expect(error).not.toHaveProperty('response')
        expect(error).not.toHaveProperty('config')
        expect(transport).toHaveBeenCalledTimes(API_429_MAX_RETRIES + 1)
    })

    it('retries a 429 on a policy create, since the gateway rejected it before processing', async () => {
        const apiConfig = createTestConfig()
        const transport = stubTransport(apiConfig, [rateLimited, { status: 200, data: { id: 'p1' } }])

        await expect(
            new SODPoliciesApi(apiConfig).createSodPolicyV1({ sodPolicy: { name: 'P1' } })
        ).resolves.toMatchObject({
            data: { id: 'p1' },
        })
        expect(transport).toHaveBeenCalledTimes(2)
    })

    it('surfaces the ISC error text and trackingId without retrying a 400', async () => {
        const apiConfig = createTestConfig()
        const transport = stubTransport(apiConfig, [
            {
                status: 400,
                data: {
                    detailCode: '400.1 Bad Request Content',
                    trackingId: 'abc123',
                    messages: [{ locale: 'en-US', localeOrigin: 'DEFAULT', text: 'Invalid query syntax' }],
                    causes: [],
                },
            },
        ])

        await expect(new SearchApi(apiConfig).searchPostV1({ search })).rejects.toThrow(
            'HTTP 400 Bad Request: Invalid query syntax (trackingId abc123)'
        )
        expect(transport).toHaveBeenCalledTimes(1)
    })

    it('does not retry a policy create on 503, because it may already have been created', async () => {
        const apiConfig = createTestConfig()
        const transport = stubTransport(apiConfig, [{ status: 503, data: { message: 'Service Unavailable' } }])

        await expect(new SODPoliciesApi(apiConfig).createSodPolicyV1({ sodPolicy: { name: 'P1' } })).rejects.toThrow(
            'HTTP 503 Service Unavailable: Service Unavailable'
        )
        expect(transport).toHaveBeenCalledTimes(1)
    })

    it('retries a 503 for read-only requests, including the Search POST', async () => {
        jest.useFakeTimers()
        const apiConfig = createTestConfig()
        const listTransport = stubTransport(apiConfig, [{ status: 503 }, { status: 200, data: [] }])

        const list = new SODPoliciesApi(apiConfig).listSodPoliciesV1()
        await jest.advanceTimersByTimeAsync(1000)
        await expect(list).resolves.toMatchObject({ data: [] })
        expect(listTransport).toHaveBeenCalledTimes(2)

        const searchTransport = stubTransport(apiConfig, [{ status: 503 }, { status: 200, data: [] }])
        const searchResult = new SearchApi(apiConfig).searchPostV1({ search })
        await jest.advanceTimersByTimeAsync(1000)
        await expect(searchResult).resolves.toMatchObject({ data: [] })
        expect(searchTransport).toHaveBeenCalledTimes(2)
    })

    it('stops retrying network errors after the transient retry limit', async () => {
        jest.useFakeTimers()
        const apiConfig = createTestConfig()
        const transport = stubTransport(apiConfig, [{ networkError: 'ECONNRESET' }])

        const assertion = expect(new SODPoliciesApi(apiConfig).listSodPoliciesV1()).rejects.toThrow(
            `ECONNRESET: socket hang up (gave up after ${API_5XX_MAX_RETRIES} retries)`
        )
        await jest.advanceTimersByTimeAsync(1000 + 2000 + 4000 + 8000 + 16000)
        await assertion
        expect(transport).toHaveBeenCalledTimes(API_5XX_MAX_RETRIES + 1)
    })

    it('never retries timeouts', async () => {
        const apiConfig = createTestConfig()
        const transport = stubTransport(apiConfig, [{ networkError: 'ECONNABORTED' }])

        await expect(new SODPoliciesApi(apiConfig).listSodPoliciesV1()).rejects.toThrow('ECONNABORTED: socket hang up')
        expect(transport).toHaveBeenCalledTimes(1)
    })

    it('holds other requests while a 429 cooldown is active', async () => {
        jest.useFakeTimers()
        const apiConfig = createTestConfig()
        const transport = stubTransport(apiConfig, [
            { ...rateLimited, headers: { 'retry-after': '5' } },
            { status: 200, data: [] },
        ])
        const searchApi = new SearchApi(apiConfig)

        const first = searchApi.searchPostV1({ search })
        await jest.advanceTimersByTimeAsync(10)
        expect(transport).toHaveBeenCalledTimes(1)

        const second = searchApi.searchPostV1({ search })
        await jest.advanceTimersByTimeAsync(4000)
        expect(transport).toHaveBeenCalledTimes(1)

        await jest.advanceTimersByTimeAsync(1000)
        await expect(Promise.all([first, second])).resolves.toHaveLength(2)
        expect(transport).toHaveBeenCalledTimes(3)
    })
})

describe('computeRetryDelayMs', () => {
    afterEach(() => jest.restoreAllMocks())

    it('grows exponentially and never undercuts Retry-After', () => {
        jest.spyOn(Math, 'random').mockReturnValue(0)
        expect(computeRetryDelayMs(1)).toBe(1000)
        expect(computeRetryDelayMs(3)).toBe(4000)
        expect(computeRetryDelayMs(1, 10000)).toBe(10000)
    })

    it('widens the jitter window with each attempt and caps the wait', () => {
        jest.spyOn(Math, 'random').mockReturnValue(0.999999)
        expect(computeRetryDelayMs(1)).toBe(3000)
        expect(computeRetryDelayMs(1, 10000)).toBe(12000)
        expect(computeRetryDelayMs(5, 10000)).toBe(42000)
        expect(computeRetryDelayMs(8, 10000)).toBe(60000)
        expect(computeRetryDelayMs(1, 300000)).toBe(60000)
    })
})
