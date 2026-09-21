import { buildBatchedIdQueries, chunkArray, deduplicateById, executeWithRetry, getRetryDelayMs, isRateLimitError } from './api-helper'
import { API_RETRY_BASE_DELAY_MS, API_RETRY_MAX_ATTEMPTS } from '../config/defaults'

describe('chunkArray', () => {
    it('splits items into fixed-size chunks', () => {
        expect(chunkArray(['a', 'b', 'c', 'd', 'e'], 2)).toEqual([['a', 'b'], ['c', 'd'], ['e']])
    })

    it('returns single chunk when size exceeds item count', () => {
        expect(chunkArray(['a', 'b'], 10)).toEqual([['a', 'b']])
    })
})

describe('buildBatchedIdQueries', () => {
    it('returns one query when ids fit in batch size', () => {
        expect(buildBatchedIdQueries(['id1', 'id2'], 50, 'id:', ' OR ')).toEqual(['id:id1 OR id:id2'])
    })

    it('returns multiple queries when ids exceed batch size', () => {
        expect(buildBatchedIdQueries(['id1', 'id2', 'id3'], 2, 'id:', ' OR ', '@entitlements(', ')')).toEqual([
            '@entitlements(id:id1 OR id:id2)',
            '@entitlements(id:id3)',
        ])
    })
})

describe('deduplicateById', () => {
    it('removes duplicate items by id', () => {
        expect(
            deduplicateById([
                { id: '1', name: 'first' },
                { id: '1', name: 'duplicate' },
                { id: '2', name: 'second' },
            ])
        ).toEqual([
            { id: '1', name: 'duplicate' },
            { id: '2', name: 'second' },
        ])
    })
})

describe('rate limit retry helpers', () => {
    it('detects HTTP 429 errors', () => {
        expect(isRateLimitError({ response: { status: 429 } })).toBe(true)
        expect(isRateLimitError({ response: { status: 400 } })).toBe(false)
        expect(isRateLimitError(new Error('boom'))).toBe(false)
    })

    it('honors Retry-After header when present', () => {
        expect(getRetryDelayMs({ response: { status: 429, headers: { 'retry-after': '3' } } }, 0)).toBe(3000)
    })

    it('falls back to exponential backoff without Retry-After', () => {
        expect(getRetryDelayMs({ response: { status: 429, headers: {} } }, 0)).toBe(API_RETRY_BASE_DELAY_MS)
        expect(getRetryDelayMs({ response: { status: 429, headers: {} } }, 2)).toBe(API_RETRY_BASE_DELAY_MS * 4)
    })

    it('retries on 429 then succeeds', async () => {
        let attempts = 0
        const result = await executeWithRetry(async () => {
            attempts += 1
            if (attempts < 3) {
                throw { response: { status: 429, headers: { 'retry-after': '0' } }, message: 'Request failed with status code 429' }
            }
            return 'ok'
        }, 'test retry')

        expect(result).toBe('ok')
        expect(attempts).toBe(3)
    })

    it('does not retry non-429 errors', async () => {
        let attempts = 0
        await expect(
            executeWithRetry(async () => {
                attempts += 1
                throw { response: { status: 400 }, message: 'Bad Request' }
            }, 'test no retry')
        ).rejects.toMatchObject({ response: { status: 400 } })
        expect(attempts).toBe(1)
    })

    it('stops after max retry attempts', async () => {
        let attempts = 0
        await expect(
            executeWithRetry(async () => {
                attempts += 1
                throw { response: { status: 429, headers: { 'retry-after': '0' } }, message: 'Request failed with status code 429' }
            }, 'test max retries')
        ).rejects.toMatchObject({ response: { status: 429 } })
        expect(attempts).toBe(API_RETRY_MAX_ATTEMPTS + 1)
    })
})
