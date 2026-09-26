import { buildBatchedIdQueries, chunkArray, deduplicateById, wrapApiCallResult } from './api-helper'

jest.mock('./logger', () => ({ logger: { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() } }))

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

describe('wrapApiCallResult', () => {
    it('returns the data on success', async () => {
        await expect(wrapApiCallResult(async () => ['a'], 'Error listing')).resolves.toEqual({ ok: true, data: ['a'] })
    })

    it('returns the failure message prefixed with the operation instead of throwing', async () => {
        const result = await wrapApiCallResult(async () => {
            throw new Error('HTTP 429 Too Many Requests: Rate Limit Exceeded (gave up after 15 retries)')
        }, 'Error finding entitlements using Search API')

        expect(result).toEqual({
            ok: false,
            error: 'Error finding entitlements using Search API: HTTP 429 Too Many Requests: Rate Limit Exceeded (gave up after 15 retries)',
        })
    })

    it('does not retry on its own', async () => {
        const fn = jest.fn().mockRejectedValue(new Error('boom'))
        await wrapApiCallResult(fn, 'Error')
        expect(fn).toHaveBeenCalledTimes(1)
    })
})
