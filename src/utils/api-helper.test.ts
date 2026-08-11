import { buildBatchedIdQueries, chunkArray, deduplicateById } from './api-helper'

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
