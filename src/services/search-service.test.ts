import { Configuration, Paginator } from '../types/sailpoint-api'
import { DtoType } from '../types/enums'
import { SearchService } from './search-service'

jest.mock('../utils/logger', () => ({
    logger: { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() },
}))

const apiConfig = {} as Configuration
const rateLimitError = 'HTTP 429 Too Many Requests: Rate Limit Exceeded (gave up after 15 retries)'

describe('SearchService', () => {
    afterEach(() => jest.restoreAllMocks())

    it('returns the entitlements matched by a query', async () => {
        const paginate = jest.spyOn(Paginator, 'paginateSearchApi').mockResolvedValue({ data: [{ id: 'e1' }] } as never)

        await expect(new SearchService().searchEntitlementsByQuery(apiConfig, 'id:"e1"')).resolves.toEqual({
            items: [{ id: 'e1' }],
        })
        expect(paginate).toHaveBeenCalledWith(
            expect.anything(),
            expect.objectContaining({ query: { query: 'id:"e1"' }, sort: ['id'] })
        )
    })

    it('returns the search error instead of an empty result', async () => {
        jest.spyOn(Paginator, 'paginateSearchApi').mockRejectedValue(new Error(rateLimitError))

        await expect(new SearchService().searchEntitlementsByQuery(apiConfig, 'id:"e1"')).resolves.toEqual({
            items: [],
            error: `Error finding entitlements using Search API: ${rateLimitError}`,
        })
    })

    it('batches id lookups and reports a failed batch', async () => {
        const ids = Array.from({ length: 60 }, (_, index) => `e${index}`)
        const paginate = jest
            .spyOn(Paginator, 'paginateSearchApi')
            .mockResolvedValueOnce({ data: [{ id: 'e0' }] } as never)
            .mockRejectedValueOnce(new Error(rateLimitError))

        await expect(new SearchService().searchEntitlementsByIds(apiConfig, ids)).resolves.toEqual({
            items: [],
            error: `Error finding entitlements using Search API: ${rateLimitError}`,
        })
        expect(paginate).toHaveBeenCalledTimes(2)
    })

    it('resolves an identity by an exact attribute match', async () => {
        const paginate = jest
            .spyOn(Paginator, 'paginateSearchApi')
            .mockResolvedValue({ data: [{ id: 'i1', name: 'owner' }] } as never)

        await expect(new SearchService().searchIdentityByAttribute(apiConfig, 'name', 'owner')).resolves.toEqual({
            identity: { id: 'i1', name: 'owner', type: DtoType.Identity },
        })
        expect(paginate).toHaveBeenCalledWith(
            expect.anything(),
            expect.objectContaining({ query: { query: 'name.exact:"owner"' } })
        )
    })
})
