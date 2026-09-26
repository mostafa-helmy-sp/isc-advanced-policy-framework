import { Configuration, Paginator, SourcesApi } from '../types/sailpoint-api'
import { EntitlementDocument } from '../types/search-documents'
import { EntitlementHierarchyService } from './entitlement-hierarchy'
import { SearchService } from './search-service'

jest.mock('../utils/logger', () => ({
    logger: { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() },
}))

function entitlement(id: string): EntitlementDocument {
    return { id, name: id, schema: 'group', source: { id: 'source-1', name: 'AD' } } as EntitlementDocument
}

describe('EntitlementHierarchyService', () => {
    afterEach(() => jest.restoreAllMocks())

    it('terminates on circular group nesting and returns each entitlement once', async () => {
        jest.spyOn(SourcesApi.prototype, 'getSourceSchemasV1').mockResolvedValue({
            data: [{ name: 'group', hierarchyAttribute: 'memberOf', configuration: {} }],
        } as never)
        const childrenById: Record<string, Array<{ id: string }>> = { A: [{ id: 'B' }], B: [{ id: 'A' }] }
        const listChildren = async (_api: unknown, _fn: unknown, args?: { id?: string }) => ({
            data: childrenById[args?.id ?? ''] ?? [],
        })
        const paginate = jest.spyOn(Paginator, 'paginate').mockImplementation(listChildren as never)
        const searchService = {
            searchEntitlementsByIds: jest.fn(async (_apiConfig: Configuration, ids: string[]) => ({
                items: ids.map(entitlement),
            })),
        } as unknown as SearchService

        const result = await new EntitlementHierarchyService(searchService).includeEntitlementHierarchy(
            {} as Configuration,
            [entitlement('A')]
        )

        expect(result.error).toBeUndefined()
        expect(result.items.map((item) => item.id).sort()).toEqual(['A', 'B'])
        expect(paginate).toHaveBeenCalledTimes(2)
    })
})
