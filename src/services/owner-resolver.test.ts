import { Configuration, Paginator } from '../types/sailpoint-api'
import { DtoType } from '../types/enums'
import * as apiHelper from '../utils/api-helper'
import { OwnerResolverService } from './owner-resolver'
import { SearchService } from './search-service'

describe('OwnerResolverService cache', () => {
    const apiConfig = {} as Configuration

    it('reuses cached owner lookups within the same aggregation run', async () => {
        const searchService = {
            searchIdentityByAttribute: jest.fn().mockResolvedValue({
                identity: { id: 'identity-1', name: 'owner', type: DtoType.Identity },
            }),
        } as unknown as SearchService

        const resolver = new OwnerResolverService(searchService, 'name')

        await (resolver as unknown as { resolveOwnerReference: Function }).resolveOwnerReference(
            apiConfig,
            DtoType.Identity,
            'owner'
        )
        await (resolver as unknown as { resolveOwnerReference: Function }).resolveOwnerReference(
            apiConfig,
            DtoType.Identity,
            'owner'
        )

        expect(searchService.searchIdentityByAttribute).toHaveBeenCalledTimes(1)
    })

    it('caches governance group member lookups', async () => {
        jest.spyOn(apiHelper, 'wrapApiCallResult').mockImplementation(async (fn) => ({ ok: true, data: await fn() }))
        const paginateSpy = jest.spyOn(Paginator, 'paginate').mockResolvedValue({
            data: [{ id: 'member-1', name: 'Member' }],
        } as never)

        const resolver = new OwnerResolverService({} as SearchService, 'name')
        const policyConfig = {
            violationOwnerType: DtoType.GovernanceGroup,
            violationOwner: 'Accounting',
        }
        const violationOwner = { id: 'group-1', name: 'Accounting', type: DtoType.GovernanceGroup }

        await resolver.resolvePolicyRecipients(apiConfig, policyConfig as never, violationOwner, undefined)
        await resolver.resolvePolicyRecipients(apiConfig, policyConfig as never, violationOwner, undefined)

        expect(paginateSpy).toHaveBeenCalledTimes(1)
    })

    it('clearCaches resets cached owner lookups', async () => {
        const searchService = {
            searchIdentityByAttribute: jest.fn().mockResolvedValue({
                identity: { id: 'identity-1', name: 'owner', type: DtoType.Identity },
            }),
        } as unknown as SearchService

        const resolver = new OwnerResolverService(searchService, 'name')

        await (resolver as unknown as { resolveOwnerReference: Function }).resolveOwnerReference(
            apiConfig,
            DtoType.Identity,
            'owner'
        )
        resolver.clearCaches()
        await (resolver as unknown as { resolveOwnerReference: Function }).resolveOwnerReference(
            apiConfig,
            DtoType.Identity,
            'owner'
        )

        expect(searchService.searchIdentityByAttribute).toHaveBeenCalledTimes(2)
    })

    it('surfaces API errors instead of treating them as missing owners', async () => {
        const searchService = {
            searchIdentityByAttribute: jest.fn().mockResolvedValue({
                error: 'Error finding identity using Search API: Request failed with status code 429',
            }),
        } as unknown as SearchService

        const resolver = new OwnerResolverService(searchService, 'name')
        const result = await resolver.resolvePolicyOwner(apiConfig, {
            policyOwnerType: DtoType.Identity,
            policyOwner: 'owner',
        } as never)

        expect(result.owner).toBeUndefined()
        expect(result.error).toContain('status code 429')
        expect(searchService.searchIdentityByAttribute).toHaveBeenCalledTimes(1)
    })
})
