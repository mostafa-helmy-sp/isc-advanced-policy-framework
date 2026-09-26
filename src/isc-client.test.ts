import { IscClient } from './isc-client'
import { PolicyConfig } from './model/policy-config'
import { CampaignService } from './services/campaign-service'
import { OwnerResolverService } from './services/owner-resolver'
import { SearchService } from './services/search-service'
import { SodPolicyService } from './services/sod-policy-service'
import { DtoType } from './types/enums'
import { logger } from './utils/logger'

jest.mock('./api/client-factory', () => ({ createApiConfig: jest.fn(() => ({})) }))
jest.mock('./utils/logger', () => ({
    logger: { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() },
    runWithPolicyLogger: (_policyName: string, fn: () => Promise<unknown>) => fn(),
}))

const rateLimitError =
    'Error finding entitlements using Search API: HTTP 429 Too Many Requests: Rate Limit Exceeded (gave up after 15 retries)'

const policyConfig = new PolicyConfig({
    attributes: {
        PolicyName: 'P1',
        PolicyType: 'SOD',
        PolicyOwnerType: 'IDENTITY',
        PolicyOwner: 'owner',
        Query1: 'id:"e1"',
        Query2: 'id:"e2"',
        ViolationOwnerType: 'MANAGER',
    },
})

function createClient(): IscClient {
    return new IscClient({
        apiUrl: 'https://tenant.api.identitynow.com',
        clientId: 'id',
        clientSecret: 'secret',
        policyConfigSourceName: 'Policies',
    })
}

function errorMessagesOf(result: { attributes: Record<string, unknown> }): string[] {
    return JSON.parse(result.attributes.errorMessages as string)
}

describe('IscClient.processSodPolicyConfig error reporting', () => {
    beforeEach(() => {
        jest.spyOn(OwnerResolverService.prototype, 'resolvePolicyOwner').mockResolvedValue({
            owner: { id: 'o1', name: 'owner', type: DtoType.Identity },
        })
        jest.spyOn(OwnerResolverService.prototype, 'resolveViolationOwner').mockResolvedValue({})
        jest.spyOn(OwnerResolverService.prototype, 'resolveCoOwners').mockResolvedValue({ refs: [], errors: [] })
    })

    afterEach(() => jest.restoreAllMocks())

    it('reports the real search failure, labelled by query, instead of "returns no entitlements"', async () => {
        jest.spyOn(SearchService.prototype, 'searchEntitlementsByQuery')
            .mockResolvedValueOnce({ items: [], error: rateLimitError })
            .mockResolvedValueOnce({ items: [{ id: 'e2', name: 'E2' }] })

        const result = await createClient().processSodPolicyConfig(policyConfig)

        expect(errorMessagesOf(result)).toEqual([`Entitlement Query 1 [id:"e1"]: ${rateLimitError}`])
        expect(result.attributes.policyConfigured).toBe(false)
        expect(logger.warn).toHaveBeenCalledWith(
            { errorMessages: [`Entitlement Query 1 [id:"e1"]: ${rateLimitError}`] },
            '### Finished processing policy [P1] with 1 error(s) ###'
        )
    })

    it('reports "returns no entitlements" only for a search that succeeded with zero results', async () => {
        jest.spyOn(SearchService.prototype, 'searchEntitlementsByQuery').mockResolvedValue({ items: [] })

        const result = await createClient().processSodPolicyConfig(policyConfig)

        expect(errorMessagesOf(result)).toEqual([
            'Entitlement Query 1 [id:"e1"] returns no entitlements',
            'Entitlement Query 2 [id:"e2"] returns no entitlements',
        ])
    })

    it('records an unexpected exception on the policy instead of aborting the aggregation', async () => {
        jest.spyOn(SearchService.prototype, 'searchEntitlementsByQuery').mockRejectedValue(new Error('boom'))

        const result = await createClient().processSodPolicyConfig(policyConfig)

        expect(errorMessagesOf(result)).toEqual(['Unexpected error while processing policy: boom'])
    })
})

describe('IscClient.processSodPolicyConfig workflow', () => {
    const owner = { id: 'o1', name: 'owner', type: DtoType.Identity }
    const fullPolicyConfig = new PolicyConfig({
        attributes: {
            PolicyName: 'P1',
            PolicyType: 'SOD',
            PolicyOwnerType: 'IDENTITY',
            PolicyOwner: 'owner',
            Query1: 'id:"e1"',
            Query2: 'id:"e2"',
            ViolationOwnerType: 'MANAGER',
            Actions: 'REPORT,CERTIFY',
            PolicySchedule: 'WEEKLY',
            CertificationName: 'Campaign',
            CertificationDescription: 'Desc',
            CertificationSchedule: 'MONTHLY',
        },
    })

    beforeEach(() => {
        jest.spyOn(OwnerResolverService.prototype, 'resolvePolicyOwner').mockResolvedValue({ owner })
        jest.spyOn(OwnerResolverService.prototype, 'resolveViolationOwner').mockResolvedValue({})
        jest.spyOn(OwnerResolverService.prototype, 'resolveCoOwners').mockResolvedValue({ refs: [], errors: [] })
        jest.spyOn(OwnerResolverService.prototype, 'resolvePolicyRecipients').mockResolvedValue({ recipients: [owner] })
        jest.spyOn(SearchService.prototype, 'searchEntitlementsByQuery')
            .mockResolvedValueOnce({ items: [{ id: 'e1', name: 'E1', schema: 'group', source: { name: 'AD' } }] })
            .mockResolvedValueOnce({ items: [{ id: 'e2', name: 'E2', schema: 'group', source: { name: 'AD' } }] })
        jest.spyOn(SearchService.prototype, 'searchAccessProfilesByEntitlements').mockResolvedValue({
            items: [{ id: 'ap1', name: 'AP1' }],
        })
        jest.spyOn(SearchService.prototype, 'searchRolesByAccessProfilesOrEntitlements').mockResolvedValue({
            items: [],
        })
        jest.spyOn(SodPolicyService.prototype, 'findExistingPolicy').mockResolvedValue({})
        jest.spyOn(SodPolicyService.prototype, 'createPolicy').mockResolvedValue(['', 'policy-1', 'query-1'])
        jest.spyOn(SodPolicyService.prototype, 'setPolicySchedule').mockResolvedValue('')
        jest.spyOn(CampaignService.prototype, 'findExistingCampaign').mockResolvedValue({})
        jest.spyOn(CampaignService.prototype, 'createPolicyCampaign').mockResolvedValue(['', 'campaign-1'])
        jest.spyOn(CampaignService.prototype, 'setCampaignSchedule').mockResolvedValue('')
    })

    afterEach(() => jest.restoreAllMocks())

    it('creates the policy, its report schedule, and its campaign', async () => {
        const result = await createClient().processSodPolicyConfig(fullPolicyConfig)

        expect(errorMessagesOf(result)).toEqual([])
        expect(result.attributes).toMatchObject({
            policyQuery: 'query-1',
            policyConfigured: true,
            policyScheduleConfigured: true,
            campaignConfigured: true,
            campaignScheduleConfigured: true,
            leftHandEntitlementCount: 1,
            rightHandEntitlementCount: 1,
        })
        expect(logger.info).toHaveBeenCalledWith('### Finished processing policy [P1] ###')
    })

    it('labels an access profile lookup failure with the query side it belongs to', async () => {
        jest.spyOn(SearchService.prototype, 'searchAccessProfilesByEntitlements')
            .mockResolvedValueOnce({ items: [], error: 'Error finding access profiles using Search API: HTTP 429' })
            .mockResolvedValueOnce({ items: [] })

        const result = await createClient().processSodPolicyConfig(fullPolicyConfig)

        expect(errorMessagesOf(result)).toEqual([
            'Query 1 access profile lookup: Error finding access profiles using Search API: HTTP 429',
        ])
        expect(result.attributes.policyConfigured).toBe(true)
    })

    it('reports a failed policy create with the API error', async () => {
        jest.spyOn(SodPolicyService.prototype, 'createPolicy').mockResolvedValue([
            'Error creating a new Policy using SOD-Policies API: HTTP 400 Bad Request: Invalid owner',
            '',
            '',
        ])

        const result = await createClient().processSodPolicyConfig(fullPolicyConfig)

        expect(errorMessagesOf(result)).toEqual([
            'Error creating a new Policy using SOD-Policies API: HTTP 400 Bad Request: Invalid owner',
        ])
        expect(result.attributes.policyConfigured).toBe(false)
    })

    it('deletes the policy and its campaign for DELETE_ALL', async () => {
        jest.spyOn(SodPolicyService.prototype, 'findExistingPolicy').mockResolvedValue({ policy: { id: 'policy-1' } })
        jest.spyOn(SodPolicyService.prototype, 'deletePolicy').mockResolvedValue('')
        jest.spyOn(CampaignService.prototype, 'findExistingCampaign').mockResolvedValue({
            campaign: { id: 'campaign-1' },
        } as never)
        jest.spyOn(CampaignService.prototype, 'deletePolicyCampaign').mockResolvedValue('')
        const deleteConfig = new PolicyConfig({
            attributes: { PolicyName: 'P1', PolicyType: 'SOD', Actions: 'DELETE_ALL', CertificationName: 'Campaign' },
        })

        const result = await createClient().processSodPolicyConfig(deleteConfig)

        expect(errorMessagesOf(result)).toEqual([])
        expect(result.attributes).toMatchObject({ policyDeleted: true, campaignDeleted: true })
    })
})
