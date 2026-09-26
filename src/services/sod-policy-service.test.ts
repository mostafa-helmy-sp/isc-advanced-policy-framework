import { PolicyConfig } from '../model/policy-config'
import { Configuration, SODPoliciesApi, SodPolicyLevelEnum } from '../types/sailpoint-api'
import { SodPolicyService } from './sod-policy-service'

jest.mock('../utils/logger', () => ({
    logger: { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() },
}))

const apiConfig = {} as Configuration
const policyConfig = new PolicyConfig({ attributes: { PolicyName: 'P1', PolicyType: 'SOD' } })
const owner = { id: 'o1', name: 'owner', type: 'IDENTITY' } as never
const violationOwner = {} as never
const criteria = {} as never

function createPolicy() {
    return new SodPolicyService().createPolicy(
        apiConfig,
        policyConfig,
        owner,
        violationOwner,
        criteria,
        SodPolicyLevelEnum.High,
        []
    )
}

function updatePolicy() {
    return new SodPolicyService().updatePolicy(
        apiConfig,
        'p1',
        policyConfig,
        owner,
        violationOwner,
        criteria,
        SodPolicyLevelEnum.High,
        []
    )
}

describe('SodPolicyService', () => {
    afterEach(() => jest.restoreAllMocks())

    it('returns the id and query of a created policy', async () => {
        jest.spyOn(SODPoliciesApi.prototype, 'createSodPolicyV1').mockResolvedValue({
            data: { id: 'p1', policyQuery: 'q1' },
        } as never)

        await expect(createPolicy()).resolves.toEqual(['', 'p1', 'q1'])
    })

    it('returns the API error when a create fails', async () => {
        jest.spyOn(SODPoliciesApi.prototype, 'createSodPolicyV1').mockRejectedValue(
            new Error('HTTP 400 Bad Request: Invalid owner')
        )

        await expect(createPolicy()).resolves.toEqual([
            'Error creating a new Policy using SOD-Policies API: HTTP 400 Bad Request: Invalid owner',
            '',
            '',
        ])
    })

    it('returns the updated policy query, or the API error when an update fails', async () => {
        const patch = jest.spyOn(SODPoliciesApi.prototype, 'patchSodPolicyV1')
        patch.mockResolvedValueOnce({ data: { policyQuery: 'q1' } } as never)
        await expect(updatePolicy()).resolves.toEqual(['', 'q1'])

        patch.mockRejectedValueOnce(new Error('HTTP 404 Not Found: Policy not found'))
        await expect(updatePolicy()).resolves.toEqual([
            'Error updating existing Policy using SOD-Policies API: HTTP 404 Not Found: Policy not found',
            '',
        ])
    })

    it('reports a failed lookup instead of treating it as "no existing policy"', async () => {
        jest.spyOn(SODPoliciesApi.prototype, 'listSodPoliciesV1').mockRejectedValue(
            new Error('HTTP 429 Too Many Requests: Rate Limit Exceeded (gave up after 15 retries)')
        )

        await expect(new SodPolicyService().findExistingPolicy(apiConfig, policyConfig)).resolves.toEqual({
            error: 'Error finding existing Policy using SOD-Policies API: HTTP 429 Too Many Requests: Rate Limit Exceeded (gave up after 15 retries)',
        })
    })
})
