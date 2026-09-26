import { PolicyConfig } from '../model/policy-config'
import { CertificationCampaignsApi, Configuration } from '../types/sailpoint-api'
import { CampaignService } from './campaign-service'

jest.mock('../utils/logger', () => ({
    logger: { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() },
}))

const apiConfig = {} as Configuration
const policyConfig = new PolicyConfig({
    attributes: {
        PolicyName: 'P1',
        PolicyType: 'SOD',
        ViolationOwnerType: 'MANAGER',
        CertificationName: 'Campaign',
        CertificationDescription: 'Desc',
    },
})

function createCampaign() {
    return new CampaignService().createPolicyCampaign(apiConfig, policyConfig, 'query-1', [], undefined, 'P2W')
}

describe('CampaignService', () => {
    afterEach(() => jest.restoreAllMocks())

    it('returns the id of a created campaign', async () => {
        jest.spyOn(CertificationCampaignsApi.prototype, 'createCampaignTemplateV1').mockResolvedValue({
            data: { id: 'c1' },
        } as never)

        await expect(createCampaign()).resolves.toEqual(['', 'c1'])
    })

    it('returns the API error when a create fails', async () => {
        jest.spyOn(CertificationCampaignsApi.prototype, 'createCampaignTemplateV1').mockRejectedValue(
            new Error('HTTP 400 Bad Request: Invalid reviewer')
        )

        await expect(createCampaign()).resolves.toEqual([
            'Error creating new Campaign using Certification-Campaigns API: HTTP 400 Bad Request: Invalid reviewer',
            '',
        ])
    })

    it('returns the API error when an update fails', async () => {
        jest.spyOn(CertificationCampaignsApi.prototype, 'patchCampaignTemplateV1').mockRejectedValue(
            new Error('HTTP 429 Too Many Requests: Rate Limit Exceeded (gave up after 15 retries)')
        )

        await expect(
            new CampaignService().updatePolicyCampaign(apiConfig, 'c1', policyConfig, 'query-1', [], undefined, 'P2W')
        ).resolves.toBe(
            'Error updating existing Campaign using Certification-Campaigns API: HTTP 429 Too Many Requests: Rate Limit Exceeded (gave up after 15 retries)'
        )
    })

    it('reports a failed lookup instead of treating it as "no existing campaign"', async () => {
        jest.spyOn(CertificationCampaignsApi.prototype, 'getCampaignTemplatesV1').mockRejectedValue(
            new Error('HTTP 503 Service Unavailable: down (gave up after 5 retries)')
        )

        await expect(new CampaignService().findExistingCampaign(apiConfig, policyConfig)).resolves.toEqual({
            error: 'Error finding existing Campaign using Certification-Campaigns API: HTTP 503 Service Unavailable: down (gave up after 5 retries)',
        })
    })
})
