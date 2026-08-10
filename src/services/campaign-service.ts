import {
    AccessConstraint,
    Campaign2AllOfSearchCampaignInfoReviewer,
    Campaign2AllOfSearchCampaignInfoTypeEnum,
    Campaign2CorrelatedStatusEnum,
    Campaign2TypeEnum,
    CampaignJsonPatchOperationOpEnum,
    CampaignTemplate,
    CertificationCampaignsApi,
    Configuration,
    Schedule2,
    ViolationOwnerAssignmentConfigAssignmentRuleEnum,
} from '../types/sailpoint-api'
import { PolicyConfig } from '../model/policy-config'
import { wrapApiCall, wrapApiMutation } from '../utils/api-helper'

export class CampaignService {
    async findExistingCampaign(apiConfig: Configuration, policyConfig: PolicyConfig): Promise<CampaignTemplate | undefined> {
        const filter = `name eq "${policyConfig.certificationName}"`
        const certsApi = new CertificationCampaignsApi(apiConfig)
        const request = { filters: filter }

        const existingCampaign = await wrapApiCall(
            () => certsApi.getCampaignTemplatesV1(request).then((r) => r.data),
            'Error finding existing Campaign using Certification-Campaigns API',
            request
        )

        if (!existingCampaign || existingCampaign.length === 0 || !existingCampaign[0].id) {
            return undefined
        }
        return existingCampaign[0]
    }

    async deletePolicyCampaign(apiConfig: Configuration, campaignId: string): Promise<string> {
        const certsApi = new CertificationCampaignsApi(apiConfig)
        const request = { id: campaignId }
        return wrapApiMutation(
            () => certsApi.deleteCampaignTemplateV1(request).then(() => undefined),
            'Error deleting existing campaign using Certification-Campaigns API',
            request
        )
    }

    async createPolicyCampaign(
        apiConfig: Configuration,
        policyConfig: PolicyConfig,
        policyQuery: string,
        accessConstraints: AccessConstraint[],
        violationOwner: Campaign2AllOfSearchCampaignInfoReviewer | undefined,
        campaignDuration: string
    ): Promise<[errorMessage: string, campaignId: string]> {
        const certsApi = new CertificationCampaignsApi(apiConfig)
        const reviewer =
            policyConfig.violationOwnerType !== ViolationOwnerAssignmentConfigAssignmentRuleEnum.Manager
                ? violationOwner
                : undefined

        const request = {
            campaignTemplate: {
                name: policyConfig.certificationName,
                description: policyConfig.certificationDescription,
                deadlineDuration: campaignDuration,
                created: '',
                modified: null,
                campaign: {
                    name: policyConfig.certificationName,
                    description: policyConfig.certificationDescription,
                    type: Campaign2TypeEnum.Search,
                    correlatedStatus: Campaign2CorrelatedStatusEnum.Correlated,
                    recommendationsEnabled: true,
                    emailNotificationEnabled: true,
                    sunsetCommentsRequired: true,
                    searchCampaignInfo: {
                        type: Campaign2AllOfSearchCampaignInfoTypeEnum.Identity,
                        description: policyConfig.certificationDescription,
                        reviewer,
                        query: policyQuery,
                        accessConstraints,
                    },
                },
            },
        }

        try {
            const newCampaign = await certsApi.createCampaignTemplateV1(request)
            return ['', newCampaign.data.id ?? '']
        } catch (error) {
            const errorMessage = `Error creating new Campaign using Certification-Campaigns API: ${error instanceof Error ? error.message : error}`
            return [errorMessage, '']
        }
    }

    async updatePolicyCampaign(
        apiConfig: Configuration,
        campaignId: string,
        policyConfig: PolicyConfig,
        policyQuery: string,
        accessConstraints: AccessConstraint[],
        violationOwner: Campaign2AllOfSearchCampaignInfoReviewer | undefined,
        campaignDuration: string
    ): Promise<string> {
        const certsApi = new CertificationCampaignsApi(apiConfig)
        const reviewer =
            policyConfig.violationOwnerType !== ViolationOwnerAssignmentConfigAssignmentRuleEnum.Manager
                ? violationOwner
                : undefined

        const request = {
            id: campaignId,
            jsonPatchOperation: [
                { op: CampaignJsonPatchOperationOpEnum.Replace, path: '/name', value: policyConfig.certificationName },
                { op: CampaignJsonPatchOperationOpEnum.Replace, path: '/description', value: policyConfig.certificationDescription },
                { op: CampaignJsonPatchOperationOpEnum.Replace, path: '/deadlineDuration', value: campaignDuration },
                { op: CampaignJsonPatchOperationOpEnum.Replace, path: '/campaign/name', value: policyConfig.certificationName },
                { op: CampaignJsonPatchOperationOpEnum.Replace, path: '/campaign/description', value: policyConfig.certificationDescription },
                {
                    op: CampaignJsonPatchOperationOpEnum.Replace,
                    path: '/campaign/searchCampaignInfo/description',
                    value: policyConfig.certificationDescription,
                },
                { op: CampaignJsonPatchOperationOpEnum.Replace, path: '/campaign/searchCampaignInfo/reviewer', value: reviewer },
                { op: CampaignJsonPatchOperationOpEnum.Replace, path: '/campaign/searchCampaignInfo/query', value: policyQuery },
                {
                    op: CampaignJsonPatchOperationOpEnum.Replace,
                    path: '/campaign/searchCampaignInfo/accessConstraints',
                    value: accessConstraints,
                },
            ],
        }

        return wrapApiMutation(
            () => certsApi.patchCampaignTemplateV1(request).then(() => undefined),
            'Error updating existing Campaign using Certification-Campaigns API',
            request
        )
    }

    async setCampaignSchedule(apiConfig: Configuration, campaignId: string, campaignSchedule: Schedule2): Promise<string> {
        const certsApi = new CertificationCampaignsApi(apiConfig)
        const request = { id: campaignId, schedule2: campaignSchedule }
        return wrapApiMutation(
            () => certsApi.setCampaignTemplateScheduleV1(request).then(() => undefined),
            'Error setting campaign schedule using Certification-Campaigns API',
            request
        )
    }
}
