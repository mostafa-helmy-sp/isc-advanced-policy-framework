import {
    Configuration,
    JsonPatchOperationOpEnum,
    Schedule,
    SodPolicy,
    SodPolicyConflictingAccessCriteria,
    SodPolicyLevelEnum,
    SodPolicyOwnerRef,
    SodPolicySecondaryOwnerRefsInner,
    SodPolicyStateEnum,
    SodPolicyTypeEnum,
    SodRecipient,
    SODPoliciesApi,
    ViolationOwnerAssignmentConfig,
} from '../types/sailpoint-api'
import { PolicyConfig } from '../model/policy-config'
import { wrapApiCall, wrapApiMutation } from '../utils/api-helper'

export class SodPolicyService {
    async findExistingPolicy(apiConfig: Configuration, policyConfig: PolicyConfig): Promise<SodPolicy | undefined> {
        const filter = `name eq "${policyConfig.policyName}"`
        const policyApi = new SODPoliciesApi(apiConfig)
        const request = { filters: filter }

        const existingPolicy = await wrapApiCall(
            () => policyApi.listSodPoliciesV1(request).then((r) => r.data),
            'Error finding existing Policy using SOD-Policies API',
            request
        )

        if (!existingPolicy || existingPolicy.length === 0 || !existingPolicy[0].id) {
            return undefined
        }
        return existingPolicy[0]
    }

    async deletePolicy(apiConfig: Configuration, policyId: string): Promise<string> {
        const policyApi = new SODPoliciesApi(apiConfig)
        const request = { id: policyId }
        return wrapApiMutation(
            () => policyApi.deleteSodPolicyV1(request).then(() => undefined),
            'Error deleting existing policy using SOD-Policies API',
            request
        )
    }

    async createPolicy(
        apiConfig: Configuration,
        policyConfig: PolicyConfig,
        policyOwner: SodPolicyOwnerRef,
        violationOwner: ViolationOwnerAssignmentConfig,
        conflictingAccessCriteria: SodPolicyConflictingAccessCriteria,
        level: SodPolicyLevelEnum,
        secondaryOwnerRefs: SodPolicySecondaryOwnerRefsInner[]
    ): Promise<[errorMessage: string, policyId: string, policyQuery: string]> {
        const policyApi = new SODPoliciesApi(apiConfig)
        const policyState = policyConfig.policyState ? SodPolicyStateEnum.Enforced : SodPolicyStateEnum.NotEnforced
        const request = {
            sodPolicy: {
                name: policyConfig.policyName,
                description: policyConfig.policyDescription,
                ownerRef: policyOwner,
                secondaryOwnerRefs,
                level,
                externalPolicyReference: policyConfig.externalReference,
                compensatingControls: policyConfig.mitigatingControls,
                correctionAdvice: policyConfig.correctionAdvice,
                state: policyState,
                tags: policyConfig.tags,
                violationOwnerAssignmentConfig: violationOwner,
                type: SodPolicyTypeEnum.ConflictingAccessBased,
                conflictingAccessCriteria,
            },
        }

        try {
            const newPolicy = await policyApi.createSodPolicyV1(request)
            return ['', newPolicy.data.id ?? '', newPolicy.data.policyQuery ?? '']
        } catch (error) {
            const errorMessage = `Error creating a new Policy using SOD-Policies API: ${error instanceof Error ? error.message : error}`
            return [errorMessage, '', '']
        }
    }

    async updatePolicy(
        apiConfig: Configuration,
        existingPolicyId: string,
        policyConfig: PolicyConfig,
        policyOwner: SodPolicyOwnerRef,
        violationOwner: ViolationOwnerAssignmentConfig,
        conflictingAccessCriteria: SodPolicyConflictingAccessCriteria,
        level: SodPolicyLevelEnum,
        secondaryOwnerRefs: SodPolicySecondaryOwnerRefsInner[]
    ): Promise<[errorMessage: string, policyQuery: string]> {
        const policyApi = new SODPoliciesApi(apiConfig)
        const policyState = policyConfig.policyState ? SodPolicyStateEnum.Enforced : SodPolicyStateEnum.NotEnforced
        const request = {
            id: existingPolicyId,
            jsonPatchOperation: [
                { op: JsonPatchOperationOpEnum.Replace, path: '/name', value: policyConfig.policyName },
                { op: JsonPatchOperationOpEnum.Replace, path: '/description', value: policyConfig.policyDescription },
                { op: JsonPatchOperationOpEnum.Replace, path: '/ownerRef', value: policyOwner },
                { op: JsonPatchOperationOpEnum.Replace, path: '/secondaryOwnerRefs', value: secondaryOwnerRefs },
                { op: JsonPatchOperationOpEnum.Replace, path: '/level', value: level },
                { op: JsonPatchOperationOpEnum.Replace, path: '/externalPolicyReference', value: policyConfig.externalReference },
                { op: JsonPatchOperationOpEnum.Replace, path: '/compensatingControls', value: policyConfig.mitigatingControls },
                { op: JsonPatchOperationOpEnum.Replace, path: '/correctionAdvice', value: policyConfig.correctionAdvice },
                { op: JsonPatchOperationOpEnum.Replace, path: '/state', value: policyState },
                { op: JsonPatchOperationOpEnum.Replace, path: '/tags', value: policyConfig.tags },
                { op: JsonPatchOperationOpEnum.Replace, path: '/violationOwnerAssignmentConfig', value: violationOwner },
                { op: JsonPatchOperationOpEnum.Replace, path: '/conflictingAccessCriteria', value: conflictingAccessCriteria },
            ],
        }

        try {
            const patchedPolicy = await policyApi.patchSodPolicyV1(request)
            return ['', patchedPolicy.data.policyQuery ?? '']
        } catch (error) {
            const errorMessage = `Error updating existing Policy using SOD-Policies API: ${error instanceof Error ? error.message : error}`
            return [errorMessage, '']
        }
    }

    async setPolicySchedule(
        apiConfig: Configuration,
        policyId: string,
        policyConfig: PolicyConfig,
        policySchedule: Schedule,
        policyRecipients: SodRecipient[]
    ): Promise<string> {
        const policyApi = new SODPoliciesApi(apiConfig)
        const request = {
            id: policyId,
            sodPolicySchedule: {
                name: `${policyConfig.policySchedule}: ${policyConfig.policyName}`,
                description: policyConfig.policyDescription,
                schedule: policySchedule,
                recipients: policyRecipients,
            },
        }
        return wrapApiMutation(
            () => policyApi.putPolicyScheduleV1(request).then(() => undefined),
            'Error setting Policy Schedule using SOD-Policies API',
            request
        )
    }
}
