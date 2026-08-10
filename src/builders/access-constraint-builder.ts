import {
    AccessConstraint,
    AccessConstraintOperatorEnum,
    AccessConstraintTypeEnum,
    AccessCriteriaCriteriaListInner,
    AccessCriteriaCriteriaListInnerTypeEnum,
    SodPolicyConflictingAccessCriteria,
    ViolationOwnerAssignmentConfig,
    ViolationOwnerAssignmentConfigAssignmentRuleEnum,
} from '../types/sailpoint-api'
import { PolicyConfig } from '../model/policy-config'
import { EntitlementDocument, AccessProfileDocument, RoleDocument } from '../types/search-documents'
import { OwnerReference } from '../types/search-documents'
import { buildIdArray, mergeUnique } from '../utils/api-helper'

export function buildConflictingAccessCriteriaList(items: EntitlementDocument[]): AccessCriteriaCriteriaListInner[] {
    return items.map((item) => ({
        id: item.id,
        type: AccessCriteriaCriteriaListInnerTypeEnum.Entitlement,
        name: item.name
    }))
}

export function buildPolicyConflictingAccessCriteria(
    policyConfig: PolicyConfig,
    query1Entitlements: EntitlementDocument[],
    query2Entitlements: EntitlementDocument[]
): SodPolicyConflictingAccessCriteria {
    return {
        leftCriteria: {
            name: policyConfig.query1Name,
            criteriaList: buildConflictingAccessCriteriaList(query1Entitlements),
        },
        rightCriteria: {
            name: policyConfig.query2Name,
            criteriaList: buildConflictingAccessCriteriaList(query2Entitlements),
        },
    }
}

export function buildCampaignAccessConstraints(
    entitlements1: EntitlementDocument[],
    entitlements2: EntitlementDocument[],
    accessProfiles1: AccessProfileDocument[],
    accessProfiles2: AccessProfileDocument[],
    roles1: RoleDocument[],
    roles2: RoleDocument[]
): [accessConstraints: AccessConstraint[], leftHandTotalCount: number, rightHandTotalCount: number, totalCount: number] {
    const accessConstraints: AccessConstraint[] = []
    const entitlement1Ids = buildIdArray(entitlements1)
    const entitlement2Ids = buildIdArray(entitlements2)
    const accessProfile1Ids = buildIdArray(accessProfiles1)
    const accessProfile2Ids = buildIdArray(accessProfiles2)
    const role1Ids = buildIdArray(roles1)
    const role2Ids = buildIdArray(roles2)

    const entitlementIds = mergeUnique(entitlement1Ids, entitlement2Ids)
    const accessProfileIds = mergeUnique(accessProfile1Ids, accessProfile2Ids)
    const roleIds = mergeUnique(role1Ids, role2Ids)

    if (entitlementIds.length > 0) {
        accessConstraints.push({
            type: AccessConstraintTypeEnum.Entitlement,
            ids: entitlementIds,
            operator: AccessConstraintOperatorEnum.Selected,
        })
    }
    if (accessProfileIds.length > 0) {
        accessConstraints.push({
            type: AccessConstraintTypeEnum.AccessProfile,
            ids: accessProfileIds,
            operator: AccessConstraintOperatorEnum.Selected,
        })
    }
    if (roleIds.length > 0) {
        accessConstraints.push({
            type: AccessConstraintTypeEnum.Role,
            ids: roleIds,
            operator: AccessConstraintOperatorEnum.Selected,
        })
    }

    const leftHandTotalCount = entitlement1Ids.length + accessProfile1Ids.length + role1Ids.length
    const rightHandTotalCount = entitlement2Ids.length + accessProfile2Ids.length + role2Ids.length
    const totalCount = entitlementIds.length + accessProfileIds.length + roleIds.length
    return [accessConstraints, leftHandTotalCount, rightHandTotalCount, totalCount]
}

export function buildViolationOwnerAssignmentConfig(violationOwner: OwnerReference | undefined): ViolationOwnerAssignmentConfig {
    if (violationOwner) {
        return {
            assignmentRule: ViolationOwnerAssignmentConfigAssignmentRuleEnum.Static,
            ownerRef: violationOwner as ViolationOwnerAssignmentConfig['ownerRef'],
        }
    }
    return {
        assignmentRule: ViolationOwnerAssignmentConfigAssignmentRuleEnum.Manager,
    }
}
