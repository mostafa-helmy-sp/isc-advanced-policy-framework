/**
 * Re-exports and local aliases for sailpoint-api-client v2 types used by this connector.
 * Import runtime values from the main package; use type-only imports here to avoid
 * bundling SDK source files during ncc build.
 */
export {
    AccountsApi,
    CertificationCampaignsApi,
    Configuration,
    ConfigurationParameters,
    EntitlementsApi,
    GovernanceGroupsApi,
    Paginator,
    SearchApi,
    SODPoliciesApi,
    SourcesApi,
} from 'sailpoint-api-client'

export type { Account } from 'sailpoint-api-client/dist/accounts/api'
export type {
    AccessConstraint,
    Campaign2AllOfSearchCampaignInfoReviewer,
    CampaignTemplate,
    Schedule2,
} from 'sailpoint-api-client/dist/certification_campaigns/api'
export {
    AccessConstraintOperatorEnum,
    AccessConstraintTypeEnum,
    Campaign2AllOfSearchCampaignInfoTypeEnum,
    Campaign2CorrelatedStatusEnum,
    Campaign2TypeEnum,
    JsonPatchOperationOpEnum as CampaignJsonPatchOperationOpEnum,
    Schedule2DaysTypeEnum,
    Schedule2HoursTypeEnum,
    Schedule2TypeEnum,
} from 'sailpoint-api-client/dist/certification_campaigns/api'
export type { Index, Search } from 'sailpoint-api-client/dist/search/api'
export { Index as SearchIndex } from 'sailpoint-api-client/dist/search/api'
export type {
    AccessCriteriaCriteriaListInner,
    Schedule,
    SodPolicy,
    SodPolicyConflictingAccessCriteria,
    SodPolicyOwnerRef,
    SodPolicySecondaryOwnerRefsInner,
    SodRecipient,
    ViolationOwnerAssignmentConfig,
} from 'sailpoint-api-client/dist/sod_policies/api'
export {
    AccessCriteriaCriteriaListInnerTypeEnum,
    JsonPatchOperationOpEnum,
    ScheduleType,
    SelectorType,
    SodPolicyLevelEnum,
    SodPolicyStateEnum,
    SodPolicyTypeEnum,
    ViolationOwnerAssignmentConfigAssignmentRuleEnum,
} from 'sailpoint-api-client/dist/sod_policies/api'
