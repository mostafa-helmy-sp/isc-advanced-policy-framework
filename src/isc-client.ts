import { logger } from '@sailpoint/connector-sdk'
import {
    Campaign2AllOfSearchCampaignInfoReviewer,
    Configuration,
    SodPolicyOwnerRef,
    SodRecipient,
    ViolationOwnerAssignmentConfigAssignmentRuleEnum,
} from './types/sailpoint-api'
import { createApiConfig } from './api/client-factory'
import {
    buildCampaignAccessConstraints,
    buildPolicyConflictingAccessCriteria,
    buildViolationOwnerAssignmentConfig,
} from './builders/access-constraint-builder'
import { buildCampaignSchedule, buildPolicySchedule } from './builders/schedule-builder'
import { ConnectorConfig, resolveConnectorSettings } from './config/connector-config'
import { PolicyConfig, toPolicyConfigAccount } from './model/policy-config'
import { PolicyImpl } from './model/policy-impl'
import { CampaignService } from './services/campaign-service'
import { EntitlementHierarchyService } from './services/entitlement-hierarchy'
import { OwnerResolverService } from './services/owner-resolver'
import { PolicyConfigSourceService } from './services/policy-config-source'
import { SearchService } from './services/search-service'
import { SodPolicyService } from './services/sod-policy-service'
import { PolicyAction, PolicyType } from './types/enums'
import { buildEntitlementNameArray, buildNameArray } from './utils/api-helper'
import { parsePolicyLevel } from './utils/owner-parser'

export { PolicyAction, PolicyType } from './types/enums'

/**
 * Facade for ISC API operations. Reads policy definitions from a CSV source and
 * provisions SOD policies, schedules, and certification campaigns.
 */
export class IscClient {
    private readonly config: ConnectorConfig
    private readonly settings: ReturnType<typeof resolveConnectorSettings>
    private readonly apiConfig: Configuration
    private readonly policyConfigSource: PolicyConfigSourceService
    private readonly searchService: SearchService
    private readonly entitlementHierarchyService: EntitlementHierarchyService
    private readonly ownerResolver: OwnerResolverService
    private readonly sodPolicyService: SodPolicyService
    private readonly campaignService: CampaignService

    constructor(config: ConnectorConfig) {
        this.config = config
        this.settings = resolveConnectorSettings(config)
        this.apiConfig = createApiConfig(config)
        this.policyConfigSource = new PolicyConfigSourceService(this.apiConfig, this.settings.policyConfigSourceName)
        this.searchService = new SearchService()
        this.entitlementHierarchyService = new EntitlementHierarchyService(this.searchService)
        this.ownerResolver = new OwnerResolverService(this.searchService, this.settings.identityResolutionAttribute)
        this.sodPolicyService = new SodPolicyService()
        this.campaignService = new CampaignService()
    }

    isParallelProcessing(): boolean {
        return this.settings.parallelProcessing
    }

    getMaxConcurrentPolicies(): number {
        return this.settings.maxConcurrentPolicies
    }

    /** Returns all policy configuration rows from the CSV source. */
    async getAllPolicyConfigs() {
        return this.policyConfigSource.getAllPolicyConfigs()
    }

    /**
     * Validates connectivity by resolving the policy configuration source ID.
     * @returns An error message if the source cannot be found, otherwise undefined.
     */
    async testConnection(): Promise<string | undefined> {
        const sourceId = await this.policyConfigSource.getPolicyConfigSourceId()
        if (!sourceId) {
            return 'Unable to retrieve the Policy Configuration Source ID using the Provided Source Name'
        }
        return undefined
    }

    /**
     * Reads and processes a single policy by name (std:account:read).
     * @returns The processed policy result, or undefined if not found or not SOD type.
     */
    async getAccount(identity: string): Promise<PolicyImpl | undefined> {
        const policyConfigObject = await this.policyConfigSource.getPolicyConfigByName(identity)
        if (!policyConfigObject) {
            return undefined
        }
        const policyConfig = new PolicyConfig(toPolicyConfigAccount(policyConfigObject))
        if (policyConfig.policyType === PolicyType.SOD) {
            return this.processSodPolicyConfig(policyConfig, this.apiConfig)
        }
        return undefined
    }

    /**
     * Processes a single SOD policy configuration: resolves entitlements, creates or updates
     * the policy, optionally schedules reports and certification campaigns.
     */
    async processSodPolicyConfig(policyConfig: PolicyConfig, apiConfig?: Configuration): Promise<PolicyImpl> {
        logger.info(`### Processing policy [${policyConfig.policyName}] ###`)

        const errorMessages: string[] = []
        const policyImpl = new PolicyImpl(policyConfig.policyName)
        const activeApiConfig = apiConfig ?? (this.settings.parallelProcessing ? createApiConfig(this.config) : this.apiConfig)

        if (policyConfig.actions.includes(PolicyAction.DELETE_ALL)) {
            await this.handleDeletePolicy(activeApiConfig, policyConfig, policyImpl, errorMessages)
        } else {
            const processed = await this.handlePolicyUpsert(activeApiConfig, policyConfig, policyImpl, errorMessages)
            if (!processed) {
                policyImpl.attributes.errorMessages = JSON.stringify(errorMessages)
                return policyImpl
            }
        }

        await this.handleDeleteCampaign(activeApiConfig, policyConfig, policyImpl, errorMessages)

        logger.info(`### Finished processing policy [${policyConfig.policyName}] ###`)
        policyImpl.attributes.errorMessages = JSON.stringify(errorMessages)
        return policyImpl
    }

    private async handleDeletePolicy(
        apiConfig: Configuration,
        policyConfig: PolicyConfig,
        policyImpl: PolicyImpl,
        errorMessages: string[]
    ): Promise<void> {
        const existingPolicy = await this.sodPolicyService.findExistingPolicy(apiConfig, policyConfig)
        if (existingPolicy?.id) {
            const errorMessage = await this.sodPolicyService.deletePolicy(apiConfig, existingPolicy.id)
            if (errorMessage) {
                errorMessages.push(errorMessage)
            } else {
                policyImpl.attributes.policyDeleted = true
            }
        } else {
            errorMessages.push(`No Policy found by name [${policyConfig.policyName}] to delete.`)
        }
    }

    private async handlePolicyUpsert(
        apiConfig: Configuration,
        policyConfig: PolicyConfig,
        policyImpl: PolicyImpl,
        errorMessages: string[]
    ): Promise<boolean> {
        let canProcess = true
        let [query1Entitlements, query2Entitlements] = await Promise.all([
            this.searchService.searchEntitlementsByQuery(apiConfig, policyConfig.query1),
            this.searchService.searchEntitlementsByQuery(apiConfig, policyConfig.query2),
        ])

        if (this.settings.resolveNestedEntitlements) {
            ;[query1Entitlements, query2Entitlements] = await Promise.all([
                this.entitlementHierarchyService.includeEntitlementHierarchy(apiConfig, query1Entitlements),
                this.entitlementHierarchyService.includeEntitlementHierarchy(apiConfig, query2Entitlements),
            ])
        }

        policyImpl.attributes.leftHandEntitlements = JSON.stringify(buildEntitlementNameArray(query1Entitlements))
        policyImpl.attributes.rightHandEntitlements = JSON.stringify(buildEntitlementNameArray(query2Entitlements))
        policyImpl.attributes.leftHandEntitlementCount = query1Entitlements.length
        policyImpl.attributes.rightHandEntitlementCount = query2Entitlements.length

        if (query1Entitlements.length === 0) {
            canProcess = false
            errorMessages.push(`Entitlement Query 1 [${policyConfig.query1}] returns no entitlements`)
        }
        if (query2Entitlements.length === 0) {
            canProcess = false
            errorMessages.push(`Entitlement Query 2 [${policyConfig.query2}] returns no entitlements`)
        }
        if (query1Entitlements.length > this.settings.maxEntitlementsPerPolicySide) {
            canProcess = false
            errorMessages.push(
                `Entitlement Query 1 [${policyConfig.query1}] result exceeds Identity Security Cloud limit of ${this.settings.maxEntitlementsPerPolicySide} entitlements`
            )
        }
        if (query2Entitlements.length > this.settings.maxEntitlementsPerPolicySide) {
            canProcess = false
            errorMessages.push(
                `Entitlement Query 2 [${policyConfig.query2}] result exceeds Identity Security Cloud limit of ${this.settings.maxEntitlementsPerPolicySide} entitlements`
            )
        }

        const [policyOwner, violationOwner] = await Promise.all([
            this.ownerResolver.resolvePolicyOwner(apiConfig, policyConfig),
            this.ownerResolver.resolveViolationOwner(apiConfig, policyConfig),
        ])
        if (!policyOwner) {
            canProcess = false
            errorMessages.push(
                `Unable to resolve Policy Owner. Type: ${policyConfig.policyOwnerType}, Value: ${policyConfig.policyOwner}`
            )
        }

        if (!violationOwner && policyConfig.violationOwnerType !== ViolationOwnerAssignmentConfigAssignmentRuleEnum.Manager) {
            canProcess = false
            errorMessages.push(
                `Unable to resolve Violation Manager. Type: ${policyConfig.violationOwnerType}, Value: ${policyConfig.violationOwner}`
            )
        }

        const levelResult = parsePolicyLevel(policyConfig.level)
        if (levelResult.error) {
            canProcess = false
            errorMessages.push(levelResult.error)
        }

        const coOwnerResult = await this.ownerResolver.resolveCoOwners(apiConfig, policyConfig.coOwners)
        if (coOwnerResult.errors.length > 0) {
            canProcess = false
            errorMessages.push(...coOwnerResult.errors)
        }

        if (!canProcess || !policyOwner || !levelResult.level) {
            return false
        }

        const conflictingAccessCriteria = buildPolicyConflictingAccessCriteria(policyConfig, query1Entitlements, query2Entitlements)
        const violationOwnerAssignmentConfig = buildViolationOwnerAssignmentConfig(violationOwner)
        const existingPolicy = await this.sodPolicyService.findExistingPolicy(apiConfig, policyConfig)

        let policyId = ''
        let policyQuery = ''
        let errorMessage = ''

        if (existingPolicy?.id) {
            ;[errorMessage, policyQuery] = await this.sodPolicyService.updatePolicy(
                apiConfig,
                existingPolicy.id,
                policyConfig,
                policyOwner as SodPolicyOwnerRef,
                violationOwnerAssignmentConfig,
                conflictingAccessCriteria,
                levelResult.level,
                coOwnerResult.refs
            )
            policyId = existingPolicy.id
        } else {
            ;[errorMessage, policyId, policyQuery] = await this.sodPolicyService.createPolicy(
                apiConfig,
                policyConfig,
                policyOwner as SodPolicyOwnerRef,
                violationOwnerAssignmentConfig,
                conflictingAccessCriteria,
                levelResult.level,
                coOwnerResult.refs
            )
        }

        if (errorMessage) {
            errorMessages.push(errorMessage)
            return false
        }
        if (!policyId) {
            errorMessages.push('No policy Id returned while processing the policy?')
            return false
        }
        if (!policyQuery) {
            errorMessages.push('No policyQuery returned while processing the policy?')
            return false
        }

        policyImpl.attributes.policyQuery = policyQuery
        policyImpl.attributes.policyConfigured = true

        if (policyConfig.actions.includes(PolicyAction.REPORT)) {
            const scheduleOptions = {
                hourlyScheduleDay: this.settings.hourlyScheduleDay,
                weeklyScheduleDay: this.settings.weeklyScheduleDay,
                monthlyScheduleDay: this.settings.monthlyScheduleDay,
            }
            const policySchedule = buildPolicySchedule(policyConfig.policySchedule, scheduleOptions)
            if (policySchedule) {
                const policyRecipients = await this.ownerResolver.resolvePolicyRecipients(
                    apiConfig,
                    policyConfig,
                    violationOwner,
                    policyOwner
                )
                errorMessage = await this.sodPolicyService.setPolicySchedule(
                    apiConfig,
                    policyId,
                    policyConfig,
                    policySchedule,
                    policyRecipients as SodRecipient[]
                )
                if (errorMessage) {
                    errorMessages.push(errorMessage)
                } else {
                    policyImpl.attributes.policyScheduleConfigured = true
                }
            } else {
                errorMessages.push(`Unable to build policy schedule using schedule [${policyConfig.policySchedule}]`)
            }
        }

        const [query1AccessProfiles, query2AccessProfiles] = await Promise.all([
            this.searchService.searchAccessProfilesByEntitlements(apiConfig, query1Entitlements),
            this.searchService.searchAccessProfilesByEntitlements(apiConfig, query2Entitlements),
        ])
        const [query1Roles, query2Roles] = await Promise.all([
            this.searchService.searchRolesByAccessProfilesOrEntitlements(apiConfig, query1Entitlements, query1AccessProfiles),
            this.searchService.searchRolesByAccessProfilesOrEntitlements(apiConfig, query2Entitlements, query2AccessProfiles),
        ])

        policyImpl.attributes.leftHandAccessProfiles = JSON.stringify(buildNameArray(query1AccessProfiles))
        policyImpl.attributes.rightHandAccessProfiles = JSON.stringify(buildNameArray(query2AccessProfiles))
        policyImpl.attributes.leftHandRoles = JSON.stringify(buildNameArray(query1Roles))
        policyImpl.attributes.rightHandRoles = JSON.stringify(buildNameArray(query2Roles))

        const [accessConstraints, leftHandTotalCount, rightHandTotalCount, totalCount] = buildCampaignAccessConstraints(
            query1Entitlements,
            query2Entitlements,
            query1AccessProfiles,
            query2AccessProfiles,
            query1Roles,
            query2Roles
        )

        policyImpl.attributes.leftHandTotalCount = leftHandTotalCount
        policyImpl.attributes.rightHandTotalCount = rightHandTotalCount
        policyImpl.attributes.totalCount = totalCount

        if (policyConfig.actions.includes(PolicyAction.CERTIFY) && !policyConfig.actions.includes(PolicyAction.DELETE_CAMPAIGN)) {
            await this.handleCampaignUpsert(
                apiConfig,
                policyConfig,
                policyImpl,
                errorMessages,
                policyQuery,
                accessConstraints,
                violationOwner as Campaign2AllOfSearchCampaignInfoReviewer | undefined,
                totalCount
            )
        }

        return true
    }

    private async handleCampaignUpsert(
        apiConfig: Configuration,
        policyConfig: PolicyConfig,
        policyImpl: PolicyImpl,
        errorMessages: string[],
        policyQuery: string,
        accessConstraints: ReturnType<typeof buildCampaignAccessConstraints>[0],
        violationOwner: Campaign2AllOfSearchCampaignInfoReviewer | undefined,
        totalCount: number
    ): Promise<void> {
        let canProcess = true

        if (totalCount > this.settings.maxAccessItemsPerCampaign) {
            canProcess = false
            errorMessages.push(
                `Total number of access items to review exceeds Identity Security Cloud limit of ${this.settings.maxAccessItemsPerCampaign} access items.`
            )
        }
        if (!policyConfig.certificationName) {
            canProcess = false
            errorMessages.push('A Certification Campaign Name is required to define a Certification Campaign.')
        }
        if (!policyConfig.certificationDescription) {
            canProcess = false
            errorMessages.push('A Certification Campaign Description is required to define a Certification Campaign.')
        }

        if (!canProcess) {
            return
        }

        const existingCampaign = await this.campaignService.findExistingCampaign(apiConfig, policyConfig)
        let campaignId = ''
        let errorMessage = ''

        if (existingCampaign?.id) {
            errorMessage = await this.campaignService.updatePolicyCampaign(
                apiConfig,
                existingCampaign.id,
                policyConfig,
                policyQuery,
                accessConstraints,
                violationOwner as Campaign2AllOfSearchCampaignInfoReviewer | undefined,
                this.settings.campaignDuration
            )
            campaignId = existingCampaign.id
        } else {
            ;[errorMessage, campaignId] = await this.campaignService.createPolicyCampaign(
                apiConfig,
                policyConfig,
                policyQuery,
                accessConstraints,
                violationOwner as Campaign2AllOfSearchCampaignInfoReviewer | undefined,
                this.settings.campaignDuration
            )
        }

        if (errorMessage) {
            errorMessages.push(errorMessage)
            return
        }
        if (!campaignId) {
            errorMessages.push('No campaign Id returned while processing the policy?')
            return
        }

        policyImpl.attributes.campaignConfigured = true
        policyImpl.attributes.campaignTemplateName = policyConfig.certificationName
        policyImpl.attributes.certificationName = policyConfig.certificationName

        if (policyConfig.certificationSchedule) {
            const scheduleOptions = {
                hourlyScheduleDay: this.settings.hourlyScheduleDay,
                weeklyScheduleDay: this.settings.weeklyScheduleDay,
                monthlyScheduleDay: this.settings.monthlyScheduleDay,
            }
            const campaignSchedule = buildCampaignSchedule(policyConfig.certificationSchedule, scheduleOptions)
            if (campaignSchedule) {
                errorMessage = await this.campaignService.setCampaignSchedule(apiConfig, campaignId, campaignSchedule)
                if (errorMessage) {
                    errorMessages.push(errorMessage)
                } else {
                    policyImpl.attributes.campaignScheduleConfigured = true
                }
            } else {
                errorMessages.push(
                    `Unable to build campaign schedule using schedule [${policyConfig.certificationSchedule}]`
                )
            }
        }
    }

    private async handleDeleteCampaign(
        apiConfig: Configuration,
        policyConfig: PolicyConfig,
        policyImpl: PolicyImpl,
        errorMessages: string[]
    ): Promise<void> {
        if (!policyConfig.actions.includes(PolicyAction.DELETE_CAMPAIGN) && !policyConfig.actions.includes(PolicyAction.DELETE_ALL)) {
            return
        }

        if (!policyConfig.certificationName) {
            errorMessages.push('A Certification Campaign Name is required to delete it.')
            return
        }

        const existingCampaign = await this.campaignService.findExistingCampaign(apiConfig, policyConfig)
        if (existingCampaign?.id) {
            const errorMessage = await this.campaignService.deletePolicyCampaign(apiConfig, existingCampaign.id)
            if (errorMessage) {
                errorMessages.push(errorMessage)
            } else {
                policyImpl.attributes.campaignDeleted = true
            }
        } else {
            errorMessages.push(`No Certification Campaign found by name [${policyConfig.certificationName}] to delete.`)
        }
    }
}
