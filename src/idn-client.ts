import {
    ConnectorError,
    logger
} from "@sailpoint/connector-sdk"
import {
    Configuration,
    ConfigurationParameters,
    SourcesApi,
    SourcesApiListSourcesRequest,
    Account,
    AccountsApi,
    AccountsApiListAccountsRequest,
    SearchApi,
    Search,
    Index,
    Paginator,
    EntitlementDocument,
    AccessProfileDocument,
    RoleDocument,
    GovernanceGroupsBetaApi,
    GovernanceGroupsBetaApiListWorkgroupsRequest,
    GovernanceGroupsBetaApiListWorkgroupMembersRequest,
    DtoType,
    JsonPatchOperationOpEnum,
    SodPolicy,
    SodPolicyStateEnum,
    ViolationOwnerAssignmentConfig,
    ViolationOwnerAssignmentConfigAssignmentRuleEnum,
    SodPolicyConflictingAccessCriteria,
    AccessCriteriaCriteriaListInner,
    AccessConstraint,
    AccessConstraintTypeEnum,
    AccessConstraintOperatorEnum,
    AccessCriteriaCriteriaListInnerTypeEnum,
    SODPolicyApi,
    SodPolicyTypeEnum,
    SODPolicyApiListSodPoliciesRequest,
    SODPolicyApiCreateSodPolicyRequest,
    SODPolicyApiPatchSodPolicyRequest,
    SODPolicyApiPutPolicyScheduleRequest,
    SODPolicyApiDeleteSodPolicyRequest,
    Schedule,
    ScheduleType,
    ScheduleTypeEnum,
    ScheduleHoursTypeEnum,
    ScheduleDaysTypeEnum,
    CampaignTemplate,
    CampaignTypeEnum,
    CampaignCorrelatedStatusEnum,
    CampaignAllOfSearchCampaignInfoTypeEnum,
    CampaignAllOfSearchCampaignInfoReviewer,
    CertificationCampaignsApi,
    CertificationCampaignsApiUpdateCampaignRequest,
    CertificationCampaignsApiListCampaignTemplatesRequest,
    CertificationCampaignsApiDeleteCampaignTemplateRequest,
    CertificationCampaignsApiCreateCampaignTemplateRequest,
    CertificationCampaignsApiSetCampaignTemplateScheduleRequest
} from "sailpoint-api-client"
import { PolicyConfig } from "./model/policy-config"
import { PolicyImpl } from "./model/policy-impl"
import axiosRetry from "axios-retry"

// Set IDN Global Variables
var tokenUrlPath = "/oauth/token"
var maxHoursPerCampaignSchedule = 1
var maxWeeklyDaysPerCampaignSchedule = 1
var maxMonthlyDaysPerCampaignSchedule = 4

// Set Source Config Global Defaults
var defaultIdentityResolutionAttribute = "name"
var defaultHourlyScheduleDay = ["9"]
var defaultWeeklyScheduleDay = ["MON"]
var defaultMonthlyScheduleDay = ["1"]
var defaultCampaignDuration = "P2W"
var defaultMaxEntitlementsPerPolicySide = 400
var defaultMaxAccessItemsPerCampaign = 10000

// Set Connector Values
var sodPolicyType = "SOD"
var actionSchedulePolicy = "REPORT"
var actionCertifyViolations = "CERTIFY"
var actionDeleteAll = "DELETE_ALL"
var actionDeleteCampaign = "DELETE_CAMPAIGN"

export class IdnClient {

    private readonly apiConfig: Configuration
    private readonly policyConfigSourceName: string
    private readonly policySourceName?: string
    private policyConfigSourceId?: string
    private identityResolutionAttribute: string
    private hourlyScheduleDay: string[]
    private weeklyScheduleDay: string[]
    private monthlyScheduleDay: string[]
    private campaignDuration: string
    private maxEntitlementsPerPolicySide: number
    private maxAccessItemsPerCampaign: number

    constructor(config: any) {
        // configure the SailPoint SDK API Client
        const ConfigurationParameters: ConfigurationParameters = {
            baseurl: config.apiUrl,
            clientId: config.clientId,
            clientSecret: config.clientSecret,
            tokenUrl: config.apiUrl + tokenUrlPath
        }
        this.apiConfig = new Configuration(ConfigurationParameters)
        this.apiConfig.retriesConfig = {
            retries: 10,
            // retryDelay: (retryCount) => { return retryCount * 2000; },
            retryDelay: (retryCount, error) => axiosRetry.exponentialDelay(retryCount, error, 2000),
            retryCondition: (error) => {
                return error.response?.status === 429;
            },
            onRetry: (retryCount, error, requestConfig) => {
                logger.debug(`Retrying API [${requestConfig.url}] due to request error: [${error}]. Try number [${retryCount}]`)
            }
        }
        // configure the rest of the source parameters
        this.policyConfigSourceName = config.policyConfigSourceName
        this.policySourceName = config.policySourceName
        this.identityResolutionAttribute = config.identityResolutionAttribute ?? defaultIdentityResolutionAttribute
        this.hourlyScheduleDay = config.hourlyScheduleDay ? (Array.isArray(config.hourlyScheduleDay) ? config.hourlyScheduleDay : [config.hourlyScheduleDay]) : defaultHourlyScheduleDay
        this.weeklyScheduleDay = config.weeklyScheduleDay ? (Array.isArray(config.weeklyScheduleDay) ? config.weeklyScheduleDay : [config.weeklyScheduleDay]) : defaultWeeklyScheduleDay
        this.monthlyScheduleDay = config.monthlyScheduleDay ? (Array.isArray(config.monthlyScheduleDay) ? config.monthlyScheduleDay : [config.monthlyScheduleDay]) : defaultMonthlyScheduleDay
        this.campaignDuration = config.campaignDuration || defaultCampaignDuration
        this.maxEntitlementsPerPolicySide = config.maxEntitlementsPerPolicySide || defaultMaxEntitlementsPerPolicySide
        this.maxAccessItemsPerCampaign = config.maxAccessItemsPerCampaign || defaultMaxAccessItemsPerCampaign
    }

    async getPolicyConfigSourceId(): Promise<string | undefined> {
        let filter = `name eq "${this.policyConfigSourceName}"`
        // Check if Source ID is null
        if (!this.policyConfigSourceId) {
            // Get and set Source ID if not already set
            logger.debug("Policy Config Source ID not set, getting the ID using the Sources API")
            const sourceApi = new SourcesApi(this.apiConfig)
            const sourcesRequest: SourcesApiListSourcesRequest = {
                filters: filter
            }
            try {
                const sources = await sourceApi.listSources(sourcesRequest)
                if (sources.data.length > 0) {
                    this.policyConfigSourceId = sources.data[0].id
                }
            } catch (error) {
                let errorMessage = `Error retrieving Policy Configurations Source ID using Sources API: ${(error as Error).message}`
                let debugMessage = `Failed Sources API request: ${JSON.stringify(error)}`
                logger.error(sourcesRequest, errorMessage)
                logger.debug(debugMessage)
                throw new ConnectorError(errorMessage)
            }
        }
        // Return set Source ID
        logger.debug(`Policy Config Source Id: [${this.policyConfigSourceId}]`)
        return this.policyConfigSourceId
    }

    async getAllPolicyConfigs(): Promise<Account[]> {
        // Get Policy Config Source ID
        await this.getPolicyConfigSourceId()
        const filter = `sourceId eq "${this.policyConfigSourceId}"`
        // Use Accounts API to get the Policy configurations stored as accounts in the Policy Config Source
        const accountsApi = new AccountsApi(this.apiConfig)
        const accountsRequest: AccountsApiListAccountsRequest = {
            filters: filter
        }
        try {
            const accounts = await Paginator.paginate(accountsApi, accountsApi.listAccounts, { filters: filter })
            logger.debug(`Found ${accounts.data.length} Policy Configurations`)
            return accounts.data
        } catch (error) {
            let errorMessage = `Error retrieving Policy Configurations from the Policy Config Source using ListAccounts API: ${(error as Error).message}`
            let debugMessage = `Failed ListAccounts API request: ${JSON.stringify(error)}`
            logger.error(accountsRequest, errorMessage)
            logger.debug(debugMessage)
            throw new ConnectorError(errorMessage)
        }
    }

    async getPolicyConfigByName(policyName: string): Promise<Account> {
        // Get Policy Config Source ID
        await this.getPolicyConfigSourceId()
        const filter = `sourceId eq "${this.policyConfigSourceId}" and name eq "${policyName}"`
        // Use Accounts API to get the Policy configuration stored as an account in the Policy Config Source by name
        const accountsApi = new AccountsApi(this.apiConfig)
        const accountsRequest: AccountsApiListAccountsRequest = {
            filters: filter
        }
        try {
            const accounts = await accountsApi.listAccounts(accountsRequest)
            logger.debug(`Found ${accounts.data.length} Policy Configurations`)
            return accounts.data[0]
        } catch (error) {
            let errorMessage = `Error retrieving single Policy Configuration from the Policy Config Source using ListAccounts API: ${(error as Error).message}`
            let debugMessage = `Failed ListAccounts API request: ${JSON.stringify(error)}`
            logger.error(accountsRequest, errorMessage)
            logger.debug(debugMessage)
            throw new ConnectorError(errorMessage)
        }
    }

    async findExistingPolicy(policyConfig: PolicyConfig): Promise<SodPolicy | undefined> {
        const filter = `name eq "${policyConfig.policyName}"`
        const policyApi = new SODPolicyApi(this.apiConfig)
        const findPolicyRequest: SODPolicyApiListSodPoliciesRequest = {
            filters: filter
        }
        try {
            const existingPolicy = await policyApi.listSodPolicies(findPolicyRequest)
            // Check if no policy already exists
            if (existingPolicy.data.length == 0 || !existingPolicy.data[0].id) {
                return
            } else {
                return existingPolicy.data[0]
            }
        } catch (error) {
            let errorMessage = `Error finding existing Policy using SOD-Policies API: ${(error as Error).message}`
            let debugMessage = `Failed SOD-Policies API request: ${JSON.stringify(error)}`
            logger.error(findPolicyRequest, errorMessage)
            logger.debug(debugMessage)
            return
        }
    }

    async findExistingCampaign(policyConfig: PolicyConfig): Promise<CampaignTemplate | undefined> {
        const filter = `name eq "${policyConfig.certificationName}"`
        const certsApi = new CertificationCampaignsApi(this.apiConfig)
        const findCampaignRequest: CertificationCampaignsApiListCampaignTemplatesRequest = {
            filters: filter
        }
        try {
            const existingCampaign = await certsApi.listCampaignTemplates(findCampaignRequest)
            // Check if no campaign already exists
            if (existingCampaign.data.length == 0 || !existingCampaign.data[0].id) {
                return
            } else {
                return existingCampaign.data[0]
            }
        } catch (error) {
            let errorMessage = `Error finding existing Campaign using Certification-Campaigns API: ${(error as Error).message}`
            let debugMessage = `Failed Certification-Campaigns API request: ${JSON.stringify(error)}`
            logger.error(findCampaignRequest, errorMessage)
            logger.debug(debugMessage)
            return
        }
    }

    buildIdQuery(items: any[], itemPrefix: string, joiner: string, prefix?: string, suffix?: string): string {
        let query = ""
        // Add global prefix, e.g.: "@entitlements("
        if (prefix) {
            query += prefix
        }
        let count = 0
        for (const item of items) {
            // Add joiner first unless first item, e.g.: " OR "
            if (count > 0) {
                query += joiner
            }
            // Add item prefix, e.g.: "id:"
            query += itemPrefix
            query += item.id
            count++
        }
        // Add global suffix, e.g.: ")"
        if (suffix) {
            query += suffix
        }
        return query
    }

    buildIdArray(items: any[]): string[] {
        let ids: string[] = []
        items.forEach(item => ids.push(item.id))
        return ids
    }

    mergeUnique(items1: any[], items2: any[]): any[] {
        return [... new Set([...items1, ...items2])]
    }

    async searchEntitlementsByQuery(query: string): Promise<EntitlementDocument[]> {
        const searchApi = new SearchApi(this.apiConfig)
        const search: Search = {
            indices: [
                Index.Entitlements
            ],
            query: {
                query: query
            },
            queryResultFilter: {
                includes: [
                    "id",
                    "name",
                    "schema",
                    "type",
                    "source.name"
                ]
            },
            sort: ["id"]
        }
        try {
            const entitlements = (await Paginator.paginateSearchApi(searchApi, search)).data as EntitlementDocument[]
            return entitlements
        } catch (error) {
            let errorMessage = `Error finding entitlements using Search API: ${(error as Error).message}`
            let debugMessage = `Failed Search API request: ${JSON.stringify(error)}`
            logger.error(search, errorMessage)
            logger.debug(debugMessage)
            return []
        }
    }

    async searchAccessProfilesbyEntitlements(entitlements: any[]): Promise<AccessProfileDocument[]> {
        if (!entitlements || entitlements.length == 0) {
            return []
        }
        const query = this.buildIdQuery(entitlements, "id:", " OR ", "@entitlements(", ")")
        const searchApi = new SearchApi(this.apiConfig)
        const search: Search = {
            indices: [
                Index.Accessprofiles
            ],
            query: {
                query: query
            },
            queryResultFilter: {
                includes: [
                    "id",
                    "name",
                    "type",
                    "source.name"
                ]
            },
            sort: ["id"]
        }
        try {
            const accessProfiles: AccessProfileDocument[] = (await Paginator.paginateSearchApi(searchApi, search)).data
            return accessProfiles
        } catch (error) {
            let errorMessage = `Error finding access profiles using Search API: ${(error as Error).message}`
            let debugMessage = `Failed Search API request: ${JSON.stringify(error)}`
            logger.error(search, errorMessage)
            logger.debug(debugMessage)
            return []
        }
    }

    async searchRolesByAccessProfilesOrEntitlements(entitlements: any[], accessProfiles: any[]): Promise<RoleDocument[]> {
        let query
        if (entitlements && entitlements.length > 0) {
            query = this.buildIdQuery(entitlements, "id:", " OR ", "@entitlements(", ")")
        }
        if (accessProfiles && accessProfiles.length > 0) {
            if (!query) {
                query = this.buildIdQuery(accessProfiles, "accessProfiles.id:", " OR ")
            } else {
                query += this.buildIdQuery(accessProfiles, "accessProfiles.id:", " OR ", " OR ")
            }
        }
        if (!query) {
            return []
        }
        const searchApi = new SearchApi(this.apiConfig)
        const search: Search = {
            indices: [
                Index.Roles
            ],
            query: {
                query: query
            },
            queryResultFilter: {
                includes: [
                    "id",
                    "name",
                    "type"
                ]
            },
            sort: ["id"]
        }
        try {
            const roles: RoleDocument[] = (await Paginator.paginateSearchApi(searchApi, search)).data
            return roles
        } catch (error) {
            let errorMessage = `Error finding roles using Search API: ${(error as Error).message}`
            let debugMessage = `Failed Search API request: ${JSON.stringify(error)}`
            logger.error(search, errorMessage)
            logger.debug(debugMessage)
            return []
        }
    }

    async searchIdentityByAttribute(attribute: string, value: string): Promise<any> {
        const searchApi = new SearchApi(this.apiConfig)
        let query = ""
        if (attribute === "name" || attribute === "employeeNumber" || attribute === "id") {
            query = `${attribute}:"${value}"`
        } else {
            query = `attributes.${attribute}.exact:"${value}"`
        }
        const search: Search = {
            indices: [
                Index.Identities
            ],
            query: {
                query: query
            },
            queryResultFilter: {
                includes: [
                    "id",
                    "name",
                    "type"
                ]
            },
            sort: ["id"]
        }
        try {
            const identities = await Paginator.paginateSearchApi(searchApi, search)
            // Check if no identity exists
            if (identities.data.length == 0) {
                return
            } else {
                // Use the first identity if more than one match
                const identity = identities.data[0]
                return { id: identity.id, name: identity.name, type: identity._type.toUpperCase() }
            }
        } catch (error) {
            let errorMessage = `Error finding identity using Search API: ${(error as Error).message}`
            let debugMessage = `Failed Search API request: ${JSON.stringify(error)}`
            logger.error(search, errorMessage)
            logger.debug(debugMessage)
            return
        }
    }

    async searchGovGroupByName(govGroupName: string): Promise<any> {
        const filter = `name eq "${govGroupName}"`
        const govGroupApi = new GovernanceGroupsBetaApi(this.apiConfig)
        const findGovGroupRequest: GovernanceGroupsBetaApiListWorkgroupsRequest = {
            filters: filter
        }
        try {
            const existingGovGroup = await govGroupApi.listWorkgroups(findGovGroupRequest)
            // Check if no governance group exists
            if (existingGovGroup.data.length == 0) {
                return
            } else {
                // Use the first governance group if more than one match
                const govGroup = existingGovGroup.data[0]
                return { id: govGroup.id, name: govGroup.name, type: DtoType.GovernanceGroup }
            }
        } catch (error) {
            let errorMessage = `Error finding Governance Group using Governance-Groups API: ${(error as Error).message}`
            let debugMessage = `Failed Governance-Groups API request: ${JSON.stringify(error)}`
            logger.error(findGovGroupRequest, errorMessage)
            logger.debug(debugMessage)
            return
        }
    }

    async findGovGroupMembers(govGroupId: string): Promise<any[]> {
        const govGroupApi = new GovernanceGroupsBetaApi(this.apiConfig)
        const findGovGroupMembersRequest: GovernanceGroupsBetaApiListWorkgroupMembersRequest = {
            workgroupId: govGroupId
        }
        try {
            const govGroupMembers = await Paginator.paginate(govGroupApi, govGroupApi.listWorkgroupMembers, { workgroupId: govGroupId })
            // Check if no governance group members exist
            if (govGroupMembers.data.length == 0) {
                return []
            } else {
                // Return the governance group members
                let members: any[] = []
                govGroupMembers.data.forEach(govGroupMember => members.push({ id: govGroupMember.id, type: DtoType.Identity, name: govGroupMember.name }))
                return members
            }
        } catch (error) {
            let errorMessage = `Error finding Governance Group members using Governance-Groups API: ${(error as Error).message}`
            let debugMessage = `Failed Governance-Groups API request: ${JSON.stringify(error)}`
            logger.error(findGovGroupMembersRequest, errorMessage)
            logger.debug(debugMessage)
            return []
        }
    }

    buildConflictingAccessCriteriaList(items: EntitlementDocument[]): AccessCriteriaCriteriaListInner[] {
        let criteriaList: AccessCriteriaCriteriaListInner[] = []
        items.forEach(item => criteriaList.push({ id: item.id, type: AccessCriteriaCriteriaListInnerTypeEnum.Entitlement }))
        return criteriaList
    }

    buildPolicyConflictingAccessCriteria(policyConfig: PolicyConfig, query1Entitlemnts: EntitlementDocument[], query2Entitlemnts: EntitlementDocument[]): SodPolicyConflictingAccessCriteria {
        // Build ID,Type,Name arrays
        const leftCriteria = this.buildConflictingAccessCriteriaList(query1Entitlemnts)
        const rightCriteria = this.buildConflictingAccessCriteriaList(query2Entitlemnts)
        // Build the conflicting access criteria
        const criteria: SodPolicyConflictingAccessCriteria = {
            leftCriteria: {
                name: policyConfig.query1Name,
                criteriaList: leftCriteria
            },
            rightCriteria: {
                name: policyConfig.query2Name,
                criteriaList: rightCriteria
            }
        }
        return criteria
    }

    buildCampaignAccsesConstraints(entitlements1: EntitlementDocument[], entitlements2: EntitlementDocument[], accessProfiles1: AccessProfileDocument[], accessProfiles2: AccessProfileDocument[], roles1: RoleDocument[], roles2: RoleDocument[]): [accessConstraints: AccessConstraint[], leftHandTotalCount: number, rightHandTotalCount: number, totalCount: number] {
        let accessConstraints: AccessConstraint[] = []
        // Build ID only arrays
        const entitlement1Ids = this.buildIdArray(entitlements1)
        const entitlement2Ids = this.buildIdArray(entitlements2)
        const accessProfile1Ids = this.buildIdArray(accessProfiles1)
        const accessProfile2Ids = this.buildIdArray(accessProfiles2)
        const role1Ids = this.buildIdArray(roles1)
        const role2Ids = this.buildIdArray(roles2)
        // Merge left and right arrays uniquely
        const entitlementIds: string[] = this.mergeUnique(entitlement1Ids, entitlement2Ids)
        const accessProfileIds: string[] = this.mergeUnique(accessProfile1Ids, accessProfile2Ids)
        const roleIds: string[] = this.mergeUnique(role1Ids, role2Ids)
        // Add relevant sections to the access constraints
        if (entitlementIds.length > 0) {
            accessConstraints.push({ type: AccessConstraintTypeEnum.Entitlement, ids: entitlementIds, operator: AccessConstraintOperatorEnum.Selected })
        }
        if (accessProfileIds.length > 0) {
            accessConstraints.push({ type: AccessConstraintTypeEnum.AccessProfile, ids: accessProfileIds, operator: AccessConstraintOperatorEnum.Selected })
        }
        if (roleIds.length > 0) {
            accessConstraints.push({ type: AccessConstraintTypeEnum.Role, ids: roleIds, operator: AccessConstraintOperatorEnum.Selected })
        }
        // Calculate metrics to be used on the aggregated policy
        const leftHandTotalCount = entitlement1Ids.length + accessProfile1Ids.length + role1Ids.length
        const rightHandTotalCount = entitlement2Ids.length + accessProfile2Ids.length + role2Ids.length
        const totalCount = entitlementIds.length + accessProfileIds.length + roleIds.length
        return [accessConstraints, leftHandTotalCount, rightHandTotalCount, totalCount]
    }

    buildEntitlementNameArray(items: any[]): string[] {
        let names: string[] = []
        items.forEach(item => names.push(`Source: ${item.source.name}, Type: ${item.schema}, Name: ${item.name}`))
        return names
    }

    buildNameArray(items: any[]): string[] {
        let names: string[] = []
        items.forEach(item => names.push(item.name))
        return names
    }

    buildPolicySchedule(scheduleConfig: string): Schedule | any {
        let schedule
        if (scheduleConfig == ScheduleType.Daily) {
            schedule = {
                type: ScheduleType.Daily,
                hours: {
                    type: ScheduleHoursTypeEnum.List,
                    values: this.hourlyScheduleDay
                }
            }
        } else if (scheduleConfig == ScheduleType.Weekly) {
            schedule = {
                type: ScheduleType.Weekly,
                hours: {
                    type: ScheduleHoursTypeEnum.List,
                    values: this.hourlyScheduleDay
                },
                days: {
                    type: ScheduleDaysTypeEnum.List,
                    values: this.weeklyScheduleDay
                }
            }
        } else if (scheduleConfig == ScheduleType.Monthly) {
            schedule = {
                type: ScheduleType.Monthly,
                hours: {
                    type: ScheduleHoursTypeEnum.List,
                    values: this.hourlyScheduleDay
                },
                days: {
                    type: ScheduleDaysTypeEnum.List,
                    values: this.monthlyScheduleDay
                }
            }
        }
        if (schedule)
            return schedule
    }

    buildCampaignSchedule(scheduleConfig: string): Schedule | undefined {
        let schedule
        if (scheduleConfig == ScheduleTypeEnum.Weekly) {
            schedule = {
                type: ScheduleTypeEnum.Weekly,
                hours: {
                    type: ScheduleHoursTypeEnum.List,
                    values: this.hourlyScheduleDay.slice(0, maxHoursPerCampaignSchedule)
                },
                days: {
                    type: ScheduleDaysTypeEnum.List,
                    values: this.weeklyScheduleDay.slice(0, maxWeeklyDaysPerCampaignSchedule)
                }
            }
        } else if (scheduleConfig == ScheduleTypeEnum.Monthly) {
            schedule = {
                type: ScheduleTypeEnum.Monthly,
                hours: {
                    type: ScheduleHoursTypeEnum.List,
                    values: this.hourlyScheduleDay.slice(0, maxHoursPerCampaignSchedule)
                },
                days: {
                    type: ScheduleDaysTypeEnum.List,
                    values: this.monthlyScheduleDay.slice(0, maxMonthlyDaysPerCampaignSchedule)
                }
            }
        }
        if (schedule)
            return schedule
    }

    async resolvePolicyOwner(policyConfig: PolicyConfig): Promise<any> {
        if (policyConfig.policyOwnerType == DtoType.Identity) {
            return await this.searchIdentityByAttribute(this.identityResolutionAttribute, policyConfig.policyOwner)
        } else if (policyConfig.policyOwnerType == DtoType.GovernanceGroup) {
            return await this.searchGovGroupByName(policyConfig.policyOwner)
        }
    }

    async resolveViolationOwner(policyConfig: PolicyConfig): Promise<any> {
        if (policyConfig.violationOwnerType == DtoType.Identity && policyConfig.violationOwner) {
            return await this.searchIdentityByAttribute(this.identityResolutionAttribute, policyConfig.violationOwner)
        } else if (policyConfig.violationOwnerType == DtoType.GovernanceGroup && policyConfig.violationOwner) {
            return await this.searchGovGroupByName(policyConfig.violationOwner)
        }
    }

    buildviolationOwnerAssignmentConfig(violationOwner: any): ViolationOwnerAssignmentConfig {
        let violationOwnerConfig: ViolationOwnerAssignmentConfig
        if (violationOwner) {
            violationOwnerConfig = { assignmentRule: ViolationOwnerAssignmentConfigAssignmentRuleEnum.Static, ownerRef: violationOwner }
        } else {
            violationOwnerConfig = { assignmentRule: ViolationOwnerAssignmentConfigAssignmentRuleEnum.Manager }
        }
        return violationOwnerConfig
    }

    async resolvePolicyRecipients(policyConfig: PolicyConfig, violationOwner: any, policyOwner: any): Promise<any[]> {
        let recipients = []
        if (policyConfig.violationOwnerType == DtoType.Identity && policyConfig.violationOwner) {
            // Return the violation manager
            recipients = [violationOwner]
        } else if (policyConfig.violationOwnerType == DtoType.GovernanceGroup && policyConfig.violationOwner) {
            // Resolve governance group members
            recipients = await this.findGovGroupMembers(violationOwner.id)
        }
        // Use the policy owner if no violation managers found
        if (!recipients || recipients.length == 0) {
            recipients = [policyOwner]
        }
        return recipients
    }

    async deletePolicy(policyId: string): Promise<string> {
        let errorMessage = ""
        // Delete the Policy via API
        const policyApi = new SODPolicyApi(this.apiConfig)
        const deletePolicyRequest: SODPolicyApiDeleteSodPolicyRequest = {
            id: policyId
        }
        try {
            await policyApi.deleteSodPolicy(deletePolicyRequest)
        } catch (error) {
            errorMessage = `Error deleting existing policy using SOD-Policies API: ${(error as Error).message}`
            let debugMessage = `Failed SOD-Policies API request: ${JSON.stringify(error)}`
            logger.error(deletePolicyRequest, errorMessage)
            logger.debug(debugMessage)
        }
        return errorMessage
    }

    async createPolicy(policyConfig: PolicyConfig, policyOwner: any, violationOwner: any, conflictingAccessCriteria: any): Promise<[errorMessage: string, policyId: string, policyQuery: string]> {
        let errorMessage = ""
        let policyId = ""
        let policyQuery = ""
        let policyState = policyConfig.policyState ? SodPolicyStateEnum.Enforced : SodPolicyStateEnum.NotEnforced
        // Submit the new Policy via API
        const policyApi = new SODPolicyApi(this.apiConfig)
        const newPolicyRequest: SODPolicyApiCreateSodPolicyRequest = {
            sodPolicy: {
                name: policyConfig.policyName,
                description: policyConfig.policyDescription,
                ownerRef: policyOwner,
                externalPolicyReference: policyConfig.externalReference,
                compensatingControls: policyConfig.mitigatingControls,
                correctionAdvice: policyConfig.correctionAdvice,
                state: policyState,
                tags: policyConfig.tags,
                violationOwnerAssignmentConfig: violationOwner,
                type: SodPolicyTypeEnum.ConflictingAccessBased,
                conflictingAccessCriteria: conflictingAccessCriteria
            }
        }
        try {
            const newPolicy = await policyApi.createSodPolicy(newPolicyRequest)
            if (newPolicy.data.id) {
                policyId = newPolicy.data.id
            }
            if (newPolicy.data.policyQuery) {
                policyQuery = newPolicy.data.policyQuery
            }
        } catch (error) {
            errorMessage = `Error creating a new Policy using SOD-Policies API: ${(error as Error).message}`
            let debugMessage = `Failed SOD-Policies API request: ${JSON.stringify(error)}`
            logger.error(newPolicyRequest, errorMessage)
            logger.debug(debugMessage)
        }
        return [errorMessage, policyId, policyQuery]
    }

    async updatePolicy(existingPolicyId: string, policyConfig: PolicyConfig, policyOwner: any, violationOwner: any, conflictingAccessCriteria: any): Promise<[errorMessage: string, policyQuery: string]> {
        let errorMessage = ""
        let policyQuery = ""
        let policyState = policyConfig.policyState ? SodPolicyStateEnum.Enforced : SodPolicyStateEnum.NotEnforced
        // Submit the patch Policy via API
        const policyApi = new SODPolicyApi(this.apiConfig)
        const patchPolicyRequest: SODPolicyApiPatchSodPolicyRequest = {
            id: existingPolicyId,
            jsonPatchOperation: [
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/name",
                    value: policyConfig.policyName
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/description",
                    value: policyConfig.policyDescription
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/ownerRef",
                    value: policyOwner
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/externalPolicyReference",
                    value: policyConfig.externalReference
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/compensatingControls",
                    value: policyConfig.mitigatingControls
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/correctionAdvice",
                    value: policyConfig.correctionAdvice
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/state",
                    value: policyState
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/tags",
                    value: policyConfig.tags
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/violationOwnerAssignmentConfig",
                    value: violationOwner
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/conflictingAccessCriteria",
                    value: conflictingAccessCriteria
                },
            ]
        }
        try {
            const patchedPolicy = await policyApi.patchSodPolicy(patchPolicyRequest)
            if (patchedPolicy.data.policyQuery) {
                policyQuery = patchedPolicy.data.policyQuery
            }
        } catch (error) {
            errorMessage = `Error updating existing Policy using SOD-Policies API: ${(error as Error).message}`
            let debugMessage = `Failed SOD-Policies API request: ${JSON.stringify(error)}`
            logger.error(patchPolicyRequest, errorMessage)
            logger.debug(debugMessage)
        }
        return [errorMessage, policyQuery]
    }

    async setPolicySchedule(policyId: string, policyConfig: PolicyConfig, policySchedule: any, policyRecipients: any): Promise<string> {
        let errorMessage = ""
        // Update the Policy Schedule via API
        const policyApi = new SODPolicyApi(this.apiConfig)
        const setPolicyScheduleRequest: SODPolicyApiPutPolicyScheduleRequest = {
            id: policyId,
            sodPolicySchedule: {
                name: `${policyConfig.policySchedule}: ${policyConfig.policyName}`,
                description: policyConfig.policyDescription,
                schedule: policySchedule,
                recipients: policyRecipients
            }
        }
        try {
            const newPolicySchedule = await policyApi.putPolicySchedule(setPolicyScheduleRequest)
        } catch (error) {
            errorMessage = `Error setting Policy Schedule using SOD-Policies API: ${(error as Error).message}`
            let debugMessage = `Failed SOD-Policies API request: ${JSON.stringify(error)}`
            logger.error(setPolicyScheduleRequest, errorMessage)
            logger.debug(debugMessage)
        }
        return errorMessage
    }

    async deletePolicyCampaign(campaignId: string): Promise<string> {
        let errorMessage = ""
        // Delete the Campaign via API
        const certsApi = new CertificationCampaignsApi(this.apiConfig)
        const deleteCampaignTemplareRequest: CertificationCampaignsApiDeleteCampaignTemplateRequest = {
            id: campaignId
        }
        try {
            await certsApi.deleteCampaignTemplate(deleteCampaignTemplareRequest)
        } catch (error) {
            errorMessage = `Error deleting existing campaign using Certification-Campaigns API: ${(error as Error).message}`
            let debugMessage = `Failed Certification-Campaigns API request: ${JSON.stringify(error)}`
            logger.error(deleteCampaignTemplareRequest, errorMessage)
            logger.debug(debugMessage)
        }
        return errorMessage
    }

    async createPolicyCampaign(policyConfig: PolicyConfig, policyQuery: string, accessConstraints: AccessConstraint[], violationOwner: CampaignAllOfSearchCampaignInfoReviewer | undefined, nullValue: any): Promise<[errorMessage: string, campaignId: string]> {
        let errorMessage = ""
        let campaignId = ""
        let reviewer
        if (policyConfig.violationOwnerType != ViolationOwnerAssignmentConfigAssignmentRuleEnum.Manager) {
            reviewer = violationOwner
        }
        // Create new campaign using API
        const certsApi = new CertificationCampaignsApi(this.apiConfig)
        const createCampaignRequest: CertificationCampaignsApiCreateCampaignTemplateRequest = {
            campaignTemplate: {
                name: policyConfig.certificationName,
                description: policyConfig.certificationDescription,
                deadlineDuration: this.campaignDuration,
                campaign: {
                    name: policyConfig.certificationName,
                    description: policyConfig.certificationDescription,
                    type: CampaignTypeEnum.Search,
                    correlatedStatus: CampaignCorrelatedStatusEnum.Correlated,
                    recommendationsEnabled: true,
                    emailNotificationEnabled: true,
                    sunsetCommentsRequired: true,
                    searchCampaignInfo: {
                        type: CampaignAllOfSearchCampaignInfoTypeEnum.Identity,
                        description: policyConfig.certificationDescription,
                        reviewer: reviewer,
                        query: policyQuery,
                        accessConstraints: accessConstraints
                    }
                },
                created: nullValue,
                modified: nullValue
            }
        }
        try {
            const newCampaign = await certsApi.createCampaignTemplate(createCampaignRequest)
            if (newCampaign.data.id) {
                campaignId = newCampaign.data.id
            }
        } catch (error) {
            errorMessage = `Error creating new Campaign using Certification-Campaigns API: ${(error as Error).message}`
            let debugMessage = `Failed Certification-Campaigns API request: ${JSON.stringify(error)}`
            logger.error(createCampaignRequest, errorMessage)
            logger.debug(debugMessage)
        }
        return [errorMessage, campaignId]
    }

    async updatePolicyCampaign(campaignId: string, policyConfig: PolicyConfig, policyQuery: string, accessConstraints: any, violationOwner: any): Promise<string> {
        let errorMessage = ""
        let reviewer = null
        if (policyConfig.violationOwnerType != ViolationOwnerAssignmentConfigAssignmentRuleEnum.Manager) {
            reviewer = violationOwner
        }
        // Update existing campaign using API
        const certsApi = new CertificationCampaignsApi(this.apiConfig)
        const patchCampaignRequest: CertificationCampaignsApiUpdateCampaignRequest = {
            id: campaignId,
            jsonPatchOperation: [
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/name",
                    value: policyConfig.certificationName
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/description",
                    value: policyConfig.certificationDescription
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/deadlineDuration",
                    value: this.campaignDuration
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/campaign/name",
                    value: policyConfig.certificationName
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/campaign/description",
                    value: policyConfig.certificationDescription
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/campaign/searchCampaignInfo/description",
                    value: policyConfig.certificationDescription
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/campaign/searchCampaignInfo/reviewer",
                    value: reviewer
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/campaign/searchCampaignInfo/query",
                    value: policyQuery
                },
                {
                    op: JsonPatchOperationOpEnum.Replace,
                    path: "/campaign/searchCampaignInfo/accessConstraints",
                    value: accessConstraints
                }
            ]
        }
        try {
            const newCampaign = await certsApi.patchCampaignTemplate(patchCampaignRequest)
        } catch (error) {
            errorMessage = `Error updating existing Campaign using Certification-Campaigns API: ${(error as Error).message}`
            let debugMessage = `Failed Certification-Campaigns API request: ${JSON.stringify(error)}`
            logger.error(patchCampaignRequest, errorMessage)
            logger.debug(debugMessage)
        }
        return errorMessage
    }

    async setCampaignSchedule(campaignId: string, campaignSchedule: Schedule): Promise<string> {
        let errorMessage = ""
        // Update the Campaign Schedule via API
        const certsApi = new CertificationCampaignsApi(this.apiConfig)
        const setCampaignScheduleRequest: CertificationCampaignsApiSetCampaignTemplateScheduleRequest = {
            id: campaignId,
            schedule: campaignSchedule
        }
        try {
            const newCampaignSchedule = await certsApi.setCampaignTemplateSchedule(setCampaignScheduleRequest)
        } catch (error) {
            errorMessage = `Error setting campaign schedule using Certification-Campaigns API: ${(error as Error).message}`
            let debugMessage = `Failed Certification-Campaigns API request: ${JSON.stringify(error)}`
            logger.error(setCampaignScheduleRequest, errorMessage)
            logger.debug(debugMessage)
        }
        return errorMessage
    }

    async processSodPolicyConfig(policyConfig: PolicyConfig): Promise<PolicyImpl> {
        logger.info(`### Processing policy [${policyConfig.policyName}] ###`)

        // Create Policy Implementation object
        let canProcess = true
        let errorMessages = []
        let policyImpl = new PolicyImpl(policyConfig.policyName)

        // Create common variables
        let errorMessage = ""
        let policyId = ""
        let policyQuery = ""

        if (policyConfig.actions.includes(actionDeleteAll)) {
            // Check if policy already exists
            const existingPolicy = await this.findExistingPolicy(policyConfig)
            if (existingPolicy && existingPolicy.id) {
                // Delete existing policy
                errorMessage = await this.deletePolicy(existingPolicy.id)
                // Update Policy Impl Object with any error messages
                if (errorMessage) {
                    errorMessages.push(errorMessage)
                } else {
                    // Set policyScheduleConfigured flag to true
                    policyImpl.attributes.policyDeleted = true
                }
            } else {
                errorMessages.push(`No Policy found by name [${policyConfig.certificationName}] to delete.`)
            }

        } else {
            // Find LeftHand & RightHand Entitlements using the Search API
            const query1Entitlemnts = await this.searchEntitlementsByQuery(policyConfig.query1)
            const query2Entitlemnts = await this.searchEntitlementsByQuery(policyConfig.query2)

            policyImpl.attributes.leftHandEntitlements = JSON.stringify(this.buildEntitlementNameArray(query1Entitlemnts))
            policyImpl.attributes.rightHandEntitlements = JSON.stringify(this.buildEntitlementNameArray(query2Entitlemnts))
            policyImpl.attributes.leftHandEntitlementCount = query1Entitlemnts.length
            policyImpl.attributes.rightHandEntitlementCount = query2Entitlemnts.length

            // Check if either side of the query exceeds the Identity Security Cloud limits
            if (query1Entitlemnts.length == 0) {
                canProcess = false
                errorMessages.push(`Entitlement Query 1 [${policyConfig.query1}] returns no entitlements`)
            }
            if (query2Entitlemnts.length == 0) {
                canProcess = false
                errorMessages.push(`Entitlement Query 2 [${policyConfig.query2}] returns no entitlements`)
            }

            // Check if either side of the query exceeds the Identity Security Cloud limits
            if (query1Entitlemnts.length > this.maxEntitlementsPerPolicySide) {
                canProcess = false
                errorMessages.push(`Entitlement Query 1 [${policyConfig.query1}] result exceeds Identity Security Cloud limit of ${this.maxEntitlementsPerPolicySide} entitlements`)
            }
            if (query2Entitlemnts.length > this.maxEntitlementsPerPolicySide) {
                canProcess = false
                errorMessages.push(`Entitlement Query 2 [${policyConfig.query2}] result exceeds Identity Security Cloud limit of ${this.maxEntitlementsPerPolicySide} entitlements`)
            }

            // Prepare Policy Owner refereneces
            const policyOwner = await this.resolvePolicyOwner(policyConfig)
            // Error if Policy Owner cannot be resolved/found
            if (!policyOwner) {
                canProcess = false
                errorMessages.push(`Unable to resolve Policy Owner. Type: ${policyConfig.policyOwnerType}, Value: ${policyConfig.policyOwner}`)
            }

            // Prepare Violation Owner refereneces
            const violationOwner = await this.resolveViolationOwner(policyConfig)
            // Error if Violation Owner cannot be resolved/found
            if (!violationOwner && policyConfig.violationOwnerType != ViolationOwnerAssignmentConfigAssignmentRuleEnum.Manager) {
                canProcess = false
                errorMessages.push(`Unable to resolve Violation Manager. Type: ${policyConfig.violationOwnerType}, Value: ${policyConfig.violationOwner}`)
            }

            // Build the Conflicting Access Criteria
            const conflictingAccessCriteria = this.buildPolicyConflictingAccessCriteria(policyConfig, query1Entitlemnts, query2Entitlemnts)

            // Create or Update an existing policy only if the canProcess flag is true
            if (canProcess) {
                // Check if policy already exists
                const violationOwnerAssignmentConfig = this.buildviolationOwnerAssignmentConfig(violationOwner)
                const existingPolicy = await this.findExistingPolicy(policyConfig)
                if (existingPolicy && existingPolicy.id) {
                    [errorMessage, policyQuery] = await this.updatePolicy(existingPolicy.id, policyConfig, policyOwner, violationOwnerAssignmentConfig, conflictingAccessCriteria)
                    policyId = existingPolicy.id
                } else {
                    // Create a new Policy
                    [errorMessage, policyId, policyQuery] = await this.createPolicy(policyConfig, policyOwner, violationOwnerAssignmentConfig, conflictingAccessCriteria)
                }
                // Stop processing if any errors come up
                if (errorMessage) {
                    errorMessages.push(errorMessage)
                    policyImpl.attributes.errorMessages = JSON.stringify(errorMessages)
                    return policyImpl
                }
                // Stop processing if no policy id returned
                if (!policyId) {
                    errorMessages.push(`No policy Id returned while processing the policy?`)
                    policyImpl.attributes.errorMessages = JSON.stringify(errorMessages)
                    return policyImpl
                }
                // Stop processing if no policy id returned
                if (!policyQuery) {
                    errorMessages.push(`No policyQuery Id returned while processing the policy?`)
                    policyImpl.attributes.errorMessages = JSON.stringify(errorMessages)
                    return policyImpl
                }
                policyImpl.attributes.policyQuery = policyQuery
                // Set policyConfigured flag to true
                policyImpl.attributes.policyConfigured = true
            } else {
                policyImpl.attributes.errorMessages = JSON.stringify(errorMessages)
                return policyImpl
            }

            // Configure the Policy Schedule if required
            if (policyConfig.actions.includes(actionSchedulePolicy)) {
                const policySchedule = this.buildPolicySchedule(policyConfig.policySchedule)
                if (policySchedule) {
                    const policyRecipients = await this.resolvePolicyRecipients(policyConfig, violationOwner, policyOwner)
                    errorMessage = await this.setPolicySchedule(policyId, policyConfig, policySchedule, policyRecipients)
                    // Update Policy Impl Object with any error messages
                    if (errorMessage) {
                        errorMessages.push(errorMessage)
                    } else {
                        // Set policyScheduleConfigured flag to true
                        policyImpl.attributes.policyScheduleConfigured = true
                    }
                } else {
                    errorMessages.push(`Unable to build policy schedule using schedule [${policyConfig.policySchedule}]`)
                }
            }

            // Calculate additional Policy Metrics
            // Find LeftHand & RightHand AccessProfiles using the Search API
            const query1AccessProfiles = await this.searchAccessProfilesbyEntitlements(query1Entitlemnts)
            const query2AccessProfiles = await this.searchAccessProfilesbyEntitlements(query2Entitlemnts)

            // Find LeftHand & RightHand Roles using the Search API
            const query1Roles = await this.searchRolesByAccessProfilesOrEntitlements(query1Entitlemnts, query1AccessProfiles)
            const query2Roles = await this.searchRolesByAccessProfilesOrEntitlements(query2Entitlemnts, query2AccessProfiles)

            // Build AccessProfile and Role Name lists
            const leftHandAccessProfiles = this.buildNameArray(query1AccessProfiles)
            const rightHandAccessProfiles = this.buildNameArray(query2AccessProfiles)
            const leftHandRoles = this.buildNameArray(query1Roles)
            const rightHandRoles = this.buildNameArray(query2Roles)

            // Build campaign access constrains and calculate policy campaign metrics
            let [accessConstraints, leftHandTotalCount, rightHandTotalCount, totalCount] = this.buildCampaignAccsesConstraints(query1Entitlemnts, query2Entitlemnts, query1AccessProfiles, query2AccessProfiles, query1Roles, query2Roles)

            // Update the Policy Impl object
            policyImpl.attributes.leftHandAccessProfiles = JSON.stringify(leftHandAccessProfiles)
            policyImpl.attributes.rightHandAccessProfiles = JSON.stringify(rightHandAccessProfiles)
            policyImpl.attributes.leftHandRoles = JSON.stringify(leftHandRoles)
            policyImpl.attributes.rightHandRoles = JSON.stringify(rightHandRoles)
            policyImpl.attributes.leftHandTotalCount = leftHandTotalCount
            policyImpl.attributes.rightHandTotalCount = rightHandTotalCount
            policyImpl.attributes.totalCount = totalCount

            // Configure the Policy Campaign if required
            if (policyConfig.actions.includes(actionCertifyViolations) && !policyConfig.actions.includes(actionDeleteCampaign)) {
                // Reset canProcess flag
                canProcess = true
                let campaignId = ""

                // Ensure the total number of access items did not exceed Identity Security Cloud limits
                if (totalCount > this.maxAccessItemsPerCampaign) {
                    canProcess = false
                    errorMessages.push(`Total number of access items to review exceeds Identity Security Cloud limit of ${this.maxAccessItemsPerCampaign} access items.`)
                }

                // Ensure a proper certification campaign name and description have been provided
                if (!policyConfig.certificationName) {
                    canProcess = false
                    errorMessages.push(`A Certification Campaign Name is required to define a Certification Campaign.`)
                }
                if (!policyConfig.certificationDescription) {
                    canProcess = false
                    errorMessages.push(`A Certification Campaign Description is required to define a Certification Campaign.`)
                }

                // Create or Update an existing campaign only if the canProcess flag is true
                if (canProcess) {
                    // Check if Campaign already exists
                    const existingCampaign = await this.findExistingCampaign(policyConfig)

                    if (existingCampaign && existingCampaign.id) {
                        // Update existing campaign
                        errorMessage = await this.updatePolicyCampaign(existingCampaign.id, policyConfig, policyQuery, accessConstraints, violationOwner)
                        campaignId = existingCampaign.id
                    } else {
                        // Create a new campaign
                        [errorMessage, campaignId] = await this.createPolicyCampaign(policyConfig, policyQuery, accessConstraints, violationOwner, null)
                    }
                    // Stop processing if any errors come up
                    if (errorMessage) {
                        errorMessages.push(errorMessage)
                        policyImpl.attributes.errorMessages = JSON.stringify(errorMessages)
                        return policyImpl
                    }
                    // Stop processing if no policy id returned
                    if (!campaignId) {
                        errorMessages.push(`No campaign Id returned while processing the policy?`)
                        policyImpl.attributes.errorMessages = JSON.stringify(errorMessages)
                        return policyImpl
                    }
                    // Set campaignConfigured flag to true
                    policyImpl.attributes.campaignConfigured = true
                    policyImpl.attributes.certificationName = policyConfig.certificationName
                } else {
                    policyImpl.attributes.errorMessages = JSON.stringify(errorMessages)
                    return policyImpl
                }

                // Configure the Campaign Schedule if required
                if (policyConfig.certificationSchedule) {
                    const campaignSchedule = this.buildCampaignSchedule(policyConfig.certificationSchedule)
                    if (campaignSchedule) {
                        errorMessage = await this.setCampaignSchedule(campaignId, campaignSchedule)
                        // Update Policy Impl Object with any error messages
                        if (errorMessage) {
                            errorMessages.push(errorMessage)
                        } else {
                            // Set campaignScheduleConfigured flag to true
                            policyImpl.attributes.campaignScheduleConfigured = true
                        }
                    } else {
                        errorMessages.push(`Unable to build campaign schedule using schedule [${policyConfig.certificationSchedule}]`)
                    }
                }
            }
        }

        // Delete the Policy Campaign if required
        if (policyConfig.actions.includes(actionDeleteCampaign) || policyConfig.actions.includes(actionDeleteAll)) {
            // Reset canProcess flag
            canProcess = true
            errorMessage = ""

            // Ensure a proper certification campaign name has been provided
            if (!policyConfig.certificationName) {
                canProcess = false
                errorMessages.push(`A Certification Campaign Name is required to delete it.`)
            }

            // Delete existing campaign only if the canProcess flag is true
            if (canProcess) {
                // Check if Campaign already exists
                const existingCampaign = await this.findExistingCampaign(policyConfig)

                if (existingCampaign && existingCampaign.id) {
                    // Delete existing campaign
                    errorMessage = await this.deletePolicyCampaign(existingCampaign.id)
                    // Update Policy Impl Object with any error messages
                    if (errorMessage) {
                        errorMessages.push(errorMessage)
                    } else {
                        // Set policyScheduleConfigured flag to true
                        policyImpl.attributes.campaignDeleted = true
                    }
                } else {
                    errorMessages.push(`No Certification Campaign found by name [${policyConfig.certificationName}] to delete.`)
                }
            }
        }


        logger.info(`### Finished processing policy [${policyConfig.policyName}] ###`)

        // Return final Policy Impl Object
        policyImpl.attributes.errorMessages = JSON.stringify(errorMessages)
        return policyImpl
    }

    async getAllAccounts(): Promise<any[]> {
        let policyImpls: any[] = []
        // Reading Policy Configurations from the Policy Configuration Source
        const policyConfigs = await this.getAllPolicyConfigs()
        for (const policyConfigObject of policyConfigs) {
            let policyConfig = new PolicyConfig(policyConfigObject)
            // Only Process SOD policies for now
            if (policyConfig.policyType == sodPolicyType) {
                policyImpls.push(this.processSodPolicyConfig(policyConfig))
            }
        }
        return policyImpls
    }

    async getAccount(identity: string): Promise<PolicyImpl | undefined> {
        const policyConfigObject = await this.getPolicyConfigByName(identity)
        if (policyConfigObject) {
            let policyConfig = new PolicyConfig(policyConfigObject)
            // Only Process SOD policies for now
            if (policyConfig.policyType == sodPolicyType) {
                return this.processSodPolicyConfig(policyConfig)
            }
        }
    }

    async testConnection(): Promise<any> {
        let sourceId = await this.getPolicyConfigSourceId()
        if (!sourceId) {
            return "Unable to retrieve the Policy Configuration Source ID using the Provided Source Name"
        }
    }

}