import {
    ConnectorError,
    logger
} from "@sailpoint/connector-sdk"
import {
    Configuration,
    ConfigurationParameters,
    SourcesV2025Api,
    SourcesV2025ApiListSourcesRequest,
    AccountV2025,
    AccountsV2025Api,
    AccountsV2025ApiListAccountsRequest,
    SearchApi,
    SearchV2025,
    IndexV2025,
    Paginator,
    EntitlementDocumentV2025,
    AccessProfileDocumentV2025,
    RoleDocumentV2025,
    IdentityDocumentV2025,
    GovernanceGroupsV2025Api,
    GovernanceGroupsV2025ApiListWorkgroupsRequest,
    GovernanceGroupsV2025ApiListWorkgroupMembersRequest,
    DtoTypeV2025,
    SodPolicyV2025,
    SodPolicyV2025StateV2025,
    SodPolicyV2025TypeV2025,
    SodPolicyConflictingAccessCriteriaV2025,
    ViolationOwnerAssignmentConfigV2025,
    ViolationOwnerAssignmentConfigV2025AssignmentRuleV2025,
    AccessCriteriaCriteriaListInnerV2025,
    AccessCriteriaCriteriaListInnerV2025TypeV2025,
    AccessConstraintV2025,
    AccessConstraintV2025TypeV2025,
    AccessConstraintV2025OperatorV2025,
    ScheduleV2025,
    ScheduleTypeV2025,
    ScheduleHoursV2025TypeV2025,
    ScheduleDaysV2025TypeV2025,
    SODPoliciesV2025Api,
    SODPoliciesV2025ApiListSodPoliciesRequest,
    SODPoliciesV2025ApiDeleteSodPolicyRequest,
    SODPoliciesV2025ApiCreateSodPolicyRequest,
    SODPoliciesV2025ApiPatchSodPolicyRequest,
    SODPoliciesV2025ApiPutPolicyScheduleRequest,
    CampaignTemplateV2025,
    CampaignV2025TypeV2025,
    CampaignV2025CorrelatedStatusV2025,
    CampaignAllOfSearchCampaignInfoV2025TypeV2025,
    CampaignAllOfSearchCampaignInfoReviewerV2025,
    CertificationCampaignsV2025Api,
    CertificationCampaignsV2025ApiGetCampaignTemplatesRequest,
    CertificationCampaignsV2025ApiUpdateCampaignRequest,
    CertificationCampaignsV2025ApiDeleteCampaignTemplateRequest,
    CertificationCampaignsV2025ApiCreateCampaignTemplateRequest,
    CertificationCampaignsV2025ApiSetCampaignTemplateScheduleRequest,
    JsonPatchOperationV2025OpV2025
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

export enum PolicyType {
    SOD = "SOD"
}

export enum PolicyAction {
    REPORT = "REPORT",
    CERTIFY = "CERTIFY",
    DELETE_ALL = "DELETE_ALL",
    DELETE_CAMPAIGN = "DELETE_CAMPAIGN"
}

export class IscClient {

    private readonly config: any
    private readonly apiConfig: Configuration
    private readonly policyConfigSourceName: string
    private policyConfigSourceId?: string
    private identityResolutionAttribute: string
    private hourlyScheduleDay: string[]
    private weeklyScheduleDay: string[]
    private monthlyScheduleDay: string[]
    private campaignDuration: string
    private maxEntitlementsPerPolicySide: number
    private maxAccessItemsPerCampaign: number
    private parallelProcessing: boolean

    createApiConfig() {
        // Configure the SailPoint SDK API Client
        const ConfigurationParameters: ConfigurationParameters = {
            baseurl: this.config.apiUrl,
            clientId: this.config.clientId,
            clientSecret: this.config.clientSecret,
            tokenUrl: this.config.apiUrl + tokenUrlPath,
        }
        const apiConfig = new Configuration(ConfigurationParameters)
        apiConfig.experimental = true
        apiConfig.retriesConfig = {
            retries: 10,
            retryDelay: (retryCount, error) => axiosRetry.exponentialDelay(retryCount, error, 2000),
            retryCondition: (error) => {
                return error.response?.status === 429;
            },
            onRetry: (retryCount, error, requestConfig) => {
                logger.debug(`Retrying API [${requestConfig.url}] due to request error: [${error}]. Try number [${retryCount}]`)
            }
        }
        return apiConfig
    }

    constructor(config: any) {
        this.config = config
        this.apiConfig = this.createApiConfig()
        // configure the rest of the source parameters
        this.policyConfigSourceName = config.policyConfigSourceName
        this.identityResolutionAttribute = config.identityResolutionAttribute ?? defaultIdentityResolutionAttribute
        this.hourlyScheduleDay = config.hourlyScheduleDay ? (Array.isArray(config.hourlyScheduleDay) ? config.hourlyScheduleDay : [config.hourlyScheduleDay]) : defaultHourlyScheduleDay
        this.weeklyScheduleDay = config.weeklyScheduleDay ? (Array.isArray(config.weeklyScheduleDay) ? config.weeklyScheduleDay : [config.weeklyScheduleDay]) : defaultWeeklyScheduleDay
        this.monthlyScheduleDay = config.monthlyScheduleDay ? (Array.isArray(config.monthlyScheduleDay) ? config.monthlyScheduleDay : [config.monthlyScheduleDay]) : defaultMonthlyScheduleDay
        this.campaignDuration = config.campaignDuration || defaultCampaignDuration
        this.maxEntitlementsPerPolicySide = config.maxEntitlementsPerPolicySide || defaultMaxEntitlementsPerPolicySide
        this.maxAccessItemsPerCampaign = config.maxAccessItemsPerCampaign || defaultMaxAccessItemsPerCampaign
        this.parallelProcessing = config.parallelProcessing || false
    }

    isParallelProcessing(): boolean {
        return this.parallelProcessing
    }

    async getPolicyConfigSourceId(): Promise<string | undefined> {
        let filter = `name eq "${this.policyConfigSourceName}"`
        // Check if Source ID is null
        if (!this.policyConfigSourceId) {
            // Get and set Source ID if not already set
            logger.debug("Policy Config Source ID not set, getting the ID using the Sources API")
            const sourceApi = new SourcesV2025Api(this.apiConfig)
            const sourcesRequest: SourcesV2025ApiListSourcesRequest = {
                filters: filter
            }
            try {
                const sources = await sourceApi.listSources(sourcesRequest)
                if (sources.data.length > 0) {
                    this.policyConfigSourceId = sources.data[0].id
                }
            } catch (error) {
                let errorMessage = `Error retrieving Policy Configurations Source ID using Sources API: ${error instanceof Error ? error.message : error}`
                logger.error(sourcesRequest, errorMessage)
                logger.debug(error, "Failed Sources API request")
                throw new ConnectorError(errorMessage)
            }
        }
        // Return set Source ID
        logger.debug(`Policy Config Source Id: [${this.policyConfigSourceId}]`)
        return this.policyConfigSourceId
    }

    async getAllPolicyConfigs(): Promise<AccountV2025[]> {
        // Get Policy Config Source ID
        await this.getPolicyConfigSourceId()
        const filter = `sourceId eq "${this.policyConfigSourceId}"`
        // Use Accounts API to get the Policy configurations stored as accounts in the Policy Config Source
        const accountsApi = new AccountsV2025Api(this.apiConfig)
        const accountsRequest: AccountsV2025ApiListAccountsRequest = {
            filters: filter
        }
        try {
            const accounts = await Paginator.paginate(accountsApi, accountsApi.listAccounts, { filters: filter })
            logger.debug(`Found ${accounts.data.length} Policy Configurations`)
            return accounts.data
        } catch (error) {
            let errorMessage = `Error retrieving Policy Configurations from the Policy Config Source using ListAccounts API: ${error instanceof Error ? error.message : error}`
            logger.error(accountsRequest, errorMessage)
            logger.debug(error, "Failed ListAccounts API request")
            throw new ConnectorError(errorMessage)
        }
    }

    async getPolicyConfigByName(policyName: string): Promise<AccountV2025> {
        // Get Policy Config Source ID
        await this.getPolicyConfigSourceId()
        const filter = `sourceId eq "${this.policyConfigSourceId}" and name eq "${policyName}"`
        // Use Accounts API to get the Policy configuration stored as an account in the Policy Config Source by name
        const accountsApi = new AccountsV2025Api(this.apiConfig)
        const accountsRequest: AccountsV2025ApiListAccountsRequest = {
            filters: filter
        }
        try {
            const accounts = await accountsApi.listAccounts(accountsRequest)
            logger.debug(`Found ${accounts.data.length} Policy Configurations`)
            return accounts.data[0]
        } catch (error) {
            let errorMessage = `Error retrieving single Policy Configuration from the Policy Config Source using ListAccounts API: ${error instanceof Error ? error.message : error}`
            logger.error(accountsRequest, errorMessage)
            logger.debug(error, "Failed ListAccounts API request")
            throw new ConnectorError(errorMessage)
        }
    }

    async findExistingPolicy(apiConfig: Configuration, policyConfig: PolicyConfig): Promise<SodPolicyV2025 | undefined> {
        const filter = `name eq "${policyConfig.policyName}"`
        const policyApi = new SODPoliciesV2025Api(apiConfig)
        const findPolicyRequest: SODPoliciesV2025ApiListSodPoliciesRequest = {
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
            let errorMessage = `Error finding existing Policy using SOD-Policies API: ${error instanceof Error ? error.message : error}`
            logger.error(findPolicyRequest, errorMessage)
            logger.debug(error, "Failed SOD-Policies API request")
            return
        }
    }

    async findExistingCampaign(apiConfig: Configuration, policyConfig: PolicyConfig): Promise<CampaignTemplateV2025 | undefined> {
        const filter = `name eq "${policyConfig.certificationName}"`
        const certsApi = new CertificationCampaignsV2025Api(apiConfig)
        const findCampaignRequest: CertificationCampaignsV2025ApiGetCampaignTemplatesRequest = {
            filters: filter
        }
        try {
            const existingCampaign = await certsApi.getCampaignTemplates(findCampaignRequest)
            // Check if no campaign already exists
            if (existingCampaign.data.length == 0 || !existingCampaign.data[0].id) {
                return
            } else {
                return existingCampaign.data[0]
            }
        } catch (error) {
            let errorMessage = `Error finding existing Campaign using Certification-Campaigns API: ${error instanceof Error ? error.message : error}`
            logger.error(findCampaignRequest, errorMessage)
            logger.debug(error, "Failed Certification-Campaigns API request")
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

    async searchEntitlementsByQuery(apiConfig: Configuration, query: string): Promise<EntitlementDocumentV2025[]> {
        const searchApi = new SearchApi(apiConfig)
        const search: SearchV2025 = {
            indices: [
                IndexV2025.Entitlements
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
            const entitlements = (await Paginator.paginateSearchApi(searchApi, search)).data as EntitlementDocumentV2025[]
            return entitlements
        } catch (error) {
            let errorMessage = `Error finding entitlements using Search API: ${error instanceof Error ? error.message : error}`
            logger.error(search, errorMessage)
            logger.debug(error, "Failed Search API request")
            return []
        }
    }

    async searchAccessProfilesbyEntitlements(apiConfig: Configuration, entitlements: any[]): Promise<AccessProfileDocumentV2025[]> {
        if (!entitlements || entitlements.length == 0) {
            return []
        }
        const query = this.buildIdQuery(entitlements, "id:", " OR ", "@entitlements(", ")")
        const searchApi = new SearchApi(apiConfig)
        const search: SearchV2025 = {
            indices: [
                IndexV2025.Accessprofiles
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
            const accessProfiles: AccessProfileDocumentV2025[] = (await Paginator.paginateSearchApi(searchApi, search)).data as AccessProfileDocumentV2025[]
            return accessProfiles
        } catch (error) {
            let errorMessage = `Error finding access profiles using Search API: ${error instanceof Error ? error.message : error}`
            logger.error(search, errorMessage)
            logger.debug(error, "Failed Search API request")
            return []
        }
    }

    async searchRolesByAccessProfilesOrEntitlements(apiConfig: Configuration, entitlements: any[], accessProfiles: any[]): Promise<RoleDocumentV2025[]> {
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
        const searchApi = new SearchApi(apiConfig)
        const search: SearchV2025 = {
            indices: [
                IndexV2025.Roles
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
            const roles: RoleDocumentV2025[] = (await Paginator.paginateSearchApi(searchApi, search)).data as RoleDocumentV2025[]
            return roles
        } catch (error) {
            let errorMessage = `Error finding roles using Search API: ${error instanceof Error ? error.message : error}`
            logger.error(search, errorMessage)
            logger.debug(error, "Failed Search API request")
            return []
        }
    }

    async searchIdentityByAttribute(apiConfig: Configuration, attribute: string, value: string): Promise<any> {
        const searchApi = new SearchApi(apiConfig)
        let query = ""
        if (attribute === "name" || attribute === "employeeNumber" || attribute === "id") {
            query = `${attribute}.exact:"${value}"`
        } else {
            query = `attributes.${attribute}.exact:"${value}"`
        }
        const search: SearchV2025 = {
            indices: [
                IndexV2025.Identities
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
            const identities: IdentityDocumentV2025[] = await (await Paginator.paginateSearchApi(searchApi, search)).data as IdentityDocumentV2025[]
            // Check if no identity exists
            if (identities.length == 0) {
                return
            } else {
                // Use the first identity if more than one match
                const identity = identities[0]
                return { id: identity.id, name: identity.name, type: DtoTypeV2025.Identity }
            }
        } catch (error) {
            let errorMessage = `Error finding identity using Search API: ${error instanceof Error ? error.message : error}`
            logger.error(search, errorMessage)
            logger.debug(error, "Failed Search API request")
            return
        }
    }

    async searchGovGroupByName(apiConfig: Configuration, govGroupName: string): Promise<any> {
        const filter = `name eq "${govGroupName}"`
        const govGroupApi = new GovernanceGroupsV2025Api(apiConfig)
        const findGovGroupRequest: GovernanceGroupsV2025ApiListWorkgroupsRequest = {
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
                return { id: govGroup.id, name: govGroup.name, type: DtoTypeV2025.GovernanceGroup }
            }
        } catch (error) {
            let errorMessage = `Error finding Governance Group using Governance-Groups API: ${error instanceof Error ? error.message : error}`
            logger.error(findGovGroupRequest, errorMessage)
            logger.debug(error, "Failed Governance-Groups API request")
            return
        }
    }

    async findGovGroupMembers(apiConfig: Configuration, govGroupId: string): Promise<any[]> {
        const govGroupApi = new GovernanceGroupsV2025Api(apiConfig)
        const findGovGroupMembersRequest: GovernanceGroupsV2025ApiListWorkgroupMembersRequest = {
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
                govGroupMembers.data.forEach(govGroupMember => members.push({ id: govGroupMember.id, type: DtoTypeV2025.Identity, name: govGroupMember.name }))
                return members
            }
        } catch (error) {
            let errorMessage = `Error finding Governance Group members using Governance-Groups API: ${error instanceof Error ? error.message : error}`
            logger.error(findGovGroupMembersRequest, errorMessage)
            logger.debug(error, "Failed Governance-Groups API request")
            return []
        }
    }

    buildConflictingAccessCriteriaList(items: EntitlementDocumentV2025[]): AccessCriteriaCriteriaListInnerV2025[] {
        let criteriaList: AccessCriteriaCriteriaListInnerV2025[] = []
        items.forEach(item => criteriaList.push({ id: item.id, type: AccessCriteriaCriteriaListInnerV2025TypeV2025.Entitlement }))
        return criteriaList
    }

    buildPolicyConflictingAccessCriteria(policyConfig: PolicyConfig, query1Entitlemnts: EntitlementDocumentV2025[], query2Entitlemnts: EntitlementDocumentV2025[]): SodPolicyConflictingAccessCriteriaV2025 {
        // Build ID,Type,Name arrays
        const leftCriteria = this.buildConflictingAccessCriteriaList(query1Entitlemnts)
        const rightCriteria = this.buildConflictingAccessCriteriaList(query2Entitlemnts)
        // Build the conflicting access criteria
        const criteria: SodPolicyConflictingAccessCriteriaV2025 = {
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

    buildCampaignAccsesConstraints(entitlements1: EntitlementDocumentV2025[], entitlements2: EntitlementDocumentV2025[], accessProfiles1: AccessProfileDocumentV2025[], accessProfiles2: AccessProfileDocumentV2025[], roles1: RoleDocumentV2025[], roles2: RoleDocumentV2025[]): [accessConstraints: AccessConstraintV2025[], leftHandTotalCount: number, rightHandTotalCount: number, totalCount: number] {
        let accessConstraints: AccessConstraintV2025[] = []
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
            accessConstraints.push({ type: AccessConstraintV2025TypeV2025.Entitlement, ids: entitlementIds, operator: AccessConstraintV2025OperatorV2025.Selected })
        }
        if (accessProfileIds.length > 0) {
            accessConstraints.push({ type: AccessConstraintV2025TypeV2025.AccessProfile, ids: accessProfileIds, operator: AccessConstraintV2025OperatorV2025.Selected })
        }
        if (roleIds.length > 0) {
            accessConstraints.push({ type: AccessConstraintV2025TypeV2025.Role, ids: roleIds, operator: AccessConstraintV2025OperatorV2025.Selected })
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

    buildPolicySchedule(scheduleConfig: string): ScheduleV2025 | any {
        let schedule
        if (scheduleConfig == ScheduleTypeV2025.Daily) {
            schedule = {
                type: ScheduleTypeV2025.Daily,
                hours: {
                    type: ScheduleHoursV2025TypeV2025.List,
                    values: this.hourlyScheduleDay
                }
            }
        } else if (scheduleConfig == ScheduleTypeV2025.Weekly) {
            schedule = {
                type: ScheduleTypeV2025.Weekly,
                hours: {
                    type: ScheduleHoursV2025TypeV2025.List,
                    values: this.hourlyScheduleDay
                },
                days: {
                    type: ScheduleDaysV2025TypeV2025.List,
                    values: this.weeklyScheduleDay
                }
            }
        } else if (scheduleConfig == ScheduleTypeV2025.Monthly) {
            schedule = {
                type: ScheduleTypeV2025.Monthly,
                hours: {
                    type: ScheduleHoursV2025TypeV2025.List,
                    values: this.hourlyScheduleDay
                },
                days: {
                    type: ScheduleDaysV2025TypeV2025.List,
                    values: this.monthlyScheduleDay
                }
            }
        }
        if (schedule)
            return schedule
    }

    buildCampaignSchedule(scheduleConfig: string): ScheduleV2025 | undefined {
        let schedule
        if (scheduleConfig == ScheduleTypeV2025.Weekly) {
            schedule = {
                type: ScheduleTypeV2025.Weekly,
                hours: {
                    type: ScheduleHoursV2025TypeV2025.List,
                    values: this.hourlyScheduleDay.slice(0, maxHoursPerCampaignSchedule)
                },
                days: {
                    type: ScheduleDaysV2025TypeV2025.List,
                    values: this.weeklyScheduleDay.slice(0, maxWeeklyDaysPerCampaignSchedule)
                }
            }
        } else if (scheduleConfig == ScheduleTypeV2025.Monthly) {
            schedule = {
                type: ScheduleTypeV2025.Monthly,
                hours: {
                    type: ScheduleHoursV2025TypeV2025.List,
                    values: this.hourlyScheduleDay.slice(0, maxHoursPerCampaignSchedule)
                },
                days: {
                    type: ScheduleDaysV2025TypeV2025.List,
                    values: this.monthlyScheduleDay.slice(0, maxMonthlyDaysPerCampaignSchedule)
                }
            }
        }
        if (schedule)
            return schedule
    }

    async resolvePolicyOwner(apiConfig: Configuration, policyConfig: PolicyConfig): Promise<any> {
        if (policyConfig.policyOwnerType == DtoTypeV2025.Identity) {
            return await this.searchIdentityByAttribute(apiConfig, this.identityResolutionAttribute, policyConfig.policyOwner)
        } else if (policyConfig.policyOwnerType == DtoTypeV2025.GovernanceGroup) {
            return await this.searchGovGroupByName(apiConfig, policyConfig.policyOwner)
        }
    }

    async resolveViolationOwner(apiConfig: Configuration, policyConfig: PolicyConfig): Promise<any> {
        if (policyConfig.violationOwnerType == DtoTypeV2025.Identity && policyConfig.violationOwner) {
            return await this.searchIdentityByAttribute(apiConfig, this.identityResolutionAttribute, policyConfig.violationOwner)
        } else if (policyConfig.violationOwnerType == DtoTypeV2025.GovernanceGroup && policyConfig.violationOwner) {
            return await this.searchGovGroupByName(apiConfig, policyConfig.violationOwner)
        }
    }

    buildviolationOwnerAssignmentConfig(violationOwner: any): ViolationOwnerAssignmentConfigV2025 {
        let violationOwnerConfig: ViolationOwnerAssignmentConfigV2025
        if (violationOwner) {
            violationOwnerConfig = { assignmentRule: ViolationOwnerAssignmentConfigV2025AssignmentRuleV2025.Static, ownerRef: violationOwner }
        } else {
            violationOwnerConfig = { assignmentRule: ViolationOwnerAssignmentConfigV2025AssignmentRuleV2025.Manager }
        }
        return violationOwnerConfig
    }

    async resolvePolicyRecipients(apiConfig: Configuration, policyConfig: PolicyConfig, violationOwner: any, policyOwner: any): Promise<any[]> {
        let recipients = []
        if (policyConfig.violationOwnerType == DtoTypeV2025.Identity && policyConfig.violationOwner) {
            // Return the violation manager
            recipients = [violationOwner]
        } else if (policyConfig.violationOwnerType == DtoTypeV2025.GovernanceGroup && policyConfig.violationOwner) {
            // Resolve governance group members
            recipients = await this.findGovGroupMembers(apiConfig, violationOwner.id)
        }
        // Use the policy owner if no violation managers found
        if (!recipients || recipients.length == 0) {
            recipients = [policyOwner]
        }
        return recipients
    }

    async deletePolicy(apiConfig: Configuration, policyId: string): Promise<string> {
        let errorMessage = ""
        // Delete the Policy via API
        const policyApi = new SODPoliciesV2025Api(apiConfig)
        const deletePolicyRequest: SODPoliciesV2025ApiDeleteSodPolicyRequest = {
            id: policyId
        }
        try {
            await policyApi.deleteSodPolicy(deletePolicyRequest)
        } catch (error) {
            errorMessage = `Error deleting existing policy using SOD-Policies API: ${error instanceof Error ? error.message : error}`
            logger.error(deletePolicyRequest, errorMessage)
            logger.debug(error, "Failed SOD-Policies API request")
        }
        return errorMessage
    }

    async createPolicy(apiConfig: Configuration, policyConfig: PolicyConfig, policyOwner: any, violationOwner: any, conflictingAccessCriteria: any): Promise<[errorMessage: string, policyId: string, policyQuery: string]> {
        let errorMessage = ""
        let policyId = ""
        let policyQuery = ""
        let policyState = policyConfig.policyState ? SodPolicyV2025StateV2025.Enforced : SodPolicyV2025StateV2025.NotEnforced
        // Submit the new Policy via API
        const policyApi = new SODPoliciesV2025Api(apiConfig)
        const newPolicyRequest: SODPoliciesV2025ApiCreateSodPolicyRequest = {
            sodPolicyV2025: {
                name: policyConfig.policyName,
                description: policyConfig.policyDescription,
                ownerRef: policyOwner,
                externalPolicyReference: policyConfig.externalReference,
                compensatingControls: policyConfig.mitigatingControls,
                correctionAdvice: policyConfig.correctionAdvice,
                state: policyState,
                tags: policyConfig.tags,
                violationOwnerAssignmentConfig: violationOwner,
                type: SodPolicyV2025TypeV2025.ConflictingAccessBased,
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
            errorMessage = `Error creating a new Policy using SOD-Policies API: ${error instanceof Error ? error.message : error}`
            logger.error(newPolicyRequest, errorMessage)
            logger.debug(error, "Failed SOD-Policies API request")
        }
        return [errorMessage, policyId, policyQuery]
    }

    async updatePolicy(apiConfig: Configuration, existingPolicyId: string, policyConfig: PolicyConfig, policyOwner: any, violationOwner: any, conflictingAccessCriteria: any): Promise<[errorMessage: string, policyQuery: string]> {
        let errorMessage = ""
        let policyQuery = ""
        let policyState = policyConfig.policyState ? SodPolicyV2025StateV2025.Enforced : SodPolicyV2025StateV2025.NotEnforced
        // Submit the patch Policy via API
        const policyApi = new SODPoliciesV2025Api(apiConfig)
        const patchPolicyRequest: SODPoliciesV2025ApiPatchSodPolicyRequest = {
            id: existingPolicyId,
            jsonPatchOperationV2025: [
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/name",
                    value: policyConfig.policyName
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/description",
                    value: policyConfig.policyDescription
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/ownerRef",
                    value: policyOwner
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/externalPolicyReference",
                    value: policyConfig.externalReference
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/compensatingControls",
                    value: policyConfig.mitigatingControls
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/correctionAdvice",
                    value: policyConfig.correctionAdvice
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/state",
                    value: policyState
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/tags",
                    value: policyConfig.tags
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/violationOwnerAssignmentConfig",
                    value: violationOwner
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
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
            errorMessage = `Error updating existing Policy using SOD-Policies API: ${error instanceof Error ? error.message : error}`
            logger.error(patchPolicyRequest, errorMessage)
            logger.debug(error, "Failed SOD-Policies API request")
        }
        return [errorMessage, policyQuery]
    }

    async setPolicySchedule(apiConfig: Configuration, policyId: string, policyConfig: PolicyConfig, policySchedule: any, policyRecipients: any): Promise<string> {
        let errorMessage = ""
        // Update the Policy Schedule via API
        const policyApi = new SODPoliciesV2025Api(apiConfig)
        const setPolicyScheduleRequest: SODPoliciesV2025ApiPutPolicyScheduleRequest = {
            id: policyId,
            sodPolicyScheduleV2025: {
                name: `${policyConfig.policySchedule}: ${policyConfig.policyName}`,
                description: policyConfig.policyDescription,
                schedule: policySchedule,
                recipients: policyRecipients
            }
        }
        try {
            const newPolicySchedule = await policyApi.putPolicySchedule(setPolicyScheduleRequest)
        } catch (error) {
            errorMessage = `Error setting Policy Schedule using SOD-Policies API: ${error instanceof Error ? error.message : error}`
            logger.error(setPolicyScheduleRequest, errorMessage)
            logger.debug(error, "Failed SOD-Policies API request")
        }
        return errorMessage
    }

    async deletePolicyCampaign(apiConfig: Configuration, campaignId: string): Promise<string> {
        let errorMessage = ""
        // Delete the Campaign via API
        const certsApi = new CertificationCampaignsV2025Api(apiConfig)
        const deleteCampaignTemplareRequest: CertificationCampaignsV2025ApiDeleteCampaignTemplateRequest = {
            id: campaignId
        }
        try {
            await certsApi.deleteCampaignTemplate(deleteCampaignTemplareRequest)
        } catch (error) {
            errorMessage = `Error deleting existing campaign using Certification-Campaigns API: ${error instanceof Error ? error.message : error}`
            logger.error(deleteCampaignTemplareRequest, errorMessage)
            logger.debug(error, "Failed Certification-Campaigns API request")
        }
        return errorMessage
    }

    async createPolicyCampaign(apiConfig: Configuration, policyConfig: PolicyConfig, policyQuery: string, accessConstraints: AccessConstraintV2025[], violationOwner: CampaignAllOfSearchCampaignInfoReviewerV2025 | undefined, nullValue: any): Promise<[errorMessage: string, campaignId: string]> {
        let errorMessage = ""
        let campaignId = ""
        let reviewer
        if (policyConfig.violationOwnerType != ViolationOwnerAssignmentConfigV2025AssignmentRuleV2025.Manager) {
            reviewer = violationOwner
        }
        // Create new campaign using API
        const certsApi = new CertificationCampaignsV2025Api(apiConfig)
        const createCampaignRequest: CertificationCampaignsV2025ApiCreateCampaignTemplateRequest = {
            campaignTemplateV2025: {
                name: policyConfig.certificationName,
                description: policyConfig.certificationDescription,
                deadlineDuration: this.campaignDuration,
                campaign: {
                    name: policyConfig.certificationName,
                    description: policyConfig.certificationDescription,
                    type: CampaignV2025TypeV2025.Search,
                    correlatedStatus: CampaignV2025CorrelatedStatusV2025.Correlated,
                    recommendationsEnabled: true,
                    emailNotificationEnabled: true,
                    sunsetCommentsRequired: true,
                    searchCampaignInfo: {
                        type: CampaignAllOfSearchCampaignInfoV2025TypeV2025.Identity,
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
            errorMessage = `Error creating new Campaign using Certification-Campaigns API: ${error instanceof Error ? error.message : error}`
            logger.error(createCampaignRequest, errorMessage)
            logger.debug(error, "Failed Certification-Campaigns API request")
        }
        return [errorMessage, campaignId]
    }

    async updatePolicyCampaign(apiConfig: Configuration, campaignId: string, policyConfig: PolicyConfig, policyQuery: string, accessConstraints: any, violationOwner: any): Promise<string> {
        let errorMessage = ""
        let reviewer = null
        if (policyConfig.violationOwnerType != ViolationOwnerAssignmentConfigV2025AssignmentRuleV2025.Manager) {
            reviewer = violationOwner
        }
        // Update existing campaign using API
        const certsApi = new CertificationCampaignsV2025Api(apiConfig)
        const patchCampaignRequest: CertificationCampaignsV2025ApiUpdateCampaignRequest = {
            id: campaignId,
            jsonPatchOperationV2025: [
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/name",
                    value: policyConfig.certificationName
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/description",
                    value: policyConfig.certificationDescription
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/deadlineDuration",
                    value: this.campaignDuration
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/campaign/name",
                    value: policyConfig.certificationName
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/campaign/description",
                    value: policyConfig.certificationDescription
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/campaign/searchCampaignInfo/description",
                    value: policyConfig.certificationDescription
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/campaign/searchCampaignInfo/reviewer",
                    value: reviewer
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/campaign/searchCampaignInfo/query",
                    value: policyQuery
                },
                {
                    op: JsonPatchOperationV2025OpV2025.Replace,
                    path: "/campaign/searchCampaignInfo/accessConstraints",
                    value: accessConstraints
                }
            ]
        }
        try {
            const newCampaign = await certsApi.patchCampaignTemplate(patchCampaignRequest)
        } catch (error) {
            errorMessage = `Error updating existing Campaign using Certification-Campaigns API: ${error instanceof Error ? error.message : error}`
            logger.error(patchCampaignRequest, errorMessage)
            logger.debug(error, "Failed Certification-Campaigns API request")
        }
        return errorMessage
    }

    async setCampaignSchedule(apiConfig: Configuration, campaignId: string, campaignSchedule: ScheduleV2025): Promise<string> {
        let errorMessage = ""
        // Update the Campaign Schedule via API
        const certsApi = new CertificationCampaignsV2025Api(apiConfig)
        const setCampaignScheduleRequest: CertificationCampaignsV2025ApiSetCampaignTemplateScheduleRequest = {
            id: campaignId,
            scheduleV2025: campaignSchedule
        }
        try {
            const newCampaignSchedule = await certsApi.setCampaignTemplateSchedule(setCampaignScheduleRequest)
        } catch (error) {
            errorMessage = `Error setting campaign schedule using Certification-Campaigns API: ${error instanceof Error ? error.message : error}`
            logger.error(setCampaignScheduleRequest, errorMessage)
            logger.debug(error, "Failed Certification-Campaigns API request")
        }
        return errorMessage
    }

    async processSodPolicyConfig(policyConfig: PolicyConfig, apiConfig?: Configuration): Promise<PolicyImpl> {
        logger.info(`### Processing policy [${policyConfig.policyName}] ###`)

        // Create Policy Implementation object
        let canProcess = true
        let errorMessages = []
        let policyImpl = new PolicyImpl(policyConfig.policyName)

        // Create common variables
        let errorMessage = ""
        let policyId = ""
        let policyQuery = ""

        // Create a new API Client for each policy in parallel mode to minimize 429 errors due to using the same access_token
        if (!apiConfig) {
            apiConfig = this.parallelProcessing ? this.createApiConfig() : this.apiConfig
        }

        if (policyConfig.actions.includes(PolicyAction.DELETE_ALL)) {
            // Check if policy already exists
            const existingPolicy = await this.findExistingPolicy(apiConfig, policyConfig)
            if (existingPolicy && existingPolicy.id) {
                // Delete existing policy
                errorMessage = await this.deletePolicy(apiConfig, existingPolicy.id)
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
            const query1Entitlemnts = await this.searchEntitlementsByQuery(apiConfig, policyConfig.query1)
            const query2Entitlemnts = await this.searchEntitlementsByQuery(apiConfig, policyConfig.query2)

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
            const policyOwner = await this.resolvePolicyOwner(apiConfig, policyConfig)
            // Error if Policy Owner cannot be resolved/found
            if (!policyOwner) {
                canProcess = false
                errorMessages.push(`Unable to resolve Policy Owner. Type: ${policyConfig.policyOwnerType}, Value: ${policyConfig.policyOwner}`)
            }

            // Prepare Violation Owner refereneces
            const violationOwner = await this.resolveViolationOwner(apiConfig, policyConfig)
            // Error if Violation Owner cannot be resolved/found
            if (!violationOwner && policyConfig.violationOwnerType != ViolationOwnerAssignmentConfigV2025AssignmentRuleV2025.Manager) {
                canProcess = false
                errorMessages.push(`Unable to resolve Violation Manager. Type: ${policyConfig.violationOwnerType}, Value: ${policyConfig.violationOwner}`)
            }

            // Build the Conflicting Access Criteria
            const conflictingAccessCriteria = this.buildPolicyConflictingAccessCriteria(policyConfig, query1Entitlemnts, query2Entitlemnts)

            // Create or Update an existing policy only if the canProcess flag is true
            if (canProcess) {
                // Check if policy already exists
                const violationOwnerAssignmentConfig = this.buildviolationOwnerAssignmentConfig(violationOwner)
                const existingPolicy = await this.findExistingPolicy(apiConfig, policyConfig)
                if (existingPolicy && existingPolicy.id) {
                    [errorMessage, policyQuery] = await this.updatePolicy(apiConfig, existingPolicy.id, policyConfig, policyOwner, violationOwnerAssignmentConfig, conflictingAccessCriteria)
                    policyId = existingPolicy.id
                } else {
                    // Create a new Policy
                    [errorMessage, policyId, policyQuery] = await this.createPolicy(apiConfig, policyConfig, policyOwner, violationOwnerAssignmentConfig, conflictingAccessCriteria)
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
            if (policyConfig.actions.includes(PolicyAction.REPORT)) {
                const policySchedule = this.buildPolicySchedule(policyConfig.policySchedule)
                if (policySchedule) {
                    const policyRecipients = await this.resolvePolicyRecipients(apiConfig, policyConfig, violationOwner, policyOwner)
                    errorMessage = await this.setPolicySchedule(apiConfig, policyId, policyConfig, policySchedule, policyRecipients)
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
            const query1AccessProfiles = await this.searchAccessProfilesbyEntitlements(apiConfig, query1Entitlemnts)
            const query2AccessProfiles = await this.searchAccessProfilesbyEntitlements(apiConfig, query2Entitlemnts)

            // Find LeftHand & RightHand Roles using the Search API
            const query1Roles = await this.searchRolesByAccessProfilesOrEntitlements(apiConfig, query1Entitlemnts, query1AccessProfiles)
            const query2Roles = await this.searchRolesByAccessProfilesOrEntitlements(apiConfig, query2Entitlemnts, query2AccessProfiles)

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
            if (policyConfig.actions.includes(PolicyAction.CERTIFY) && !policyConfig.actions.includes(PolicyAction.DELETE_CAMPAIGN)) {
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
                    const existingCampaign = await this.findExistingCampaign(apiConfig, policyConfig)

                    if (existingCampaign && existingCampaign.id) {
                        // Update existing campaign
                        errorMessage = await this.updatePolicyCampaign(apiConfig, existingCampaign.id, policyConfig, policyQuery, accessConstraints, violationOwner)
                        campaignId = existingCampaign.id
                    } else {
                        // Create a new campaign
                        [errorMessage, campaignId] = await this.createPolicyCampaign(apiConfig, policyConfig, policyQuery, accessConstraints, violationOwner, null)
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
                        errorMessage = await this.setCampaignSchedule(apiConfig, campaignId, campaignSchedule)
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
        if (policyConfig.actions.includes(PolicyAction.DELETE_CAMPAIGN) || policyConfig.actions.includes(PolicyAction.DELETE_ALL)) {
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
                const existingCampaign = await this.findExistingCampaign(apiConfig, policyConfig)

                if (existingCampaign && existingCampaign.id) {
                    // Delete existing campaign
                    errorMessage = await this.deletePolicyCampaign(apiConfig, existingCampaign.id)
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

    async getAccount(identity: string): Promise<PolicyImpl | undefined> {
        const policyConfigObject = await this.getPolicyConfigByName(identity)
        if (policyConfigObject) {
            const policyConfig = new PolicyConfig(policyConfigObject)
            // Only Process SOD policies for now
            if (policyConfig.policyType === PolicyType.SOD) {
                return this.processSodPolicyConfig(policyConfig, this.apiConfig)
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
