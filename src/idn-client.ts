import {
    ConnectorError,
    logger
} from "@sailpoint/connector-sdk"
import {
    Configuration,
    ConfigurationParameters,
    SourcesApi,
    SourcesApiListSourcesRequest,
    AccountsApi,
    AccountsApiListAccountsRequest,
    SearchApi,
    Search,
    Paginator,
    SodPolicy,
    SodPolicyStateEnum,
    SODPolicyApi,
    SODPolicyApiListSodPoliciesRequest,
    SODPolicyApiCreateSodPolicyRequest,
    SODPolicyApiPatchSodPolicyRequest,
    SODPolicyApiPutPolicyScheduleRequest,
    SODPolicyApiDeleteSodPolicyRequest,
    Schedule,
    ScheduleType,
    ScheduleHoursTypeEnum,
    ScheduleDaysTypeEnum,
    CampaignTemplate,
    CertificationCampaignsApi,
    CertificationCampaignsApiUpdateCampaignRequest,
    CertificationCampaignsApiListCampaignTemplatesRequest,
    CertificationCampaignsApiDeleteCampaignTemplateRequest,
    CertificationCampaignsApiCreateCampaignTemplateRequest,
    CertificationCampaignsBetaApi,
    CertificationCampaignsBetaApiSetCampaignTemplateScheduleRequest,
    ScheduleBeta,
    ScheduleBetaTypeEnum,
    ScheduleHoursBetaTypeEnum,
    ScheduleDaysBetaTypeEnum,
    GovernanceGroupsBetaApi,
    GovernanceGroupsBetaApiListWorkgroupsRequest,
    GovernanceGroupsBetaApiListWorkgroupMembersRequest
} from "sailpoint-api-client"
import { PolicyConfig } from "./model/policy-config"
import { PolicyImpl } from "./model/policy-impl"
import axiosRetry from "axios-retry"

// Set IDN Global Variables
var tokenUrlPath = "/oauth/token"
var maxEntitlementsPerPolicySide = 50
var maxAccessItemsPerCampaign = 3000
var maxHoursPerCampaignSchedule = 1
var maxWeeklyDaysPerCampaignSchedule = 1
var maxMonthlyDaysPerCampaignSchedule = 4

// Set Source Config Global Defaults
var defaultIdentityResolutionAttribute = "name"
var defaultHourlyScheduleDay = ["9"]
var defaultWeeklyScheduleDay = ["MON"]
var defaultMonthlyScheduleDay = ["1"]
var defaultCampaignDuration = "P2W"

// Set Connector Values
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
            retryDelay: axiosRetry.exponentialDelay,
            retryCondition: (error) => {
                return axiosRetry.isNetworkError(error) ||
                    axiosRetry.isRetryableError(error) ||
                    error.response?.status === 429;
            },
            onRetry: (retryCount, error, requestConfig) => {
                logger.debug(`Retrying API (Try number ${retryCount}) due to request error: ${error}`)
            }
        }
        // configure the rest of the source parameters
        this.policyConfigSourceName = config.policyConfigSourceName
        this.policySourceName = config.policySourceName
        if (config.identityResolutionAttribute) {
            this.identityResolutionAttribute = config.identityResolutionAttribute
        } else {
            this.identityResolutionAttribute = defaultIdentityResolutionAttribute
        }
        if (config.hourlyScheduleDay) {
            if (Array.isArray(config.hourlyScheduleDay)) {
                this.hourlyScheduleDay = config.hourlyScheduleDay
            } else {
                this.hourlyScheduleDay = [config.hourlyScheduleDay]
            }
        } else {
            this.hourlyScheduleDay = defaultHourlyScheduleDay
        }
        if (config.weeklyScheduleDay) {
            if (Array.isArray(config.weeklyScheduleDay)) {
                this.weeklyScheduleDay = config.weeklyScheduleDay
            } else {
                this.weeklyScheduleDay = [config.weeklyScheduleDay]
            }
        } else {
            this.weeklyScheduleDay = defaultWeeklyScheduleDay
        }
        if (config.monthlyScheduleDay) {
            if (Array.isArray(config.monthlyScheduleDay)) {
                this.monthlyScheduleDay = config.monthlyScheduleDay
            } else {
                this.monthlyScheduleDay = [config.monthlyScheduleDay]
            }
        } else {
            this.monthlyScheduleDay = defaultMonthlyScheduleDay
        }
        if (config.campaignDuration) {
            this.campaignDuration = config.campaignDuration
        } else {
            this.campaignDuration = defaultCampaignDuration
        }
    }

    async getPolicyConfigSourceId(): Promise<any> {
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
            } catch (err) {
                let errorMessage = `Error retrieving Policy Configurations Source ID using Sources API ${JSON.stringify(err)} with request: ${JSON.stringify(sourcesRequest)}`
                logger.error(errorMessage, err)
                throw new ConnectorError(errorMessage)
            }
        }
        // Return set Source ID
        logger.debug(`Policy Config Source Id: [${this.policyConfigSourceId}]`)
        return this.policyConfigSourceId
    }

    async getAllPolicyConfigs(): Promise<any[]> {
        // Get Policy Config Source ID
        await this.getPolicyConfigSourceId()
        const filter = `sourceId eq "${this.policyConfigSourceId}"`
        // Use Accounts API to get the Policy configurations stored as accounts in the Policy Config Source
        const accountsApi = new AccountsApi(this.apiConfig)
        const accountsRequest: AccountsApiListAccountsRequest = {
            filters: filter
        }
        try {
            const accounts = await accountsApi.listAccounts(accountsRequest)
            logger.debug(`Found ${accounts.data.length} Policy Configurations`)
            return accounts.data
        } catch (err) {
            let errorMessage = `Error retrieving Policy Configurations from the Policy Config Source using ListAccounts API ${JSON.stringify(err)} with request: ${JSON.stringify(accountsRequest)}`
            logger.error(errorMessage, err)
            throw new ConnectorError(errorMessage)
        }
    }

    async getPolicyConfigByName(policyName: string): Promise<any> {
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
        } catch (err) {
            let errorMessage = `Error retrieving single Policy Configuration from the Policy Config Source using ListAccounts API ${JSON.stringify(err)} with request: ${JSON.stringify(accountsRequest)}`
            logger.error(errorMessage, err)
            throw new ConnectorError(errorMessage)
        }
    }

    async findExistingPolicy(policyConfig: PolicyConfig): Promise<SodPolicy> {
        const filter = `name eq "${policyConfig.policyName}"`
        const policyApi = new SODPolicyApi(this.apiConfig)
        const findPolicyRequest: SODPolicyApiListSodPoliciesRequest = {
            filters: filter
        }
        try {
            const existingPolicy = await policyApi.listSodPolicies(findPolicyRequest)
            // Check if no policy already exists
            if (existingPolicy.data.length == 0 || !existingPolicy.data[0].id) {
                return {}
            } else {
                return existingPolicy.data[0]
            }
        } catch (err) {
            let errorMessage = `Error finding existing Policy using SOD-Policies API ${JSON.stringify(err)} with request: ${JSON.stringify(findPolicyRequest)}`
            logger.error(errorMessage, err)
            return {}
        }
    }

    async findExistingCampaign(policyConfig: PolicyConfig): Promise<CampaignTemplate | any> {
        const filter = `name eq "${policyConfig.certificationName}"`
        const certsApi = new CertificationCampaignsApi(this.apiConfig)
        const findCampaignRequest: CertificationCampaignsApiListCampaignTemplatesRequest = {
            filters: filter
        }
        try {
            const existingCampaign = await certsApi.listCampaignTemplates(findCampaignRequest)
            // Check if no campaign already exists
            if (existingCampaign.data.length == 0 || !existingCampaign.data[0].id) {
                return {}
            } else {
                return existingCampaign.data[0]
            }
        } catch (err) {
            let errorMessage = `Error finding existing Campaign using Certification-Campaigns API ${JSON.stringify(err)} with request: ${JSON.stringify(findCampaignRequest)}`
            logger.error(errorMessage, err)
            return {}
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

    buildIdArray(items: any[]): any[] {
        let ids: any[] = []
        items.forEach(item => ids.push(item.id))
        return ids
    }

    mergeUnique(items1: any[], items2: any[]): any[] {
        return [... new Set([...items1, ...items2])]
    }

    async searchEntitlementsByQuery(query: string): Promise<any[]> {
        const searchApi = new SearchApi(this.apiConfig)
        const search: Search = {
            indices: [
                "entitlements"
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
            const entitlements = await Paginator.paginateSearchApi(searchApi, search)
            return entitlements.data
        } catch (err) {
            let errorMessage = `Error finding entitlements using Search API ${JSON.stringify(err)} with request: ${JSON.stringify(search)}`
            logger.error(errorMessage, err)
            return []
        }
    }

    async searchAccessProfilesbyEntitlements(entitlements: any[]): Promise<any[]> {
        if (!entitlements || entitlements.length == 0) {
            return []
        }
        const query = this.buildIdQuery(entitlements, "id:", " OR ", "@entitlements(", ")")
        const searchApi = new SearchApi(this.apiConfig)
        const search: Search = {
            indices: [
                "accessprofiles"
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
            const accessProfiles = await Paginator.paginateSearchApi(searchApi, search)
            return accessProfiles.data
        } catch (err) {
            let errorMessage = `Error finding access profiles using Search API ${JSON.stringify(err)} with request: ${JSON.stringify(search)}`
            logger.error(errorMessage, err)
            return []
        }
    }

    async searchRolesByAccessProfiles(accessProfiles: any[]): Promise<any[]> {
        if (!accessProfiles || accessProfiles.length == 0) {
            return []
        }
        const query = this.buildIdQuery(accessProfiles, "accessProfiles.id:", " OR ")
        const searchApi = new SearchApi(this.apiConfig)
        const search: Search = {
            indices: [
                "roles"
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
            const roles = await Paginator.paginateSearchApi(searchApi, search)
            return roles.data
        } catch (err) {
            let errorMessage = `Error finding roles using Search API ${JSON.stringify(err)} with request: ${JSON.stringify(search)}`
            logger.error(errorMessage, err)
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
                "identities"
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
                return { "id": identity.id, "name": identity.name, "type": identity._type.toUpperCase() }
            }
        } catch (err) {
            let errorMessage = `Error finding identity using Search API ${JSON.stringify(err)} with request: ${JSON.stringify(search)}`
            logger.error(errorMessage, err)
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
                return null
            } else {
                // Use the first governance group if more than one match
                const govGroup = existingGovGroup.data[0]
                return { "id": govGroup.id, "name": govGroup.name, "type": "GOVERNANCE_GROUP" }
            }
        } catch (err) {
            let errorMessage = `Error finding Governance Group using Governance-Groups API ${JSON.stringify(err)} with request: ${JSON.stringify(findGovGroupRequest)}`
            logger.error(errorMessage, err)
            return
        }
    }

    async findGovGroupMembers(govGroupId: string): Promise<any[]> {
        const govGroupApi = new GovernanceGroupsBetaApi(this.apiConfig)
        const findGovGroupMembersRequest: GovernanceGroupsBetaApiListWorkgroupMembersRequest = {
            workgroupId: govGroupId
        }
        try {
            const govGroupMembers = await govGroupApi.listWorkgroupMembers(findGovGroupMembersRequest)
            // Check if no governance group members exist
            if (govGroupMembers.data.length == 0) {
                return []
            } else {
                // Return the governance group members
                let members: any[] = []
                govGroupMembers.data.forEach(govGroupMember => members.push({ "id": govGroupMember.id, "type": "IDENTITY", "name": govGroupMember.name }))
                return members
            }
        } catch (err) {
            let errorMessage = `Error finding Governance Group members using Governance-Groups API ${JSON.stringify(err)} with request: ${JSON.stringify(findGovGroupMembersRequest)}`
            logger.error(errorMessage, err)
            return []
        }
    }

    buildConflictingAccessCriteriaList(items: any[]): any[] {
        let criteriaList: any[] = []
        items.forEach(item => criteriaList.push({ "id": item.id, "type": item.type.toUpperCase() }))
        return criteriaList
    }

    buildPolicyConflictingAccessCriteria(policyConfig: PolicyConfig, query1Entitlemnts: any[], query2Entitlemnts: any[]): any {
        // Build ID,Type,Name arrays
        const leftCriteria = this.buildConflictingAccessCriteriaList(query1Entitlemnts)
        const rightCriteria = this.buildConflictingAccessCriteriaList(query2Entitlemnts)
        // Build the conflicting access criteria
        const criteria = { "leftCriteria": { "name": policyConfig.query1Name, "criteriaList": leftCriteria }, "rightCriteria": { "name": policyConfig.query2Name, "criteriaList": rightCriteria } }
        return criteria
    }

    buildCampaignAccsesConstraints(entitlements1: any[], entitlements2: any[], accessProfiles1: any[], accessProfiles2: any[], roles1: any[], roles2: any[]): [accessConstraints: any[], leftHandTotalCount: number, rightHandTotalCount: number, totalCount: number] {
        let accessConstraints: any[] = []
        // Build ID only arrays
        const entitlement1Ids = this.buildIdArray(entitlements1)
        const entitlement2Ids = this.buildIdArray(entitlements2)
        const accessProfile1Ids = this.buildIdArray(accessProfiles1)
        const accessProfile2Ids = this.buildIdArray(accessProfiles2)
        const role1Ids = this.buildIdArray(roles1)
        const role2Ids = this.buildIdArray(roles2)
        // Merge left and right arrays uniquely
        const entitlementIds = this.mergeUnique(entitlement1Ids, entitlement2Ids)
        const accessProfileIds = this.mergeUnique(accessProfile1Ids, accessProfile2Ids)
        const roleIds = this.mergeUnique(role1Ids, role2Ids)
        // Add relevant sections to the access constraints
        if (entitlementIds.length > 0) {
            accessConstraints.push({ "type": "ENTITLEMENT", "ids": entitlementIds, "operator": "SELECTED" })
        }
        if (accessProfileIds.length > 0) {
            accessConstraints.push({ "type": "ACCESS_PROFILE", "ids": accessProfileIds, "operator": "SELECTED" })
        }
        if (roleIds.length > 0) {
            accessConstraints.push({ "type": "ROLE", "ids": roleIds, "operator": "SELECTED" })
        }
        // Calculate metrics to be used on the aggregated policy
        const leftHandTotalCount = entitlement1Ids.length + accessProfile1Ids.length + role1Ids.length
        const rightHandTotalCount = entitlement2Ids.length + accessProfile2Ids.length + role2Ids.length
        const totalCount = entitlementIds.length + accessProfileIds.length + roleIds.length
        return [accessConstraints, leftHandTotalCount, rightHandTotalCount, totalCount]
    }

    buildEntitlementNameArray(items: any[]): any[] {
        let names: any[] = []
        items.forEach(item => names.push(`Source: ${item.source.name}, Type: ${item.schema}, Name: ${item.name}`))
        return names
    }

    buildNameArray(items: any[]): any[] {
        let names: any[] = []
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

    buildCampaignSchedule(scheduleConfig: string): ScheduleBeta | undefined {
        let schedule
        if (scheduleConfig == ScheduleBetaTypeEnum.Weekly) {
            schedule = {
                type: ScheduleBetaTypeEnum.Weekly,
                hours: {
                    type: ScheduleHoursBetaTypeEnum.List,
                    values: this.hourlyScheduleDay.slice(0, maxHoursPerCampaignSchedule)
                },
                days: {
                    type: ScheduleDaysBetaTypeEnum.List,
                    values: this.weeklyScheduleDay.slice(0, maxWeeklyDaysPerCampaignSchedule)
                }
            }
        } else if (scheduleConfig == ScheduleBetaTypeEnum.Monthly) {
            schedule = {
                type: ScheduleBetaTypeEnum.Monthly,
                hours: {
                    type: ScheduleHoursBetaTypeEnum.List,
                    values: this.hourlyScheduleDay.slice(0, maxHoursPerCampaignSchedule)
                },
                days: {
                    type: ScheduleDaysBetaTypeEnum.List,
                    values: this.monthlyScheduleDay.slice(0, maxMonthlyDaysPerCampaignSchedule)
                }
            }
        }
        if (schedule)
            return schedule
    }

    async resolvePolicyOwner(policyConfig: PolicyConfig): Promise<any> {
        if (policyConfig.policyOwnerType == "IDENTITY") {
            return await this.searchIdentityByAttribute(this.identityResolutionAttribute, policyConfig.policyOwner)
        } else if (policyConfig.policyOwnerType == "GOVERNANCE_GROUP") {
            return await this.searchGovGroupByName(policyConfig.policyOwner)
        }
    }

    async resolveViolationOwner(policyConfig: PolicyConfig): Promise<any> {
        if (policyConfig.violationOwnerType == "IDENTITY" && policyConfig.violationOwner) {
            return await this.searchIdentityByAttribute(this.identityResolutionAttribute, policyConfig.violationOwner)
        } else if (policyConfig.violationOwnerType == "GOVERNANCE_GROUP" && policyConfig.violationOwner) {
            return await this.searchGovGroupByName(policyConfig.violationOwner)
        }
    }

    buildviolationOwnerAssignmentConfig(violationOwner: any): any {
        if (violationOwner) {
            return { "assignmentRule": "STATIC", "ownerRef": violationOwner }
        } else {
            return { "assignmentRule": "MANAGER", "ownerRef": null }
        }
    }

    async resolvePolicyRecipients(policyConfig: PolicyConfig, violationOwner: any, policyOwner: any): Promise<any[]> {
        let recipients = []
        if (policyConfig.violationOwnerType == "IDENTITY" && policyConfig.violationOwner) {
            // Return the violation manager
            recipients = [violationOwner]
        } else if (policyConfig.violationOwnerType == "GOVERNANCE_GROUP" && policyConfig.violationOwner) {
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
        } catch (err) {
            errorMessage = `Error deleting existing policy using SOD-Policies API ${JSON.stringify(err)} with request: ${JSON.stringify(deletePolicyRequest)}`
            logger.error(errorMessage, err)
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
                type: "CONFLICTING_ACCESS_BASED",
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
        } catch (err) {
            errorMessage = `Error creating a new Policy using SOD-Policies API ${JSON.stringify(err)} with request: ${JSON.stringify(newPolicyRequest)}`
            logger.error(errorMessage, err)
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
                    op: "replace",
                    path: "/name",
                    value: policyConfig.policyName
                },
                {
                    op: "replace",
                    path: "/description",
                    value: policyConfig.policyDescription
                },
                {
                    op: "replace",
                    path: "/ownerRef",
                    value: policyOwner
                },
                {
                    op: "replace",
                    path: "/externalPolicyReference",
                    value: policyConfig.externalReference
                },
                {
                    op: "replace",
                    path: "/compensatingControls",
                    value: policyConfig.mitigatingControls
                },
                {
                    op: "replace",
                    path: "/correctionAdvice",
                    value: policyConfig.correctionAdvice
                },
                {
                    op: "replace",
                    path: "/state",
                    value: policyState
                },
                {
                    op: "replace",
                    path: "/tags",
                    value: policyConfig.tags
                },
                {
                    op: "replace",
                    path: "/violationOwnerAssignmentConfig",
                    value: violationOwner
                },
                {
                    op: "replace",
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
        } catch (err) {
            errorMessage = `Error updating existing Policy using SOD-Policies API ${JSON.stringify(err)} with request: ${JSON.stringify(patchPolicyRequest)}`
            logger.error(errorMessage, err)
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
        } catch (err) {
            errorMessage = `Error setting Policy Schedule using SOD-Policies API ${JSON.stringify(err)} with request: ${JSON.stringify(setPolicyScheduleRequest)}`
            logger.error(errorMessage, err)
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
        } catch (err) {
            errorMessage = `Error deleting existing campaign using Certification-Campaigns API ${JSON.stringify(err)} with request: ${JSON.stringify(deleteCampaignTemplareRequest)}`
            logger.error(errorMessage, err)
        }
        return errorMessage
    }

    async createPolicyCampaign(policyConfig: PolicyConfig, policyQuery: string, accessConstraints: any, violationOwner: any, nullValue: any): Promise<[errorMessage: string, campaignId: string]> {
        let errorMessage = ""
        let campaignId = ""
        let reviewer = null
        if (policyConfig.violationOwnerType != "MANAGER") {
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
                    type: "SEARCH",
                    correlatedStatus: "CORRELATED",
                    recommendationsEnabled: true,
                    emailNotificationEnabled: true,
                    sunsetCommentsRequired: true,
                    searchCampaignInfo: {
                        type: "IDENTITY",
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
        } catch (err) {
            errorMessage = `Error creating new Campaign using Certification-Campaigns API ${JSON.stringify(err)} with request: ${JSON.stringify(createCampaignRequest)}`
            logger.error(errorMessage, err)
        }
        return [errorMessage, campaignId]
    }

    async updatePolicyCampaign(campaignId: string, policyConfig: PolicyConfig, policyQuery: string, accessConstraints: any, violationOwner: any): Promise<string> {
        let errorMessage = ""
        let reviewer = null
        if (policyConfig.violationOwnerType != "MANAGER") {
            reviewer = violationOwner
        }
        // Update existing campaign using API
        const certsApi = new CertificationCampaignsApi(this.apiConfig)
        const patchCampaignRequest: CertificationCampaignsApiUpdateCampaignRequest = {
            id: campaignId,
            jsonPatchOperation: [
                {
                    "op": "replace",
                    "path": "/name",
                    "value": policyConfig.certificationName
                },
                {
                    "op": "replace",
                    "path": "/description",
                    "value": policyConfig.certificationDescription
                },
                {
                    "op": "replace",
                    "path": "/deadlineDuration",
                    "value": this.campaignDuration
                },
                {
                    "op": "replace",
                    "path": "/campaign/name",
                    "value": policyConfig.certificationName
                },
                {
                    "op": "replace",
                    "path": "/campaign/description",
                    "value": policyConfig.certificationDescription
                },
                {
                    "op": "replace",
                    "path": "/campaign/searchCampaignInfo/description",
                    "value": policyConfig.certificationDescription
                },
                {
                    "op": "replace",
                    "path": "/campaign/searchCampaignInfo/reviewer",
                    "value": reviewer
                },
                {
                    "op": "replace",
                    "path": "/campaign/searchCampaignInfo/query",
                    "value": policyQuery
                },
                {
                    "op": "replace",
                    "path": "/campaign/searchCampaignInfo/accessConstraints",
                    "value": accessConstraints
                }
            ]
        }
        try {
            const newCampaign = await certsApi.patchCampaignTemplate(patchCampaignRequest)
        } catch (err) {
            errorMessage = `Error updating existing Campaign using Certification-Campaigns API ${JSON.stringify(err)} with request: ${JSON.stringify(patchCampaignRequest)}`
            logger.error(errorMessage, err)
        }
        return errorMessage
    }

    async setCampaignSchedule(campaignId: string, campaignSchedule: ScheduleBeta): Promise<string> {
        let errorMessage = ""
        // Update the Campaign Schedule via API
        const certsApi = new CertificationCampaignsBetaApi(this.apiConfig)
        const setCampaignScheduleRequest: CertificationCampaignsBetaApiSetCampaignTemplateScheduleRequest = {
            id: campaignId,
            scheduleBeta: campaignSchedule
        }
        try {
            const newCampaignSchedule = await certsApi.setCampaignTemplateSchedule(setCampaignScheduleRequest)
        } catch (err) {
            errorMessage = `Error setting campaign schedule using Certification-Campaigns API ${JSON.stringify(err)} with request: ${JSON.stringify(setCampaignScheduleRequest)}`
            logger.error(errorMessage, err)
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

            // Check if either side of the query exceeds the IdentityNow limits
            if (query1Entitlemnts.length == 0) {
                canProcess = false
                errorMessages.push(`Entitlement Query 1 [${policyConfig.query1}] returns no entitlements`)
            }
            if (query2Entitlemnts.length == 0) {
                canProcess = false
                errorMessages.push(`Entitlement Query 2 [${policyConfig.query2}] returns no entitlements`)
            }

            // Check if either side of the query exceeds the IdentityNow limits
            if (query1Entitlemnts.length > maxEntitlementsPerPolicySide) {
                canProcess = false
                errorMessages.push(`Entitlement Query 1 [${policyConfig.query1}] result exceeds IdentityNow limit of ${maxEntitlementsPerPolicySide} entitlements`)
            }
            if (query2Entitlemnts.length > maxEntitlementsPerPolicySide) {
                canProcess = false
                errorMessages.push(`Entitlement Query 2 [${policyConfig.query2}] result exceeds IdentityNow limit of ${maxEntitlementsPerPolicySide} entitlements`)
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
            if (!violationOwner && policyConfig.violationOwnerType != "MANAGER") {
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
            const query1Roles = await this.searchRolesByAccessProfiles(query1AccessProfiles)
            const query2Roles = await this.searchRolesByAccessProfiles(query2AccessProfiles)

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

                // Ensure the total number of access items did not exceed IdentityNow limits
                if (totalCount > maxAccessItemsPerCampaign) {
                    canProcess = false
                    errorMessages.push(`Total number of access items to review exceeds IdentityNow limit of ${maxAccessItemsPerCampaign} access items.`)
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
            if (policyConfig.policyType == "SOD") {
                policyImpls.push(this.processSodPolicyConfig(policyConfig))
            }
        }
        return policyImpls
    }

    async getAccount(identity: string): Promise<any> {
        const policyConfigObject = await this.getPolicyConfigByName(identity)
        if (policyConfigObject) {
            let policyConfig = new PolicyConfig(policyConfigObject)
            // Only Process SOD policies for now
            if (policyConfig.policyType == "SOD") {
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