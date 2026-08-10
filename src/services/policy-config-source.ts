import { ConnectorError, logger } from '@sailpoint/connector-sdk'
import { AccountsApi, Account, Paginator, SourcesApi, Configuration } from '../types/sailpoint-api'

export class PolicyConfigSourceService {
    private policyConfigSourceId?: string

    constructor(
        private readonly apiConfig: Configuration,
        private readonly policyConfigSourceName: string
    ) {}

    async getPolicyConfigSourceId(): Promise<string | undefined> {
        if (this.policyConfigSourceId) {
            logger.debug(`Policy Config Source Id: [${this.policyConfigSourceId}]`)
            return this.policyConfigSourceId
        }

        const filter = `name eq "${this.policyConfigSourceName}"`
        const sourceApi = new SourcesApi(this.apiConfig)
        const sourcesRequest = { filters: filter }

        try {
            const sources = await sourceApi.listSourcesV1(sourcesRequest)
            if (sources.data.length > 0) {
                this.policyConfigSourceId = sources.data[0].id
            }
        } catch (error) {
            const errorMessage = `Error retrieving Policy Configurations Source ID using Sources API: ${error instanceof Error ? error.message : error}`
            logger.error(sourcesRequest, errorMessage)
            logger.debug(error, 'Failed Sources API request')
            throw new ConnectorError(errorMessage)
        }

        logger.debug(`Policy Config Source Id: [${this.policyConfigSourceId}]`)
        return this.policyConfigSourceId
    }

    async getAllPolicyConfigs(): Promise<Account[]> {
        const sourceId = await this.getPolicyConfigSourceId()
        const filter = `sourceId eq "${sourceId}"`
        const accountsApi = new AccountsApi(this.apiConfig)
        const accountsRequest = { filters: filter }

        try {
            const accounts = await Paginator.paginate(accountsApi, accountsApi.listAccountsV1, accountsRequest)
            logger.debug(`Found ${accounts.data.length} Policy Configurations`)
            return accounts.data
        } catch (error) {
            const errorMessage = `Error retrieving Policy Configurations from the Policy Config Source using ListAccounts API: ${error instanceof Error ? error.message : error}`
            logger.error(accountsRequest, errorMessage)
            logger.debug(error, 'Failed ListAccounts API request')
            throw new ConnectorError(errorMessage)
        }
    }

    async getPolicyConfigByName(policyName: string): Promise<Account | undefined> {
        const sourceId = await this.getPolicyConfigSourceId()
        const filter = `sourceId eq "${sourceId}" and name eq "${policyName}"`
        const accountsApi = new AccountsApi(this.apiConfig)
        const accountsRequest = { filters: filter }

        try {
            const accounts = await accountsApi.listAccountsV1(accountsRequest)
            logger.debug(`Found ${accounts.data.length} Policy Configurations`)
            return accounts.data[0]
        } catch (error) {
            const errorMessage = `Error retrieving single Policy Configuration from the Policy Config Source using ListAccounts API: ${error instanceof Error ? error.message : error}`
            logger.error(accountsRequest, errorMessage)
            logger.debug(error, 'Failed ListAccounts API request')
            throw new ConnectorError(errorMessage)
        }
    }
}
