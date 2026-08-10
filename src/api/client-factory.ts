import { logger } from '@sailpoint/connector-sdk'
import axiosRetry from 'axios-retry'
import { Configuration, ConfigurationParameters } from '../types/sailpoint-api'
import { ConnectorConfig } from '../config/connector-config'
import { TOKEN_URL_PATH } from '../config/defaults'

/** Creates a SailPoint SDK Configuration with OAuth credentials and 429 retry handling. */
export function createApiConfig(config: ConnectorConfig): Configuration {
    const configurationParameters: ConfigurationParameters = {
        baseurl: config.apiUrl,
        clientId: config.clientId,
        clientSecret: config.clientSecret,
        tokenUrl: config.apiUrl + TOKEN_URL_PATH,
    }
    const apiConfig = new Configuration(configurationParameters)
    apiConfig.retriesConfig = {
        retries: 10,
        retryDelay: (retryCount, error) => axiosRetry.exponentialDelay(retryCount, error, 2000),
        retryCondition: (error) => error.response?.status === 429,
        onRetry: (retryCount, error, requestConfig) => {
            logger.debug(`Retrying API [${requestConfig.url}] due to request error: [${error}]. Try number [${retryCount}]`)
        },
    }
    return apiConfig
}
