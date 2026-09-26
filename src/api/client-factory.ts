import { Configuration, ConfigurationParameters } from '../types/sailpoint-api'
import { ConnectorConfig } from '../config/connector-config'
import { TOKEN_URL_PATH } from '../config/defaults'
import { createApiAxiosInstance } from './axios-handlers'

/** Creates a SailPoint SDK Configuration with OAuth credentials and retrying HTTP handling for every API class. */
export function createApiConfig(config: ConnectorConfig): Configuration {
    const configurationParameters: ConfigurationParameters = {
        baseurl: config.apiUrl,
        clientId: config.clientId,
        clientSecret: config.clientSecret,
        tokenUrl: config.apiUrl + TOKEN_URL_PATH,
    }
    const apiConfig = new Configuration(configurationParameters)
    apiConfig.axiosInstance = createApiAxiosInstance()
    return apiConfig
}
