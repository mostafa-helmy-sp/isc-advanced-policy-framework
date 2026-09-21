import { Configuration, ConfigurationParameters } from '../types/sailpoint-api'
import { ConnectorConfig } from '../config/connector-config'
import { TOKEN_URL_PATH } from '../config/defaults'

/** Creates a SailPoint SDK Configuration with OAuth credentials. */
export function createApiConfig(config: ConnectorConfig): Configuration {
    const configurationParameters: ConfigurationParameters = {
        baseurl: config.apiUrl,
        clientId: config.clientId,
        clientSecret: config.clientSecret,
        tokenUrl: config.apiUrl + TOKEN_URL_PATH,
    }
    return new Configuration(configurationParameters)
}
