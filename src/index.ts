import {
    Context,
    createConnector,
    readConfig,
    Response,
    logger,
    StdAccountListOutput,
    StdAccountReadInput,
    StdAccountReadOutput,
    StdTestConnectionOutput,
    StdAccountListInput,
    ConnectorError,
    StdTestConnectionInput,
} from '@sailpoint/connector-sdk'
import { IdnClient } from './idn-client'

export const connector = async () => {

    // Get connector source config
    const config = await readConfig()

    // Using SailPoint's TypeScript SDK to initialize the client
    const idnClient = new IdnClient(config)

    return createConnector()
        .stdTestConnection(async (context: Context, input: StdTestConnectionInput, res: Response<StdTestConnectionOutput>) => {
            const response = await idnClient.testConnection()
            if (response) {
                throw new ConnectorError(response)
            } else {
                logger.info(`Test Successful`)
                res.send({})
            }
        })
        .stdAccountList(async (context: Context, input: StdAccountListInput, res: Response<StdAccountListOutput>) => {
            const accounts = await idnClient.getAllAccounts()

            logger.info(`stdAccountList sent ${accounts.length} accounts`)
            for (const account of accounts) {
                res.send(await account)
            }
        })
        .stdAccountRead(async (context: Context, input: StdAccountReadInput, res: Response<StdAccountReadOutput>) => {
            logger.info(`stdAccountRead read account : ${input.identity}`)
            const account = await idnClient.getAccount(input.identity)
            if (account) {
                res.send(account)
            } else {
                logger.debug(`stdAccountRead could not find account : ${input.identity}`)
                res.send({
                    identity: input.identity,
                    attributes: {}
                })
            }
        })
}
