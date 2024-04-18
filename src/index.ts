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
import { IscClient, PolicyType } from './isc-client'
import { PolicyConfig } from './model/policy-config'
import { PolicyImpl } from './model/policy-impl'

export const connector = async () => {

    // Get connector source config
    const config = await readConfig()

    // Using SailPoint's TypeScript SDK to initialize the client
    const iscClient = new IscClient(config)

    return createConnector()
        .stdTestConnection(async (context: Context, input: StdTestConnectionInput, res: Response<StdTestConnectionOutput>) => {
            const response = await iscClient.testConnection()
            if (response) {
                throw new ConnectorError(response)
            } else {
                logger.info(`Test Successful`)
                res.send({})
            }
        })
        .stdAccountList(async (context: Context, input: StdAccountListInput, res: Response<StdAccountListOutput>) => {
            // Reading Policy Configurations from the Policy Configuration Source
            const policyConfigs = await iscClient.getAllPolicyConfigs()
            logger.info(`stdAccountList found ${policyConfigs.length} policies to process`)
            if (iscClient.isParallelProcessing()) {
                logger.info(`stdAccountList running in parallel mode`)
                // Loop Policy Configuration objects and start processing in parallel
                const policyImpls: Promise<PolicyImpl>[] = []
                for (const policyConfigObject of policyConfigs) {
                    const policyConfig = new PolicyConfig(policyConfigObject)
                    // Only Process SOD policies for now
                    if (policyConfig.policyType === PolicyType.SOD) {
                        policyImpls.push(iscClient.processSodPolicyConfig(policyConfig))
                    }
                }
                // Await each promise before returning
                for (const policyImpl of policyImpls) {
                    res.send(await policyImpl)
                }
            } else {
                logger.info(`stdAccountList running in serial mode`)
                // Loop each Policy Configuration objects one by one in series
                for (const policyConfigObject of policyConfigs) {
                    const policyConfig = new PolicyConfig(policyConfigObject)
                    // Only Process SOD policies for now
                    if (policyConfig.policyType === PolicyType.SOD) {
                        res.send(await iscClient.processSodPolicyConfig(policyConfig))
                    }
                }
            }
        })
        .stdAccountRead(async (context: Context, input: StdAccountReadInput, res: Response<StdAccountReadOutput>) => {
            logger.info(`stdAccountRead read account : ${input.identity}`)
            const account = await iscClient.getAccount(input.identity)
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
