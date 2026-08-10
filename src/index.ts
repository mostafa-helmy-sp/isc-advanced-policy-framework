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
import { Account } from './types/sailpoint-api'
import { ConnectorConfig } from './config/connector-config'
import { IscClient, PolicyType } from './isc-client'
import { PolicyConfig, toPolicyConfigAccount } from './model/policy-config'
import { PolicyImpl } from './model/policy-impl'

async function processPolicyConfigs(
    policyConfigObjects: Account[],
    iscClient: IscClient,
    parallel: boolean,
    res: Response<StdAccountListOutput>
): Promise<void> {
    if (parallel) {
        logger.info('stdAccountList running in parallel mode')
        const policyImpls: Promise<PolicyImpl>[] = []
        for (const policyConfigObject of policyConfigObjects) {
            const policyConfig = new PolicyConfig(toPolicyConfigAccount(policyConfigObject))
            if (policyConfig.policyType === PolicyType.SOD) {
                policyImpls.push(iscClient.processSodPolicyConfig(policyConfig))
            }
        }
        for (const policyImpl of policyImpls) {
            res.send(await policyImpl)
        }
    } else {
        logger.info('stdAccountList running in serial mode')
        for (const policyConfigObject of policyConfigObjects) {
            const policyConfig = new PolicyConfig(toPolicyConfigAccount(policyConfigObject))
            if (policyConfig.policyType === PolicyType.SOD) {
                res.send(await iscClient.processSodPolicyConfig(policyConfig))
            }
        }
    }
}

export const connector = async () => {
    const config = (await readConfig()) as ConnectorConfig
    const iscClient = new IscClient(config)

    return createConnector()
        .stdTestConnection(async (_context: Context, _input: StdTestConnectionInput, res: Response<StdTestConnectionOutput>) => {
            const response = await iscClient.testConnection()
            if (response) {
                throw new ConnectorError(response)
            }
            logger.info('Test Successful')
            res.send({})
        })
        .stdAccountList(async (_context: Context, _input: StdAccountListInput, res: Response<StdAccountListOutput>) => {
            const policyConfigs = await iscClient.getAllPolicyConfigs()
            logger.info(`stdAccountList found ${policyConfigs.length} policies to process`)
            await processPolicyConfigs(policyConfigs, iscClient, iscClient.isParallelProcessing(), res)
        })
        .stdAccountRead(async (_context: Context, input: StdAccountReadInput, res: Response<StdAccountReadOutput>) => {
            logger.info(`stdAccountRead read account : ${input.identity}`)
            const account = await iscClient.getAccount(input.identity)
            if (account) {
                res.send(account)
            } else {
                logger.debug(`stdAccountRead could not find account : ${input.identity}`)
                res.send({ identity: input.identity, attributes: {} })
            }
        })
}
