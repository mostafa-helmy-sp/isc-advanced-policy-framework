import { logger as sdkLogger } from '@sailpoint/connector-sdk'
import { logger, runWithPolicyLogger } from './logger'

type FakeLogger = Record<'debug' | 'info' | 'warn' | 'error', jest.Mock>

function fakeLogger(): FakeLogger {
    return { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
}

describe('policy-scoped logger', () => {
    const children = new Map<string, FakeLogger>()

    beforeEach(() => {
        children.clear()
        jest.spyOn(sdkLogger, 'child').mockImplementation(((bindings: { policyName: string }) => {
            const child = fakeLogger()
            children.set(bindings.policyName, child)
            return child
        }) as never)
    })

    afterEach(() => jest.restoreAllMocks())

    it('keeps each policy name on its own lines across concurrent, interleaved work', async () => {
        const tick = () => new Promise((resolve) => setTimeout(resolve, 1))
        const processPolicy = (name: string) =>
            runWithPolicyLogger(name, async () => {
                logger.info(`${name} start`)
                await tick()
                await Promise.all([tick().then(() => logger.info(`${name} parallel`))])
                logger.info(`${name} end`)
            })

        await Promise.all([processPolicy('A'), processPolicy('B')])

        const linesOf = (name: string) => children.get(name)?.info.mock.calls.map(([line]) => line)
        expect(linesOf('A')).toEqual(['A start', 'A parallel', 'A end'])
        expect(linesOf('B')).toEqual(['B start', 'B parallel', 'B end'])
    })

    it('passes details and message through to the policy logger', async () => {
        await runWithPolicyLogger('A', async () => logger.error({ query: 'q1' }, 'Search failed'))

        expect(children.get('A')?.error).toHaveBeenCalledWith({ query: 'q1' }, 'Search failed')
    })

    it('falls back to the root logger outside a policy', () => {
        const rootInfo = jest.spyOn(sdkLogger, 'info').mockImplementation(() => undefined)

        logger.info('stdAccountList found 2 policies to process')

        expect(rootInfo).toHaveBeenCalledWith('stdAccountList found 2 policies to process')
        expect(sdkLogger.child).not.toHaveBeenCalled()
    })
})
