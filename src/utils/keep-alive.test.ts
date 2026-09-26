import { KEEP_ALIVE_INTERVAL_MS } from '../config/defaults'
import { withKeepAlive } from './keep-alive'

describe('withKeepAlive', () => {
    beforeEach(() => jest.useFakeTimers())
    afterEach(() => jest.useRealTimers())

    it('pings keepAlive while the work runs and stops once it finishes', async () => {
        const res = { keepAlive: jest.fn() }
        let finish: () => void = () => undefined
        const work = withKeepAlive(res, () => new Promise<void>((resolve) => (finish = resolve)))

        await jest.advanceTimersByTimeAsync(KEEP_ALIVE_INTERVAL_MS * 3)
        expect(res.keepAlive).toHaveBeenCalledTimes(3)

        finish()
        await work
        await jest.advanceTimersByTimeAsync(KEEP_ALIVE_INTERVAL_MS * 3)
        expect(res.keepAlive).toHaveBeenCalledTimes(3)
    })

    it('stops pinging when the work fails', async () => {
        const res = { keepAlive: jest.fn() }

        await expect(
            withKeepAlive(res, async () => {
                throw new Error('boom')
            })
        ).rejects.toThrow('boom')
        await jest.advanceTimersByTimeAsync(KEEP_ALIVE_INTERVAL_MS * 2)

        expect(res.keepAlive).not.toHaveBeenCalled()
    })
})
