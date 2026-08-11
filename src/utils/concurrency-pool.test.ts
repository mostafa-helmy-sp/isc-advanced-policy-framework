import { runWithConcurrencyLimit } from './concurrency-pool'

describe('runWithConcurrencyLimit', () => {
    it('returns empty array for no tasks', async () => {
        await expect(runWithConcurrencyLimit([], 5)).resolves.toEqual([])
    })

    it('returns all results in task order', async () => {
        const results = await runWithConcurrencyLimit(
            [0, 1, 2, 3, 4].map((value) => async () => value * 2),
            2
        )
        expect(results).toEqual([0, 2, 4, 6, 8])
    })

    it('limits concurrent executions', async () => {
        let active = 0
        let maxActive = 0

        const tasks = Array.from({ length: 8 }, () => async () => {
            active += 1
            maxActive = Math.max(maxActive, active)
            await new Promise((resolve) => setTimeout(resolve, 20))
            active -= 1
            return true
        })

        await runWithConcurrencyLimit(tasks, 3)
        expect(maxActive).toBeLessThanOrEqual(3)
    })
})
