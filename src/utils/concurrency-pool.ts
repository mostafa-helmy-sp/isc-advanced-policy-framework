/** Runs async tasks with a maximum number of concurrent executions. Results preserve task order. */
export async function runWithConcurrencyLimit<T>(tasks: Array<() => Promise<T>>, limit: number): Promise<T[]> {
    if (tasks.length === 0) {
        return []
    }

    const effectiveLimit = Math.max(1, limit)
    const results: T[] = new Array(tasks.length)
    let nextTaskIndex = 0

    async function runWorker(): Promise<void> {
        while (nextTaskIndex < tasks.length) {
            const taskIndex = nextTaskIndex++
            results[taskIndex] = await tasks[taskIndex]()
        }
    }

    const workerCount = Math.min(effectiveLimit, tasks.length)
    await Promise.all(Array.from({ length: workerCount }, () => runWorker()))
    return results
}
