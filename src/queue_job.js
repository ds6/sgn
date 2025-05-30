'use strict';

const _queueAsyncBuckets = new Map();
const _gcLimit = 10000;

async function _asyncQueueExecutor(queue, cleanup) {
    let offset = 0;

    async function execute() {
        const limit = Math.min(queue.length, _gcLimit);

        for (let i = offset; i < limit; i++) {
            const job = queue[i];
            try {
                const result = await job.awaitable();
                job.resolve(result);
            } catch (error) {
                job.reject(error);
            }
        }

        if (limit < queue.length) {
            if (limit >= _gcLimit) {
                queue.splice(0, limit);
                offset = 0;
            } else {
                offset = limit;
            }

            return execute();
        } else {
            cleanup();
        }
    }

    try {
        await execute();
    } catch (e) {
        console.error("Error during async queue execution:", e);
        cleanup();
    }
}

module.exports = function queueJob(bucket, awaitable) {
    if (typeof awaitable !== 'function') {
        throw new TypeError('awaitable must be an async function');
    }
    
    if (!awaitable.name) {
        Object.defineProperty(awaitable, 'name', { writable: true });
        if (typeof bucket === 'string') {
            awaitable.name = bucket;
        }
    }

    let isNewQueue = false;

    if (!_queueAsyncBuckets.has(bucket)) {
        _queueAsyncBuckets.set(bucket, []);
        isNewQueue = true;
    }

    const queue = _queueAsyncBuckets.get(bucket);

    const job = new Promise((resolve, reject) => {
        queue.push({ awaitable, resolve, reject });
    });

    if (isNewQueue) {
        _asyncQueueExecutor(queue, () => _queueAsyncBuckets.delete(bucket));
    }

    return job;
};