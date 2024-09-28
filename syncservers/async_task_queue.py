import asyncio
import logging


logger = logging.getLogger(__name__)


class _TaskQueueIndex:
    def __init__(self, q_i, tasks_n) -> None:
        self.q_i = q_i
        self.tasks_n = tasks_n


class AsyncTaskQueue:
    """
    1. tasks with the same id won't run concurrently
    2. max concurrency
    """

    DEFAULT_MAX_CONCURRENT_TASKS = 3

    def __init__(self, config) -> None:
        self._id_to_queue_index = {}
        self._config = config

        async_queue_config = config.get_config("async_queue")
        max_concurrent_tasks = int(async_queue_config.get(
            "max_concurrent_tasks", self.DEFAULT_MAX_CONCURRENT_TASKS
        ))
        self._queues = [asyncio.Queue() for _ in range(max_concurrent_tasks)]

    def add(self, queue_id, task_func, *task_args, **task_kwargs):
        tq = None
        if queue_id in self._id_to_queue_index:
            tq = self._id_to_queue_index[queue_id]
            if tq.tasks_n > 0:
                # queue_id was already distributed, add to the same queue
                self._queues[tq.q_i].put_nowait((queue_id, task_func, task_args, task_kwargs))
                # update tasks
                tq.tasks_n += 1
                return

        # distribute
        q_i = self._distribute_queue_index()
        self._queues[q_i].put_nowait((queue_id, task_func, task_args, task_kwargs))

        # update
        if tq:
            tq.q_i = q_i
            tq.tasks_n = 1
        else:
            tq = _TaskQueueIndex(q_i, 1)
            self._id_to_queue_index[queue_id] = tq

    def run(self):
        """
        async run task queue
        return asyncio tasks
        """
        return [asyncio.create_task(self._run_queue(q_i)) for q_i in range(len(self._queues))]


    def _distribute_queue_index(self):
        """
        get first queue with min size
        """
        min_index = 0
        min_size = -1
        for i in range(len(self._queues)):
            qsize = self._queues[i].qsize()

            # empty ok
            if qsize == 0:
                min_index = i
                break

            # find min
            if min_size < 0 or qsize < min_size:
                min_size = qsize
                min_index = i

        return min_index

    async def _run_task(self, q):
        queue_id, task_func, task_args, task_kwargs = await q.get()
        try:
            await task_func(*task_args, **task_kwargs)
        except Exception as ex:
            logger.info("error in run_task", ex)
        q.task_done()

        # update id map
        tq = self._id_to_queue_index[queue_id]
        tq.tasks_n -= 1

    async def _run_queue(self, q_i):
        q = self._queues[q_i]
        while True:
            await self._run_task(q)
