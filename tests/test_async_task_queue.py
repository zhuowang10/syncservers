from unittest import IsolatedAsyncioTestCase, mock
from syncservers.config import Config
from syncservers.async_task_queue import AsyncTaskQueue


class Test(IsolatedAsyncioTestCase):

    def test_distribute_queue_index(self):
        config = Config(None)
        mocked_get_config = config.get_config = mock.Mock(return_value={
            "max_concurrent_tasks": "2",
        })
        q = AsyncTaskQueue(config)
        # first task
        self.assertEqual(q._distribute_queue_index(), 0)

        # add a task to queue
        q._queues[0].put_nowait(0)
        self.assertEqual(q._distribute_queue_index(), 1)

        # add more
        q._queues[1].put_nowait(0)
        q._queues[1].put_nowait(0)
        self.assertEqual(q._distribute_queue_index(), 0)

        mocked_get_config.close()


    async def test_add_and_run_task(self):
        config = Config(None)
        mocked_get_config = config.get_config = mock.Mock(return_value={
            "max_concurrent_tasks": "2",
        })

        do_task = mock.AsyncMock()
        q = AsyncTaskQueue(config)

        # add a task with id id1
        q.add("id1", do_task)
        self.assertEqual(q._queues[0].qsize(), 1)
        self.assertEqual(q._queues[1].qsize(), 0)
        self.assertEqual(q._id_to_queue_index["id1"].q_i, 0)
        self.assertEqual(q._id_to_queue_index["id1"].tasks_n, 1)

        # add another task to id1
        q.add("id1", do_task)
        self.assertEqual(q._queues[0].qsize(), 2)
        self.assertEqual(q._queues[1].qsize(), 0)
        self.assertEqual(q._id_to_queue_index["id1"].q_i, 0)
        self.assertEqual(q._id_to_queue_index["id1"].tasks_n, 2)

        # run task in queue0
        await q._run_task(q._queues[0])
        self.assertEqual(q._queues[0].qsize(), 1)
        self.assertEqual(q._queues[1].qsize(), 0)
        self.assertEqual(q._id_to_queue_index["id1"].q_i, 0)
        self.assertEqual(q._id_to_queue_index["id1"].tasks_n, 1)

        # add task to id2
        q.add("id2", do_task)
        self.assertEqual(q._queues[0].qsize(), 1)
        self.assertEqual(q._queues[1].qsize(), 1)
        self.assertEqual(q._id_to_queue_index["id1"].q_i, 0)
        self.assertEqual(q._id_to_queue_index["id1"].tasks_n, 1)
        self.assertEqual(q._id_to_queue_index["id2"].q_i, 1)
        self.assertEqual(q._id_to_queue_index["id2"].tasks_n, 1)

        # run task in queue0
        await q._run_task(q._queues[0])
        self.assertEqual(q._queues[0].qsize(), 0)
        self.assertEqual(q._queues[1].qsize(), 1)
        self.assertEqual(q._id_to_queue_index["id1"].q_i, 0)
        self.assertEqual(q._id_to_queue_index["id1"].tasks_n, 0)
        self.assertEqual(q._id_to_queue_index["id2"].q_i, 1)
        self.assertEqual(q._id_to_queue_index["id2"].tasks_n, 1)

        # add task to id3
        q.add("id3", do_task)
        self.assertEqual(q._queues[0].qsize(), 1)
        self.assertEqual(q._queues[1].qsize(), 1)
        self.assertEqual(q._id_to_queue_index["id1"].q_i, 0)
        self.assertEqual(q._id_to_queue_index["id1"].tasks_n, 0)
        self.assertEqual(q._id_to_queue_index["id2"].q_i, 1)
        self.assertEqual(q._id_to_queue_index["id2"].tasks_n, 1)
        self.assertEqual(q._id_to_queue_index["id3"].q_i, 0)
        self.assertEqual(q._id_to_queue_index["id3"].tasks_n, 1)

        # run task in queue1
        await q._run_task(q._queues[1])
        self.assertEqual(q._queues[0].qsize(), 1)
        self.assertEqual(q._queues[1].qsize(), 0)
        self.assertEqual(q._id_to_queue_index["id1"].q_i, 0)
        self.assertEqual(q._id_to_queue_index["id1"].tasks_n, 0)
        self.assertEqual(q._id_to_queue_index["id2"].q_i, 1)
        self.assertEqual(q._id_to_queue_index["id2"].tasks_n, 0)
        self.assertEqual(q._id_to_queue_index["id3"].q_i, 0)
        self.assertEqual(q._id_to_queue_index["id3"].tasks_n, 1)

        # add another task to id1
        q.add("id1", do_task)
        self.assertEqual(q._queues[0].qsize(), 1)
        self.assertEqual(q._queues[1].qsize(), 1)
        self.assertEqual(q._id_to_queue_index["id1"].q_i, 1)
        self.assertEqual(q._id_to_queue_index["id1"].tasks_n, 1)
        self.assertEqual(q._id_to_queue_index["id2"].q_i, 1)
        self.assertEqual(q._id_to_queue_index["id2"].tasks_n, 0)
        self.assertEqual(q._id_to_queue_index["id3"].q_i, 0)
        self.assertEqual(q._id_to_queue_index["id3"].tasks_n, 1)

        # add another task to id1
        q.add("id1", do_task)
        self.assertEqual(q._queues[0].qsize(), 1)
        self.assertEqual(q._queues[1].qsize(), 2)
        self.assertEqual(q._id_to_queue_index["id1"].q_i, 1)
        self.assertEqual(q._id_to_queue_index["id1"].tasks_n, 2)
        self.assertEqual(q._id_to_queue_index["id2"].q_i, 1)
        self.assertEqual(q._id_to_queue_index["id2"].tasks_n, 0)
        self.assertEqual(q._id_to_queue_index["id3"].q_i, 0)
        self.assertEqual(q._id_to_queue_index["id3"].tasks_n, 1)

        mocked_get_config.close()
