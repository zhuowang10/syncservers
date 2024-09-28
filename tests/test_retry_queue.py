from syncservers.config import Config
from syncservers.async_task_queue import AsyncTaskQueue
from syncservers.retry_queue import RetryQueue
from unittest import IsolatedAsyncioTestCase
from unittest import mock


class Test(IsolatedAsyncioTestCase):

    def get_retry_config(self):
        return {
            "max_retries": "2",
        }

    async def test_add_retries_to_sync(self):
        config = Config(None)
        mocked_get_config = config.get_config = mock.Mock(return_value={
            "max_concurrent_tasks": "0",
        })
        task_queue = AsyncTaskQueue(config)
        retry_queue = RetryQueue(task_queue, self.get_retry_config)

        # case: no tasks in queue
        retry_queue._retry_queue.clear()
        mocked_task_queue_add = task_queue.add = mock.Mock()
        await retry_queue.add_retries_to_sync()
        self.assertEqual(mocked_task_queue_add.call_count, 0)
        mocked_task_queue_add.close()

        # case: with tasks
        retry_queue._retry_queue.clear()
        for retry_task in [
            ("my_path1", 0, "my_options1", "my_path1_file1", "my_dst1", False),
            ("my_path1", 1, "my_options2", "my_path1_file2", "my_dst2", False),
            ("my_path2", 2, "my_options3", "my_path2_file1", "my_dst3", False),
        ]:
            retry_queue.add(*retry_task)
        mocked_task_queue_add = task_queue.add = mock.Mock()
        await retry_queue.add_retries_to_sync()
        self.assertEqual(mocked_task_queue_add.call_count, 2)
        mocked_task_queue_add.assert_any_call(
            "my_path1",
            mock.ANY,
            base_sync_path="my_path1",
            retry_tasks=[
                (0, "my_options1", "my_path1_file1", "my_dst1", False),
                (1, "my_options2", "my_path1_file2", "my_dst2", False),
            ],
        )
        mocked_task_queue_add.assert_any_call(
            "my_path2",
            mock.ANY,
            base_sync_path="my_path2",
            retry_tasks=[
                (2, "my_options3", "my_path2_file1", "my_dst3", False),
            ],
        )
        mocked_task_queue_add.close()

        mocked_get_config.close()

    async def test_retry_sync(self):
        config = Config(None)
        mocked_get_config = config.get_config = mock.Mock(return_value={
            "max_concurrent_tasks": "0",
        })
        task_queue = AsyncTaskQueue(config)
        retry_queue = RetryQueue(task_queue, self.get_retry_config)

        # case: success
        with mock.patch("syncservers.retry_queue.async_rsync", return_value=True, new_callable=mock.AsyncMock) as mocked_async_rsync:
            retry_queue._retry_queue.clear()
            await retry_queue._retry_sync("/tmp/d1", [
                (0, "my_options1", "/tmp/d1/d2/f1", "my_dst_url1", False),
                (2, "my_options2", "/tmp/d1/f1", "my_dst_url2", False),
            ])

            self.assertEqual(mocked_async_rsync.call_count, 2)
            mocked_async_rsync.assert_any_call(
                "my_options1",
                "/tmp/d1/d2/f1",
                "my_dst_url1",
            )
            mocked_async_rsync.assert_any_call(
                "my_options2",
                "/tmp/d1/f1",
                "my_dst_url2",
            )

        # case: not success
        with mock.patch("syncservers.retry_queue.async_rsync", return_value=False, new_callable=mock.AsyncMock) as mocked_async_rsync:
            retry_queue._retry_queue.clear()
            await retry_queue._retry_sync("/tmp/d1", [
                (0, "my_options1", "/tmp/d1/d2/f1", "my_dst_url1", False),
                (2, "my_options2", "/tmp/d1/f1", "my_dst_url2", False),
            ])

            self.assertEqual(mocked_async_rsync.call_count, 2)
            mocked_async_rsync.assert_any_call(
                "my_options1",
                "/tmp/d1/d2/f1",
                "my_dst_url1",
            )
            mocked_async_rsync.assert_any_call(
                "my_options2",
                "/tmp/d1/f1",
                "my_dst_url2",
            )

            self.assertEqual(len(retry_queue._retry_queue), 1)
            self.assertEqual(retry_queue._retry_queue.popleft(), (
                "/tmp/d1",
                1,
                "my_options1",
                "/tmp/d1/d2/f1",
                "my_dst_url1",
                False,
            ))

        mocked_get_config.close()
