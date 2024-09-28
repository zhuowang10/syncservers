import os
from syncservers.config import Config
from syncservers.sync_server import SyncServer
from syncservers.async_task_queue import AsyncTaskQueue
from syncservers.syncs import BaseSync, LiveSync, CronSync
from unittest import IsolatedAsyncioTestCase
from unittest import mock


class Test(IsolatedAsyncioTestCase):

    def get_config(self):
        curr_path = os.path.dirname(os.path.abspath(__file__))
        config = Config(os.path.join(curr_path, "config", "test_config.ini"))
        config.load_config()
        return config

    def test_add_retry_to_schedule(self):
        sync = BaseSync(None, None)

        # case: retry not on
        schedule_jobs = []
        mocked_get_config = sync._get_config = mock.Mock(return_value={})
        sync.add_retry_to_schedule(schedule_jobs)
        self.assertEqual(len(schedule_jobs), 0)
        mocked_get_config.close()

        # case: retry
        schedule_jobs = []
        mocked_get_config = sync._get_config = mock.Mock(return_value={
            "retry": "true",
            "retry_cron": "my_retry_cron",
        })
        sync.add_retry_to_schedule(schedule_jobs)
        self.assertEqual(len(schedule_jobs), 1)
        self.assertEqual(schedule_jobs[0][1], "my_retry_cron")
        mocked_get_config.close()

    def test_get_server_dst_url(self):
        sync_server = SyncServer("my_server_id", "my_server_url")
        sync = BaseSync(None, None)

        # case: paths are the same
        mocked_get_sync_url = sync_server.get_sync_url = mock.Mock()
        sync._get_server_dst_url(sync_server, "/tmp/folder1", "/tmp/folder1", "my_base_dst_path")
        mocked_get_sync_url.assert_called_once_with("my_base_dst_path", "")
        mocked_get_sync_url.close()

        # case: sub path
        mocked_get_sync_url = sync_server.get_sync_url = mock.Mock()
        sync._get_server_dst_url(sync_server, "/tmp/folder1", "/tmp/folder1/folder2/folder3", "my_base_dst_path")
        mocked_get_sync_url.assert_called_once_with("my_base_dst_path", "folder2/folder3")
        mocked_get_sync_url.close()

    def test_add_to_startup_sync(self):
        config = self.get_config()
        task_queue = AsyncTaskQueue(config)
        sync = CronSync(config, task_queue)

        # case: startup sync not on
        mocked_get_config = sync._get_config = mock.Mock(return_value={})
        mocked_task_queue_add = task_queue.add = mock.Mock()
        sync.add_to_startup_sync()
        self.assertEqual(mocked_task_queue_add.call_count, 0)
        mocked_task_queue_add.close()
        mocked_get_config.close()

        # case: startup sync
        mocked_get_config = sync._get_config = mock.Mock(return_value={
            "startup_sync": "true",
            "startup_sync_options": "a b",
            "retry": "true",
        })
        mocked_task_queue_add = task_queue.add = mock.Mock()
        sync.add_to_startup_sync()
        self.assertEqual(mocked_task_queue_add.call_count, 2)
        mocked_task_queue_add.assert_any_call(
            "/tmp/d1",
            mock.ANY,
            base_sync_path="/tmp/d1",
            sync_path="/tmp/d1",
            sync_options=["a", "b"],
            is_folder=True,
            retry=True,
        )
        mocked_task_queue_add.assert_any_call(
            "/tmp/d2",
            mock.ANY,
            base_sync_path="/tmp/d2",
            sync_path="/tmp/d2",
            sync_options=["a", "b"],
            is_folder=True,
            retry=True,
        )
        mocked_task_queue_add.close()
        mocked_get_config.close()

    async def test_add_to_sync(self):
        config = self.get_config()
        task_queue = AsyncTaskQueue(config)
        sync = BaseSync(None, task_queue)

        mocked_get_config = sync._get_config = mock.Mock(return_value={
            "sync_options": "a b",
            "retry": "true",
        })
        mocked_task_queue_add = task_queue.add = mock.Mock()
        await sync._add_to_sync("my_base_sync_path", "my_sycn_path", "my_is_folder")
        mocked_task_queue_add.assert_called_once_with(
            "my_base_sync_path",
            mock.ANY,
            base_sync_path="my_base_sync_path",
            sync_path="my_sycn_path",
            sync_options=["a", "b"],
            is_folder="my_is_folder",
            retry=True,
        )
        mocked_task_queue_add.close()
        mocked_get_config.close()

    async def test_sync(self):
        config = self.get_config()
        sync = LiveSync(config, None)

        # case: success
        with mock.patch("syncservers.syncs.async_rsync", return_value=True, new_callable=mock.AsyncMock) as mocked_async_rsync:
            sync._retry_queue._retry_queue.clear()
            mocked_get_server_dst_url = sync._get_server_dst_url = mock.Mock(return_value="my_dst_url")
            await sync._sync("/tmp/d1", "/tmp/d1/d2/f1", "my_sync_options", True, True)

            self.assertEqual(mocked_get_server_dst_url.call_count, 2)
            mocked_get_server_dst_url.assert_any_call(mock.ANY, "/tmp/d1", "/tmp/d1/d2/f1", "/sync/d1")
            mocked_get_server_dst_url.assert_any_call(mock.ANY, "/tmp/d1", "/tmp/d1/d2/f1", "/my_folder/d1")

            self.assertEqual(mocked_async_rsync.call_count, 2)
            mocked_async_rsync.assert_any_call(
                "my_sync_options",
                "/tmp/d1/d2/f1/",
                "my_dst_url/",
            )
            mocked_async_rsync.assert_any_call(
                "my_sync_options",
                "/tmp/d1/d2/f1/",
                "my_dst_url/",
            )
            mocked_get_server_dst_url.close()

        # case: not success
        with mock.patch("syncservers.syncs.async_rsync", return_value=False, new_callable=mock.AsyncMock) as mocked_async_rsync:
            sync._retry_queue._retry_queue.clear()
            mocked_get_server_dst_url = sync._get_server_dst_url = mock.Mock(return_value="my_dst_url")
            await sync._sync("/tmp/d1", "/tmp/d1/d2/f1", "my_sync_options", True, True)

            self.assertEqual(mocked_get_server_dst_url.call_count, 2)
            mocked_get_server_dst_url.assert_any_call(mock.ANY, "/tmp/d1", "/tmp/d1/d2/f1", "/sync/d1")
            mocked_get_server_dst_url.assert_any_call(mock.ANY, "/tmp/d1", "/tmp/d1/d2/f1", "/my_folder/d1")

            self.assertEqual(mocked_async_rsync.call_count, 2)
            mocked_async_rsync.assert_any_call(
                "my_sync_options",
                "/tmp/d1/d2/f1/",
                "my_dst_url/",
            )
            mocked_async_rsync.assert_any_call(
                "my_sync_options",
                "/tmp/d1/d2/f1/",
                "my_dst_url/",
            )

            self.assertEqual(len(sync._retry_queue._retry_queue), 2)
            self.assertEqual(sync._retry_queue._retry_queue.popleft(), (
                "/tmp/d1",
                0,
                "my_sync_options",
                "/tmp/d1/d2/f1",
                "my_dst_url",
                True,
            ))
            self.assertEqual(sync._retry_queue._retry_queue.popleft(), (
                "/tmp/d1",
                0,
                "my_sync_options",
                "/tmp/d1/d2/f1",
                "my_dst_url",
                True,
            ))
            mocked_get_server_dst_url.close()

    @mock.patch("asyncio.create_task", return_value="my_task")
    def test_live_run(self, mocked_create_task):
        config = self.get_config()

        live_sync = LiveSync(config, None)
        mocked_watch_path = live_sync._watch_path = mock.Mock()

        self.assertEqual(live_sync.run(), ["my_task"])
        mocked_watch_path.assert_called_once_with("/tmp/d1")
        self.assertEqual(mocked_create_task.call_count, 1)

        mocked_watch_path.close()

    def test_cron_add_to_schedule(self):
        schedule_jobs = []
        config = self.get_config()
        sync = CronSync(config, None)

        sync.add_to_schedule(schedule_jobs)
        self.assertEqual(len(schedule_jobs), 2)
        self.assertEqual(schedule_jobs[0][1], "*/4 * * * *")
        self.assertDictEqual(schedule_jobs[0][3], {
            "base_sync_path": "/tmp/d1",
            "sync_path": "/tmp/d1",
            "is_folder": True,
        })
        self.assertEqual(schedule_jobs[1][1], "*/5 * * * *")
        self.assertDictEqual(schedule_jobs[1][3], {
            "base_sync_path": "/tmp/d2",
            "sync_path": "/tmp/d2",
            "is_folder": True,
        })
