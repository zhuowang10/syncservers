import tempfile
import asyncio
import os
from unittest import IsolatedAsyncioTestCase, mock
from syncservers.config import Config, ConfigSync


class Test(IsolatedAsyncioTestCase):

    def test_get_boolean(self):
        # case: no value
        self.assertEqual(Config.get_boolean({}, "my_key", "my_defauly_value"), "my_defauly_value")

        # case: true
        self.assertTrue(Config.get_boolean({
            "my_key": "true",
        }, "my_key", None))

        self.assertTrue(Config.get_boolean({
            "my_key": "1",
        }, "my_key", None))

        # case: false
        self.assertFalse(Config.get_boolean({
            "my_key": "my_value",
        }, "my_key", None))

    def test_get_split_list(self):
        # case: no value
        self.assertEqual(Config.get_split_list({}, "my_key", ' ', "my_defauly_value"), "my_defauly_value")

        # case: split
        self.assertEqual(Config.get_split_list({
            "my_key": "a b c d"
        }, "my_key", ' ', "my_defauly_value"), ["a", "b", "c", "d"])

    @mock.patch("os.path.getmtime", return_value=100)
    @mock.patch("syncservers.config.SyncServer")
    def test_config(self, mocked_sync_server, mocked_getmtime):
        # load config
        curr_path = os.path.dirname(os.path.abspath(__file__))
        config = Config(os.path.join(curr_path, "config", "test_config.ini"))
        config.load_config()

        # verify config
        # servers
        self.assertEqual(len(config._parsed_config._servers), 2)
        self.assertTrue("server1" in config._parsed_config._servers)
        self.assertTrue("server2" in config._parsed_config._servers)
        self.assertEqual(mocked_sync_server.call_count, 2)
        mocked_sync_server.assert_any_call(server_id="server1", server_url="value1")
        mocked_sync_server.assert_any_call(server_id="server2", server_url="value2")

        # live paths
        paths = config._parsed_config._path_type_mappings["live"]
        self.assertEqual(len(paths), 1)
        path_configs, server_list = paths["/tmp/d1"]
        self.assertDictEqual(path_configs, {})
        _, dst_path = server_list[0]
        self.assertEqual(dst_path, "/sync/d1")
        _, dst_path = server_list[1]
        self.assertEqual(dst_path, "/my_folder/d1")

        # cron paths
        paths = config._parsed_config._path_type_mappings["cron"]
        self.assertEqual(len(paths), 2)
        path_configs, server_list = paths["/tmp/d1"]
        self.assertDictEqual(path_configs, {"cron": "*/4 * * * *"})
        _, dst_path = server_list[0]
        self.assertEqual(dst_path, "/sync/d1")
        path_configs, server_list = paths["/tmp/d2"]
        self.assertDictEqual(path_configs, {"cron": "*/5 * * * *"})
        _, dst_path = server_list[0]
        self.assertEqual(dst_path, "/sync/d2")

        # live config
        self.assertDictEqual(config._configs["live"], {
            "sync_options": "-avh --no-p --mkpath --timeout=5 --contimeout=3",
            "startup_sync_options": "-avh --no-p --mkpath --timeout=10 --contimeout=5",
            "startup_sync": "True",
            "retry_cron": "*/5 * * * *",
            "retry": "True",
            "max_retries": "2",
        })

        # cron config
        self.assertDictEqual(config._configs["cron"], {
            "sync_options": "-avh --no-p --mkpath --timeout=7 --contimeout=3",
        })

        # async_queue config
        self.assertDictEqual(config._configs["async_queue"], {
            "max_concurrent_tasks": "3",
        })

    async def read_events(self, watcher, events):
        async for event in watcher.watch_recursive():
            print(f"path: {event.path.resolve()} event: {event}")
            events.append(event)

    async def test_config_sync(self):
        with mock.patch("syncservers.config.Config.load_config") as mocked_load_config:
            with tempfile.TemporaryDirectory() as tmpdirname:
                config_folder_path = os.path.join(tmpdirname, "config")
                os.mkdir(config_folder_path)
                config_file_path = os.path.join(config_folder_path, "test.ini")

                # create config file
                with open(config_file_path, "w") as f:
                    f.write("config")

                # start monitor
                config = Config(config_file_path)
                cs = ConfigSync(config)
                task = cs.run()

                await asyncio.sleep(0.3)
                self.assertEqual(mocked_load_config.call_count, 0)

                # modify config file
                with open(config_file_path, "a") as f:
                    f.write("config")

                await asyncio.sleep(0.3)
                self.assertEqual(mocked_load_config.call_count, 1)

                # create another file
                another_file_path = os.path.join(config_folder_path, "another_file.ini")
                with open(another_file_path, "w") as f:
                    f.write("config")

                await asyncio.sleep(0.3)
                self.assertEqual(mocked_load_config.call_count, 1)

                # modify another file
                with open(another_file_path, "a") as f:
                    f.write("config")
                await asyncio.sleep(0.3)
                self.assertEqual(mocked_load_config.call_count, 1)

                # modify config file again
                with open(config_file_path, "a") as f:
                    f.write("config")

                await asyncio.sleep(0.3)
                self.assertEqual(mocked_load_config.call_count, 2)

                # end test
                task.cancel()
                asyncio.gather(task, return_exceptions=True)
