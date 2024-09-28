import tempfile
import asyncio
import os
import shutil
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, mock
from asyncinotify import Inotify, Mask
from syncservers.recursive_watcher import RecursiveWatcher


class Test(IsolatedAsyncioTestCase):

    @staticmethod
    def mock_iterdir(path):
        name = path.name
        if name == "tmp":
            return [Path.joinpath(path, "level1.1"), Path.joinpath(path, "level1.2")]
        if name == "level1.1":
            return [Path.joinpath(path, "level2.1"), Path.joinpath(path, "level2.2")]
        if name == "level2.1":
            return [Path.joinpath(path, "level3.1")]
        if name == "level3.1":
            return [Path.joinpath(path, "level4.1")]
        # others
        return []

    @mock.patch.object(Path, "iterdir", autospec=True, side_effect=mock_iterdir)
    @mock.patch.object(Path, "is_dir", return_result=True)
    def test_get_directories_recursive(self, mocked_isdir, mocked_iterdir):
        """
        create folder tree as:
        level1.1
            -level2.1
                -level3.1
                    -level4.1
            -level2.2
        level1.2
        """

        tmpdirname = "/tmp"
        watcher = RecursiveWatcher(None, None)
        paths = [path for path in watcher._get_directories_recursive(Path(tmpdirname))]
        self.assertEqual(len(paths), 7)
        self.assertEqual(str(paths[0]), tmpdirname)
        self.assertEqual(str(paths[1]), os.path.join(tmpdirname, "level1.2"))
        self.assertEqual(str(paths[2]), os.path.join(tmpdirname, "level1.1"))
        self.assertEqual(str(paths[3]), os.path.join(tmpdirname, "level1.1", "level2.2"))
        self.assertEqual(str(paths[4]), os.path.join(tmpdirname, "level1.1", "level2.1"))
        self.assertEqual(str(paths[5]), os.path.join(tmpdirname, "level1.1", "level2.1", "level3.1"))
        self.assertEqual(str(paths[6]), os.path.join(tmpdirname, "level1.1", "level2.1", "level3.1", "level4.1"))


    def assert_paths_watched(self, watchers, path_set):
        watched_path_set = {str(watch.path) for watch in watchers.values()}
        self.assertSetEqual(watched_path_set, path_set)

    class FakeWatcher:
        def __init__(self, path) -> None:
            self.path = path

    def test_assert_paths_watched(self):
        # both empty
        self.assert_paths_watched({}, set())

        # watchers empty
        with self.assertRaises(AssertionError):
            self.assert_paths_watched({}, {"/tmp/path1"})

        # path set empty
        with self.assertRaises(AssertionError):
            self.assert_paths_watched({
                "fd1": self.FakeWatcher(Path("/tmp/path1")),
                "fd2": self.FakeWatcher(Path("/tmp/path2")),
            }, set())

        # identical sets
        self.assert_paths_watched({
            "fd1": self.FakeWatcher(Path("/tmp/path1")),
            "fd2": self.FakeWatcher(Path("/tmp/path2")),
        }, {
            "/tmp/path2",
            "/tmp/path1"
        })

        # diff sets
        with self.assertRaises(AssertionError):
            self.assert_paths_watched({
                "fd1": self.FakeWatcher(Path("/tmp/path1")),
            }, {
                "/tmp/path2",
                "/tmp/path1"
            })

    def create_file(self, file_path):
        with open(str(file_path), "w") as f:
            f.write(file_path)

    async def read_events(self, inotify, folder, events):
        watcher = RecursiveWatcher(Path(folder), Mask.CLOSE_WRITE)
        async for event in watcher.watch_recursive(inotify):
            # events/watchers are ephemeral, copy data we want
            events.append((
                event.path,
                event.mask,
            ))

    async def test_watch_recursive(self):
        """
        test the cases of folder changes:
        1. create folder
        2. create cascading folders
        3. move folder in from un-monitored folder
        4. move folders out to un-monitored folder
        5. move folder within monitored folders
        6. delete folders
        """
        with tempfile.TemporaryDirectory() as tmpdirbasename:
            events = []

            tmpdirname = os.path.join(tmpdirbasename, "test")
            os.makedirs(tmpdirname)
            existing_dir = os.path.join(tmpdirname, "existing_dir")
            os.makedirs(existing_dir)
            outside_dir = os.path.join(tmpdirbasename,  "outside")
            os.makedirs(outside_dir)

            with Inotify() as inotify:
                watch_task = asyncio.create_task(self.read_events(inotify, tmpdirname, events))
                await asyncio.sleep(0.3)

                # existing 2 folders are watched
                self.assert_paths_watched(inotify._watches, {
                    tmpdirname,
                    existing_dir,
                })

                # create file, event
                file_path = os.path.join(tmpdirname, "f1.txt")
                self.create_file(file_path)
                await asyncio.sleep(0.3)

                # still 2 folders watched
                self.assert_paths_watched(inotify._watches, {
                    tmpdirname,
                    existing_dir,
                })

                # create folder and a file inside, no event because of racing
                folder_path = os.path.join(tmpdirname, "d1")
                os.makedirs(folder_path)
                file_path = os.path.join(folder_path, "f2.txt")
                self.create_file(file_path)
                await asyncio.sleep(0.3)

                # one more folder watched
                self.assert_paths_watched(inotify._watches, {
                    tmpdirname,
                    existing_dir,
                    os.path.join(tmpdirname, "d1"),
                })

                # create cascade folders
                folder_path = os.path.join(tmpdirname, "d2", "dd1", "ddd1")
                os.makedirs(folder_path)
                await asyncio.sleep(0.3)

                # 3 more folders watched
                self.assert_paths_watched(inotify._watches, {
                    tmpdirname,
                    existing_dir,
                    os.path.join(tmpdirname, "d1"),
                    os.path.join(tmpdirname, "d2"),
                    os.path.join(tmpdirname, "d2", "dd1"),
                    os.path.join(tmpdirname, "d2", "dd1", "ddd1"),
                })

                # move in folder from outside
                move_folder_path = os.path.join(tmpdirname, "d1", "outside")
                os.rename(outside_dir, move_folder_path)
                await asyncio.sleep(0.3)

                # one more folder watched
                self.assert_paths_watched(inotify._watches, {
                    tmpdirname,
                    existing_dir,
                    os.path.join(tmpdirname, "d1"),
                    os.path.join(tmpdirname, "d2"),
                    os.path.join(tmpdirname, "d2", "dd1"),
                    os.path.join(tmpdirname, "d2", "dd1", "ddd1"),
                    os.path.join(tmpdirname, "d1", "outside"),
                })

                # create file in watched outside folder, event
                file_path = os.path.join(tmpdirname, "d1", "outside", "f3.txt")
                self.create_file(file_path)
                await asyncio.sleep(0.3)

                # move out folder
                folder_path = os.path.join(tmpdirname, "d2", "dd1")
                move_folder_path = os.path.join(tmpdirbasename, "dd1")
                os.rename(folder_path, move_folder_path)
                await asyncio.sleep(0.3)

                # 2 folders not watched
                self.assert_paths_watched(inotify._watches, {
                    tmpdirname,
                    existing_dir,
                    os.path.join(tmpdirname, "d1"),
                    os.path.join(tmpdirname, "d2"),
                    os.path.join(tmpdirname, "d1", "outside"),
                })

                # create file in not watched folder, no event
                file_path = os.path.join(tmpdirbasename, "dd1", "ddd1", "f4.txt")
                self.create_file(file_path)
                await asyncio.sleep(0.3)

                # move folder within
                folder_path = os.path.join(tmpdirname, "existing_dir")
                move_folder_path = os.path.join(tmpdirname, "d1", "existing_dir")
                os.rename(folder_path, move_folder_path)
                await asyncio.sleep(0.3)

                # folders change
                self.assert_paths_watched(inotify._watches, {
                    tmpdirname,
                    os.path.join(tmpdirname, "d1"),
                    os.path.join(tmpdirname, "d2"),
                    os.path.join(tmpdirname, "d1", "outside"),
                    os.path.join(tmpdirname, "d1", "existing_dir")
                })

                # create file in moved folder, event
                file_path = os.path.join(tmpdirname, "d1", "existing_dir", "f5.txt")
                self.create_file(file_path)
                await asyncio.sleep(0.3)

                # delete folder
                folder_path = os.path.join(tmpdirname, "d2")
                os.removedirs(folder_path)
                await asyncio.sleep(0.3)

                # one less folder watched
                self.assert_paths_watched(inotify._watches, {
                    tmpdirname,
                    os.path.join(tmpdirname, "d1"),
                    os.path.join(tmpdirname, "d1", "outside"),
                    os.path.join(tmpdirname, "d1", "existing_dir")
                })

                # delete folders
                shutil.rmtree(os.path.join(tmpdirname, "d1"))
                await asyncio.sleep(0.3)

                # less folders watched
                self.assert_paths_watched(inotify._watches, {
                    tmpdirname,
                })

                watch_task.cancel()
                await asyncio.gather(watch_task, return_exceptions=True)

                # verify events
                self.assertEqual(len(events), 3)
                self.assertEqual(str(events[0][0]), os.path.join(tmpdirname, "f1.txt"))
                self.assertTrue(events[0][1] & Mask.CLOSE_WRITE)

                self.assertEqual(str(events[1][0]), os.path.join(tmpdirname, "d1", "outside", "f3.txt"))
                self.assertTrue(events[1][1] & Mask.CLOSE_WRITE)

                self.assertEqual(str(events[2][0]), os.path.join(tmpdirname, "d1", "existing_dir", "f5.txt"))
                self.assertTrue(events[2][1] & Mask.CLOSE_WRITE)
