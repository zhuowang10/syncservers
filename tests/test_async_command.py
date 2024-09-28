from unittest import IsolatedAsyncioTestCase

from syncservers.async_command import async_run_command, async_rsync, get_rsync_path


class Test(IsolatedAsyncioTestCase):

    async def test_async_run_command(self):
        r = await async_run_command("ls", "/tmp")
        self.assertEqual(r, 0)

    def test_get_rsync_path(self):
        self.assertEqual(get_rsync_path("/tmp", True), "/tmp/")
        self.assertEqual(get_rsync_path("/tmp/f1", False), "/tmp/f1")
