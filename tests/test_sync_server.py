from unittest import TestCase
from syncservers.sync_server import SyncServer


class Test(TestCase):

    async def test_get_sync_url(self):
        server = SyncServer("my_server_id", "my_server")

        self.assertEqual(server.get_sync_url(""), "rsync://my_server")
        self.assertEqual(server.get_sync_url("path1/path2"), "rsync://my_server/path1/path2")
