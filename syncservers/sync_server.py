import os

class SyncServer:

    def __init__(self, server_id, server_url):
        self.server_id = server_id
        self.server_url = server_url

    def get_sync_url(self, *paths):
        url = os.path.normpath(os.path.join(self.server_url, *paths))
        return f"rsync://{url}"
