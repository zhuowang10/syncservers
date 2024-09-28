from collections import deque
import logging
from asyncinotify import Inotify, Mask


logger = logging.getLogger(__name__)


class RecursiveWatcher:
    """
    based on and improved https://github.com/ProCern/asyncinotify/blob/master/examples/recursivewatch.py
    """
    def __init__(self, path, mask) -> None:
        self._path = path
        self._mask = mask

    def _get_directories_recursive(self, path):
        """
        DFS to iterate all paths within
        """
        if not path.is_dir():
            return

        stack = deque()
        stack.append(path)
        while stack:
            curr_path = stack.pop()
            yield curr_path
            for subpath in curr_path.iterdir():
                if subpath.is_dir():
                    stack.append(subpath)

    async def watch_recursive(self, inotify=None):
        """
        watch a folder recursively:
        add a watch when a folder is created/moved in
        delete a watch when a folder is moved out
        this works for folders moving within the watched folders because both move_from event and move_to event will be caught
        delete event is not monitored because ignore deletes watch
        """
        create_inotify = inotify is None
        if create_inotify:
            logger.info(f"creating inotify")
            inotify = Inotify()

        try:
            mask = self._mask | Mask.MOVED_FROM | Mask.MOVED_TO | Mask.CREATE | Mask.IGNORED
            for directory in self._get_directories_recursive(self._path):
                logger.info(f"watching folder: {directory}")
                inotify.add_watch(directory, mask)

            # Things that can throw this off:
            #
            # * Doing two changes on a directory or something before the program
            #   has a time to handle it (this will also throw off a lot of inotify
            #   code, though)
            #
            # * Trying to watch a path that doesn't exist won't automatically
            #   create it or anything of the sort.

            async for event in inotify:
                if Mask.ISDIR in event.mask and event.path is not None:
                    if Mask.CREATE in event.mask or Mask.MOVED_TO in event.mask:
                        # created new folder or folder moved in, add watches
                        for directory in self._get_directories_recursive(event.path):
                            logger.info(f"watching folder: {directory}")
                            inotify.add_watch(directory, mask)
                    if Mask.MOVED_FROM in event.mask:
                        # a folder is moved to another location, remove watch for this folder and subfolders
                        watches = [watch for watch in inotify._watches.values() if watch.path.is_relative_to(event.path)]
                        for watch in watches:
                            logger.info(f"unwatching folder: {watch.path}")
                            inotify.rm_watch(watch)

                    # DELETE event is not watched/handled here because IGNORED event follows deletion, and handled in asyncinotify

                if event.mask & self._mask:
                    yield event
        finally:
            if create_inotify:
                logger.info(f"closing inotify")
                inotify.close()
