from pathlib import Path
import os
import asyncio
import logging
from asyncinotify import Mask, RecursiveWatcher
from syncservers.config import Config
from syncservers.async_command import async_rsync, get_rsync_path
from syncservers.retry_queue import RetryQueue


logger = logging.getLogger(__name__)


class BaseSync:
    """
    base sync class
    """

    DEFAULT_SYNC_OPTIONS = ["-avh", "--no-p", "--mkpath", "--timeout=6", "--contimeout=2"]

    DEFAULT_RETRY_CRON = "*/2 * * * *"
    DEFAULT_RETRY = False

    DEFAULT_STARTUP_SYNC = False
    DEFAULT_STARTUP_SYNC_OPTIONS = DEFAULT_SYNC_OPTIONS

    PATH_TYPE = None
    OPTIONS_DELIMITER = ' '

    def __init__(self, config, async_task_queue) -> None:
        self._config = config
        self._async_task_queue = async_task_queue

        self._retry_queue = RetryQueue(async_task_queue, self._get_config)

    def add_retry_to_schedule(self, schedule_jobs):
        config = self._get_config()
        if not (Config.get_boolean(config, "retry", self.DEFAULT_RETRY)):
            logger.info(f"[{self.PATH_TYPE}] retry is not enabled")
            return

        retry_cron = config.get("retry_cron", self.DEFAULT_RETRY_CRON)
        logger.info(f"[{self.PATH_TYPE}] getting service retry job to schedule, cron: {retry_cron}")
        schedule_jobs.append(
            (self._retry_queue.add_retries_to_sync, retry_cron, [], {}),
        )

    def add_to_startup_sync(self):
        """
        add entire folder sync at service start
        """
        try:
            config = self._get_config()
            if not Config.get_boolean(config, "startup_sync", self.DEFAULT_STARTUP_SYNC):
                logger.info(f"[{self.PATH_TYPE}] sync at startup is not enabled")
                return

            logger.info(f"[{self.PATH_TYPE}] adding entire folders to sync at startup")
            sync_options=Config.get_split_list(
                config, "startup_sync_options", self.OPTIONS_DELIMITER, self.DEFAULT_STARTUP_SYNC_OPTIONS
            )
            retry=Config.get_boolean(config, "retry", self.DEFAULT_RETRY)

            path_mappings = self._get_path_mappings()
            for base_sync_path in path_mappings.keys():
                logger.info(f"[{self.PATH_TYPE}] adding sync path: {base_sync_path}")
                self._async_task_queue.add(
                    base_sync_path,
                    self._sync,
                    base_sync_path=base_sync_path,
                    sync_path=base_sync_path,
                    sync_options=sync_options,
                    is_folder=True,
                    retry=retry,
                )
        except Exception as ex:
            logger.error(f"[{self.PATH_TYPE}] error in add_to_startup_sync", ex)

    def _get_path_mappings(self):
        return self._config.get_path_mappings(self.PATH_TYPE)

    def _get_config(self):
        return self._config.get_config(self.PATH_TYPE)

    def _get_server_dst_url(self, sync_server, base_sync_path, sync_path, base_dst_path):
        relative_sync_path = sync_path[len(base_sync_path):].strip(os.sep)
        return sync_server.get_sync_url(base_dst_path, relative_sync_path)

    async def _add_to_sync(self, base_sync_path, sync_path, is_folder, **kwargs):
        """
        push rsync task to queue
        """
        try:
            config = self._get_config()
            sync_options=Config.get_split_list(
                config, "sync_options", self.OPTIONS_DELIMITER, self.DEFAULT_SYNC_OPTIONS
            )
            retry=Config.get_boolean(config, "retry", self.DEFAULT_RETRY)

            self._async_task_queue.add(
                base_sync_path,
                self._sync,
                base_sync_path=base_sync_path,
                sync_path=sync_path,
                sync_options=sync_options,
                is_folder=is_folder,
                retry=retry,
            )
        except Exception as ex:
            logger.error(f"[{self.PATH_TYPE}] error in _add_to_sync", ex)

    async def _sync(
            self,
            base_sync_path,
            sync_path,
            sync_options,
            is_folder,
            retry,
        ):
        """
        task actually do sync data to servers
        add to retry queue if failed
        """
        try:
            _, server_list = self._get_path_mappings().get(base_sync_path, (None, []))

            # use _* to be backward compatible when more params are available
            for sync_server, base_dst_path, *_ in server_list:
                success = True
                try:
                    base_dst_path = os.path.normpath(base_dst_path)
                    dst_url = self._get_server_dst_url(sync_server, base_sync_path, sync_path, base_dst_path)
                    logger.info(f"[{self.PATH_TYPE}] syncing {'folder' if is_folder else 'file'} {sync_path} to {dst_url}")
                    success = await async_rsync(
                        sync_options,
                        get_rsync_path(sync_path, is_folder),
                        get_rsync_path(dst_url, is_folder),
                    )
                    logger.info(f"[{self.PATH_TYPE}] success: {success}")
                except Exception as sync_ex:
                    success = False
                    logger.error(f"[{self.PATH_TYPE}] error in sync", sync_ex)
                if not success:
                    logger.error(f"[{self.PATH_TYPE}] sync failed")
                    # retry
                    if retry:
                        logger.info(f"[{self.PATH_TYPE}] will retry later")
                        self._retry_queue.add(
                            base_sync_path,
                            0,
                            sync_options,
                            sync_path,
                            dst_url,
                            is_folder,
                        )
        except Exception as ex:
            logger.error(f"[{self.PATH_TYPE}] error in _sync, sync_path: {sync_path}", ex)


class LiveSync(BaseSync):
    """
    live sync new files with some retries
    only sync new files: created files, moved in files
    focuses on fast response, does not guarantee all files be synced if errors
    ok to hot change servers config, but not source paths
    """

    PATH_TYPE = "live"

    def run(self):
        """
        start to watch paths
        return task handles
        """
        logger.info(f"[{self.PATH_TYPE}] starting sync service")

        # start watch folders
        sync_paths = self._get_path_mappings().keys()
        return [asyncio.create_task(self._watch_path(base_sync_path)) for base_sync_path in sync_paths]

    async def _watch_path(self, base_sync_path):
        logger.info(f"[{self.PATH_TYPE}] start watching all folders in: {base_sync_path}")
        watcher = RecursiveWatcher(Path(base_sync_path), Mask.CLOSE_WRITE | Mask.MOVED_TO | Mask.CREATE)
        async for event in watcher.watch_recursive():
            if event.mask & Mask.ISDIR:
                # folder event
                if event.mask & Mask.CREATE:
                    # new folder created, sync this folder
                    logger.info(f"[{self.PATH_TYPE}] folder {str(event.path)} was created, sync this folder")
                    await self._add_to_sync(base_sync_path, os.path.normpath(str(event.path)), True)
                elif event.mask & Mask.MOVED_TO:
                    # new folder created or moved in, sync this folder
                    logger.info(f"[{self.PATH_TYPE}] folder {str(event.path)} was moved in, sync this folder")
                    await self._add_to_sync(base_sync_path, os.path.normpath(str(event.path)), True)
            else:
                # file event
                if event.mask & Mask.CLOSE_WRITE:
                    # new file created, sync this file
                    logger.info(f"[{self.PATH_TYPE}] file {str(event.path)} was created, sync this file")
                    await self._add_to_sync(base_sync_path, os.path.normpath(str(event.path)), False)
                elif event.mask & Mask.MOVED_TO:
                    # new file moved in, sync this file
                    logger.info(f"[{self.PATH_TYPE}] file {str(event.path)} was moved in, sync this file")
                    await self._add_to_sync(base_sync_path, os.path.normpath(str(event.path)), False)
            # no op for other events


class CronSync(BaseSync):
    """
    cron sync with no retry since cron sync is supposed to sync at a non live pace
    ok to hot change servers config, but not source paths
    """

    PATH_TYPE = "cron"

    def add_to_schedule(self, schedule_jobs):
        for base_sync_path, (path_configs, _) in self._get_path_mappings().items():
            cron_str = path_configs["cron"]
            logger.info(f"[{self.PATH_TYPE}] adding sync task, path: {base_sync_path}, cron: {cron_str}")
            schedule_jobs.append(
                (self._add_to_sync, cron_str, [], {
                "base_sync_path": base_sync_path,
                    "sync_path": base_sync_path,
                    "is_folder": True,
                })
            )
