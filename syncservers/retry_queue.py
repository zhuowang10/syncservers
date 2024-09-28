from collections import deque
import logging
from syncservers.async_command import async_rsync, get_rsync_path


logger = logging.getLogger(__name__)


class RetryQueue:

    DEFAULT_MAX_RETRIES = 2

    def __init__(self, async_task_queue, get_retry_config) -> None:
        self._async_task_queue = async_task_queue
        self._get_retry_config = get_retry_config

        self._retry_queue = deque()

    async def add_retries_to_sync(self, **kwargs):
        """
        re-queue tasks in retry queue
        """
        try:
            if not self._retry_queue:
                return

            # divide by queue_id/base_sync_path
            sync_tasks = {}
            while self._retry_queue:
                base_sync_path, retries, options, sync_path, dst_url, is_folder = self._retry_queue.popleft()
                if base_sync_path not in sync_tasks:
                    sync_tasks[base_sync_path] = []
                sync_tasks[base_sync_path].append((retries, options, sync_path, dst_url, is_folder))

            # add to sync queue
            for base_sync_path, retry_tasks in sync_tasks.items():
                logger.info(f"adding retry, base_sync_path: {base_sync_path}, sync tasks: {len(sync_tasks)}")
                self._async_task_queue.add(
                    base_sync_path,
                    self._retry_sync,
                    base_sync_path=base_sync_path,
                    retry_tasks=retry_tasks,
                )
        except Exception as ex:
            logger.error(f"error in add_retries_to_sync", ex)

    def add(
            self,
            base_sync_path,
            retries,
            sync_options,
            sync_path,
            dst_url,
            is_folder
    ):
        self._retry_queue.append((
            base_sync_path,
            retries,
            sync_options,
            sync_path,
            dst_url,
            is_folder,
        ))

    async def _retry_sync(
            self,
            base_sync_path,
            retry_tasks,
        ):
        """
        actually retry sync
        add to retry queue if failed
        """
        try:
            logger.info("start retry sync")

            for retries, options, sync_path, dst_url, is_folder in retry_tasks:
                success = True
                try:
                    logger.info(f"retrying {'folder' if is_folder else 'file'} {sync_path} to {dst_url}")
                    success = await async_rsync(
                        options,
                        get_rsync_path(sync_path, is_folder),
                        get_rsync_path(dst_url, is_folder),
                    )
                except Exception as sync_ex:
                    success = False
                    logger.error(f"error in retry sync", sync_ex)
                if not success:
                    logger.error(f"sync failed")
                    # retry
                    retries += 1
                    config = self._get_retry_config()
                    max_retries=int(config.get("max_retries", self.DEFAULT_MAX_RETRIES))

                    if retries < max_retries:
                        logger.info(f"will retry later {retries+1}/{max_retries}")
                        self.add(
                            base_sync_path,
                            retries,
                            options,
                            sync_path,
                            dst_url,
                            is_folder,
                        )
                    else:
                        logger.info(f"already retried for {retries} time(s), stop retrying")
        except Exception as ex:
            logger.error(f"error in _retry_sync", ex)
