import os
import sys
import asyncio
import logging
from asynclcron.lcron import LightweightCron
from syncservers.config import Config, ConfigSync
from syncservers.async_task_queue import AsyncTaskQueue
from syncservers.syncs import LiveSync, CronSync


logger = logging.getLogger(__name__)


class SyncServersApp:

    def __init__(self, config, schedule_jobs) -> None:
        self._config = config
        self._schedule_jobs = schedule_jobs

    def run(self):
        logger.info(f"starting sync servers")

        tasks = []

        # async queue
        async_queue = AsyncTaskQueue(self._config)

        async_queue_tasks = async_queue.run()
        tasks.extend(async_queue_tasks)

        # live sync
        if self._config.get_path_mappings(LiveSync.PATH_TYPE):
            live_sync = LiveSync(self._config, async_queue)
            live_sync.add_retry_to_schedule(self._schedule_jobs)
            live_sync.add_to_startup_sync()
            live_sync_tasks = live_sync.run()
            tasks.extend(live_sync_tasks)

        # cron sync
        if self._config.get_path_mappings(CronSync.PATH_TYPE):
            cron_sync = CronSync(self._config, async_queue)
            cron_sync.add_retry_to_schedule(self._schedule_jobs)
            cron_sync.add_to_startup_sync()
            cron_sync.add_to_schedule(self._schedule_jobs)

        return tasks


async def run():
    # init logging
    from syncservers import config_logging
    logs_folder_path = os.path.abspath("logs")
    os.makedirs(logs_folder_path, exist_ok=True)
    log_file_path = os.path.join(logs_folder_path, "syncservers.log")
    config_logging(log_file_path)

    tasks = []

    # config
    config_file_path = os.path.abspath(os.path.join("config", "syncservers.ini"))
    config = Config(config_file_path)
    if not config.load_config():
        sys.exit(1)
    config_sync = ConfigSync(config)
    config_sync_task = config_sync.run()
    tasks.append(config_sync_task)

    schedule_jobs = []

    app = SyncServersApp(config, schedule_jobs)
    tasks.extend(app.run())

    # schedule
    schedule = LightweightCron()
    for async_func, cron, args, kwargs in schedule_jobs:
        schedule.add(
            async_func=async_func,
            cron=cron,
            args=args,
            kwargs=kwargs,
        )
    schedule_task = schedule.run()
    tasks.append(schedule_task)

    # wait for tasks
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(run())
