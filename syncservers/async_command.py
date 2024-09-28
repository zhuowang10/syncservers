import logging
import asyncio
import os


logger = logging.getLogger(__name__)


async def async_run_command(command, *args):
    logger.info(f"run command: {command} {' '.join(args)}")
    proc = await asyncio.create_subprocess_exec(
        command, *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await proc.communicate()
    if stdout:
        logger.info(f"stdout: {stdout.decode('utf-8')}")
    if stderr:
        logger.error(f"stderr: {stderr.decode('utf-8')}")
    logger.info(f"return code: {proc.returncode}")
    return proc.returncode


async def async_rsync(options, src_url, dst_url):
    r = await async_run_command("rsync", *options, src_url, dst_url)
    return r == 0


def get_rsync_path(path, is_folder):
        """
        add / to the end for folder
        """
        return f"{path}{os.sep}" if is_folder else path
