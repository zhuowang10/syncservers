import logging
from logging.handlers import RotatingFileHandler


LOGGING_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s"
LOGGING_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def config_logging(log_file_path, level=logging.INFO):
    logging.basicConfig(
        handlers=[
            RotatingFileHandler(
                filename=log_file_path,
                maxBytes=1024*1024*20,
                backupCount=10,
                encoding="utf-8",
            ),
            logging.StreamHandler(),
        ],
        level=level,
        format=LOGGING_FORMAT,
        datefmt=LOGGING_DATE_FORMAT,
    )


def config_test_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format=LOGGING_FORMAT,
        datefmt=LOGGING_DATE_FORMAT,
    )
