"""
Centralized logging utility for LandMapper data pipeline.
"""

import os
import logging
from datetime import datetime
from tqdm import tqdm

from .config import get_config


class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


def setup_project_logging():
    """
    Configure logging for project modules.

    Returns:
        logging.Logger: Configured logger instance
    """
    # Use configuration from centralized config module
    config = get_config()
    log_path = config.LOG_PATH

    timestamp = datetime.now().strftime("%Y%m%d")
    if log_path:
        log_dir = os.path.expandvars(log_path)
        if not os.path.isabs(log_dir):
            log_dir = os.path.abspath(log_dir)
        log_file = os.path.join(log_dir, f"lm_dpl_{timestamp}.log")
    else:
        log_file = f"lm_dpl_{timestamp}.log"

    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(log_file), TqdmLoggingHandler()],
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Project logging initialized to file: {log_file}")

    return logger


def get_project_logger(name=None):
    """
    Get a logger instance with project-wide configuration.

    Args:
        name (str, optional): Logger name. If None, uses calling module name.

    Returns:
        logging.Logger: Configured logger instance
    """
    if name is None:
        import inspect

        caller_frame = inspect.stack()[1]
        caller_module = inspect.getmodule(caller_frame[0])
        name = caller_module.__name__ if caller_module else __name__

    if not logging.getLogger().handlers:
        setup_project_logging()

    return logging.getLogger(name)
