import logging
from pathlib import Path

from .config import DATA_DIR


logger = logging.getLogger(__name__)


def ensure_output_directory_exists(path: Path = DATA_DIR) -> None:
    try:
        logger.debug(f"Ensuring directory exists: {DATA_DIR}")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        logger.exception(f"An error occurred trying to create {DATA_DIR}")
        raise
