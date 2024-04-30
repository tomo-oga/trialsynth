import logging
from pathlib import Path

import pandas as pd


logger = logging.getLogger(__name__)


def store_dataframe_as_flat_file(dataframe: pd.DataFrame, path: Path, delimiter: str = "\t", index: bool = False) -> None:
    try:
        dataframe.to_csv(path, sep=delimiter, index=index)
    except Exception:
        logger.exception(f"Error saving dataframe to {path}")
