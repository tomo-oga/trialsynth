"""Gets WHO data from REST API (not implemented) or saved file"""

import gzip
import logging
from pathlib import Path

from lxml import etree
import pandas as pd


logger = logging.getLogger(__name__)


def get_api_data() -> None:
    raise NotImplementedError("WHO XML data fetch not automated")

    # How to get the dump:
    #
    # Navigate to https://trialsearch.who.int/AdvSearch.aspx
    #
    # Change the recruitment status dropdown "ALL"
    # Click the "search" button
    # Click the "Export Results to XML" button on the right side
    # Click "I agree" on the pop-up
    # Click "Export all clinical trials to xml" and be patient, it's about 500MB
    # Gzip and add to this folder
    #
    # Warning: we had to trick the advanced search interface into giving us a dump. It does not appear to be complete,
    # and an automated solution may require a tool like Selenium.


def load_saved_xml_data(path: Path) -> etree.Element:
    """
    Loads the XML data from a saved file

    Parameters
    ----------
    path : Path
        Path to the saved XML data

    Returns
    -------
    etree.Element
        The parsed XML data
    """
    logger.debug(f"Loading WHO XML data from {path}")
    try:
        with gzip.open(path, "rt") as file:
            raw = file.read()
        t = "<?xml version='1.0' ?>" + raw.split("\n", 1)[1] + "</Trials_downloaded_from_ICTRP>"
        return etree.fromstring(t)
    except Exception:
        logger.exception(f"Could not load XML data from {path}")


def load_saved_pickled_data(path: Path) -> pd.DataFrame:
    """
    Loads the pickled data from a saved file

    Parameters
    ----------
    path : Path
        Path to the saved pickled data

    Returns
    -------
    DataFrame
        The pickled data
    """
    logger.debug(f"Loading pickled WHO data from {path}")
    try:
        return pd.read_pickle(path)
    except Exception:
        logger.exception(f"Could not load pickled data from {path}")
