"""
Extract clinical trial information from the WHO.

Run with `python -m trial_synth.who`
"""

import logging

import click
import pandas as pd

from .config import Config
from .fetch import load_saved_picked_data, load_saved_xml_data
from .process import Processor
from .store import store_dataframe_as_flat_file
from .transform import transform_xml_data


CONFIG = Config()
logger = logging.getLogger(__name__)


def ensure_df() -> pd.DataFrame:
    if CONFIG.parsed_pickle_path.is_file():
        return load_saved_picked_data(CONFIG.parsed_pickle_path)
    xml_data = load_saved_xml_data(CONFIG.xml_path)

    df = transform_xml_data(xml_data)
    df.to_pickle(CONFIG.parsed_pickle_path)
    return df


@click.command()
def main():
    df = ensure_df()
    store_dataframe_as_flat_file(df, CONFIG.sample_path, "\t", False)
    processor = Processor(df, CONFIG)
    processor.process_who_data()


if __name__ == "__main__":
    main()
