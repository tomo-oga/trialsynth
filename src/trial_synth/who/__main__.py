"""
Extract clinical trial information from the WHO.

Run with `python -m trial_synth.who`
"""

import logging

import click
import pandas as pd

from .config import Config
from .fetch import load_saved_pickled_data
from .process import Processor
from .store import store_dataframe_as_flat_file
from .transform import transform_csv_data
from .util import ensure_df


CONFIG = Config()
logger = logging.getLogger(__name__)





@click.command()
def main():
    df = ensure_df()
    store_dataframe_as_flat_file(df, CONFIG.sample_path, "\t", False)
    processor = Processor(df, CONFIG)
    processor.process_who_data()


if __name__ == "__main__":
    main()
