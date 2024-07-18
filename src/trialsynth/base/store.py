import csv
import logging
import gzip
from pathlib import Path
import pickle
from typing import Callable, Iterator, Tuple, Optional, Union

from .config import BaseConfig
from .models import Trial, BioEntity, Edge, Node

import pandas as pd
from tqdm import tqdm

logger = logging.getLogger(__name__)


class BaseStorer:
    def __init__(self, config: BaseConfig):
        self.node_types = config.node_types
        self.node_types_to_paths = config.node_types_to_paths
        self.edges_path = config.edges_path
        self.edges_sample_path = config.edges_sample_path
        self.config = config

    def save_trial_data(self, data: list[Tuple], path: Path, sample_path: Optional[Path] = None) -> None:
        """Save trial data from a list of tuples as compressed TSV

        Parameters
        ----------
        data: list[Tuple]
            Trial data flattened into tuples
        """
        headers = ['title', 'type', 'design', 'conditions', 'interventions',
                   'primary_outcome', 'secondary_outcome', 'secondary_ids']

        logger.info(f'Serializing and storing trial data to {path}')
        trials = (
            (
                trial[0],
                trial[1],
                trial[2],
                ','.join(trial[3]),
                ','.join(trial[4]),
                trial[5],
                trial[6],
                ','.join(trial[7])
            )
            for trial in data
        )

        with gzip.open(path, mode='wt') as file:
            csv_writer = csv.writer(file, delimiter='\t')
            csv_writer.writerow(headers)
            if sample_path:
                with sample_path.open('wt') as sample_file:
                    logger.info(f'Saving sample trial data to {sample_path}')

                    sample_writer = csv.writer(sample_file, delimiter='\t')
                    sample_writer.writerow(headers)
                    for _, trial in zip(range(self.config.num_sample_entries), trials):
                        sample_writer.writerow(trial)
                        csv_writer.writerow(trial)
            csv_writer.writerows(trials)

    def dump_data(self, data: list[Node, Edge], path: Path):
        with gzip.open(path, 'wb') as file:
            pickle.dump(data, file)
    def save_bioentities(self, entities: list[Tuple]):
        headers = ['origin', 'curie', 'term']

