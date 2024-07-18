import csv
import logging
import gzip
from pathlib import Path
import pickle
from typing import Callable, Iterator, Tuple

from .config import BaseConfig
from .models import Trial, BioEntity, Edge

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

    def save_trial_data(self, data: list[Tuple], write_sample=True) -> None:
        """Save trial data from a list of tuples as compressed TSV

        Parameters
        ----------
        data: list[Tuple]
            Trial data flattened into tuples
        """
        headers = ['title', 'type', 'design', 'conditions', 'interventions',
                   'primary_outcome', 'secondary_outcome', 'secondary_ids']
        path = self.config.processed_data_path

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
            for trial in tqdm(data, desc=f'Serializing and storing trial data to {path}', unit="trial", unit_scale=True)
        )

        with gzip.open(path, mode='wt'):
            csv_writer = csv.writer(path, delimiter='\t')
            csv_writer.writerow(headers)
            if write_sample:
                with self.config.processed_sample_path.open('w') as sample_file:
                    logger.info(f'Saving sample trial data to {sample_file}')

                    sample_writer = csv.writer(sample_file, delimiter='\t')
                    sample_writer.writerow(headers)
                    for _, trial in tqdm(zip(range(self.config.num_sample_entries), trials), desc="Writing samples"):
                        sample_writer.writerow(trial)
                        csv_writer(trial)
            csv_writer.writerows(trials)

    def save_bioentities(self, entities: list[Tuple]):
        headers = ['origin', 'curie', 'term']
    def save_node_data(self) -> None:
        """
        Save node data to disk as compressed TSV files

        Raises
        ------
        RunTimeError
            If no nodes were generated for the graph
        """
        nodes_by_type = {
            "ClinicalTrial": list(),
            "BioEntity": list()
        }

        nodes = tqdm(
            self.node_iterator(),
            desc="Generating nodes",
            unit="node"
        )
        # Map nodes to their respective types
        ix = 0
        for ix, node in enumerate(nodes):
            if isinstance(node, Trial):
                nodes_by_type["ClinicalTrial"].append(node)
            if isinstance(node, BioEntity):
                nodes_by_type["BioEntity"].append(node)
        if ix == 0:
            raise RuntimeError(f"No nodes were generated for {self.config.registry}")
        for node_type in nodes_by_type:
            nodes_path, nodes_indra_path, sample_path = self.node_types_to_paths.get(node_type)
            nodes = sorted(nodes_by_type[node_type], key=lambda x: x.curie)
            with open(nodes_indra_path, "wb") as fh:
                pickle.dump(nodes, fh)
            self.dump_nodes_to_path(nodes, nodes_path, node_type, sample_path)

    def dump_nodes_to_path(self, nodes, nodes_path, node_type, sample_path=None, write_mode="wt") -> None:
        """Dump node data to a path as a compressed TSV file

        Parameters
        ----------
        nodes : list
            Nodes to dump
        nodes_path : Path
            Where nodes are to be stored.
        sample_path : Path
            Where a sample of the nodes are to be stored. Default: None
        write_mode : str
            Write mode for the file. Default: 'wt'
        """
        logger.info(f"Dumping node data into {nodes_path}")
        if sample_path:
            logger.info(f"Dumping sample node data into {sample_path}")
        headers = ("id: ID", ":TYPE", ":TITLE", ":DESIGN", ":CONDITIONS", ":INTERVENTIONS", ":PRIMARY_OUTCOME",
                   ":SECONDARY_OUTCOME", ":SECONDARY_IDS")

        node_rows = (
            (
                node.curie,
                node_type,
                node.title,
                node.design,
                ','.join(node.conditions),
                ','.join(node.interventions),
                node.primary_outcome,
                node.secondary,
                ','.join(node.secondary_ids)
            )
            for node in tqdm(nodes, desc="Node serialization", unit="node")
        )

        with gzip.open(nodes_path, mode=write_mode) as node_file:
            node_writer = csv.writer(node_file, delimiter="\t")  # type: ignore
            # Only add header when writing to a new file
            if write_mode == "wt":
                node_writer.writerow(headers)
            if sample_path:
                with sample_path.open("w") as node_sample_file:
                    node_sample_writer = csv.writer(node_sample_file, delimiter="\t")
                    node_sample_writer.writerow(headers)
                    for _, node_row in zip(range(10), node_rows):
                        node_sample_writer.writerow(node_row)
                        node_writer.writerow(node_row)
            # Write remaining nodes
            node_writer.writerows(node_rows)

    def save_edge_data(self):
        """Save edge data as compressed TSV file"""

        edges = tqdm(
            self.edge_iterator(),
            desc="Generating edges",
            unit="edge"
        )
        edge_data: list[Edge] = []

        for edge in edges:
            edge_data.append(edge)

        self.dump_edges_to_path(edge_data)

    def dump_edges_to_path(self, rels: list[Edge], write_mode="wt"):
        """
        Save edge data to disk as a compressed TSV file.

        Parameters
        ----------
        rels : list
            The list of edges to save
        write_mode : str
            Write mode for the file. Default: 'wt'
        """

        logger.info(f"Dumping edge data into {self.edges_path}...")

        header = ":START_ID", ":END_ID", ":TYPE", ":CURIE", ":SOURCE"

        rels = sorted(
            rels, key=lambda r: (r.bio_ent_curie, r.trial_curie)
        )

        edge_rows = (
            (
                rel.bio_ent_curie,
                rel.trial_curie,
                rel.rel_type,
                rel.rel_type_curie,
                self.config.registry
            )
            for rel in tqdm(rels, desc="Edge serialization", unit="edge")
        )

        with gzip.open(self.edges_path, mode=write_mode) as edge_file:
            edge_writer = csv.writer(edge_file, delimiter="\t")  # type: ignore

            # Only add header when writing to a new file
            if write_mode == "wt":
                edge_writer.writerow(header)
            if self.edges_sample_path:
                with self.edges_sample_path.open("w") as edge_sample_file:
                    edge_sample_writer = csv.writer(edge_sample_file, delimiter="\t")
                    edge_sample_writer.writerow(header)
                    for _, edge_row in zip(range(10), edge_rows):
                        edge_sample_writer.writerow(edge_row)
                        edge_writer.writerow(edge_row)
            # Write remaining edges
            edge_writer.writerows(edge_rows)
