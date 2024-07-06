from collections import defaultdict
import csv
import logging
import gzip
from pathlib import Path
import pickle
from typing import Callable, Iterator

from .config import BaseConfig

import pandas as pd
from tqdm import tqdm

logger = logging.getLogger(__name__)


class BaseStorer:
    def __init__(self, node_iterator: Callable[[], Iterator], node_types: list[str], config: BaseConfig):
        self.node_iterator = node_iterator
        self.node_types = node_types
        self.node_types_to_paths = config.node_types_to_paths
        self.edges_path = config.edges_path
        self.edges_sample_path = config.edges_sample_path
        self.config = config

    def save_as_flat_file(self, data: pd.DataFrame, path: Path) -> None:
        """Saves data to disk as compressed TSV

        Parameters
        ----------
        data : DataFrame
            Data to save
        path : Path
            The path where data is stored
        """
        logger.debug(f"Saving {self.config.name} data to {path}")
        try:
            data.to_csv(path, sep='\t', index=False, compression="gzip")
        except Exception:
            logger.exception(f"Could not save data to {path}")
            raise

    def save_node_data(self) -> None:
        """
        Save node data to disk as compressed TSV files

        Raises
        ------
        RunTimeError
            If no nodes were generated for the graph
        """
        nodes_by_type = defaultdict(list)

        nodes = tqdm(
            self.node_iterator(),
            desc="Generating nodes",
            unit="node"
        )
        # Map nodes to their respective types
        ix = 0
        for ix, node in enumerate(nodes):
            nodes_by_type[node.labels[0]].append(node)
        if ix == 0:
            raise RuntimeError(f"No nodes were generated for {self.name}")
        for node_type in nodes_by_type:
            nodes_path, nodes_indra_path, sample_path = self.node_types_to_paths.get(node_type)
            nodes = sorted(nodes_by_type[node_type], key=lambda x: (x.db_ns, x.db_id))
            with open(nodes_indra_path, "wb") as fh:
                pickle.dump(nodes, fh)
            self.dump_nodes_to_path(nodes, nodes_path, sample_path)

    def dump_nodes_to_path(self, nodes, nodes_path, sample_path=None, write_mode="wt") -> None:
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
        metadata = sorted(set(key for node in nodes for key in node.data))
        headers = "id: ID", ":LABEL", *metadata

        node_rows = (
            (
                self.norm_id(node.db_ns, node.db_id),
                ";".join(node.labels),
                *[node.data.get(key, "") for key in metadata]
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

    def dump_edges_to_path(self, rels, write_mode="wt"):
        """
        Save edge data to disk as a compressed TSV file.

        Parameters
        ----------
        rels : list
            List of edges to save
        write_mode : str
            Write mode for the file. Default: 'wt'
        """

        logger.info(f"Dumping edge data into {self.edges_path}...")

        metadata = sorted(set(key for rel in rels for key in rel.data))
        header = ":START_ID", ":END_ID", ":TYPE", "curie", "source", *metadata

        rels = sorted(
            rels, key=lambda r: (r.source_ns, r.source_id, r.target_ns, r.target_id)
        )

        edge_rows = (
            (
                self.norm_id(rel.source_ns, rel.source_id),
                self.norm_id(rel.target_ns, rel.target_id),
                rel.rel_type,
                rel.rel_id,
                rel.target_ns,
                self.config.source_key,
                *[rel.data.get(key) for key in metadata],
            )
            for rel in tqdm(rels, desc="Edges", unit_scale=True)
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

    def norm_id(self, db_ns, db_id) -> str:
        """Normalize an identifier.

        Parameters
        ----------
        db_ns :
            The namespace of the identifier.
        db_id :
            The identifier.

        Returns
        -------
        str
            The normalized identifier.
        """

        raise NotImplementedError("Must be defined in subclass")
