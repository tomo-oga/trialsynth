import logging
from typing import Iterator

from tqdm import tqdm

from .config import BaseConfig
from .fetch import BaseFetcher
from .models import Trial, Edge, Node
from .store import BaseStorer
from .transform import BaseTransformer

import gilda

logger = logging.getLogger(__name__)


class BaseProcessor:
    """Processes registry data using Config, Fetcher, Storer, and Transformer objects.

    Attributes
    ----------
    config : Config
        User-mutable properties of registry data processing
    fetcher : Fetcher
        Fetches registry data from the REST API or a saved file
    storer : Storer
        Stores processed data to disk
    transformer : Transformer
        Transforms raw data into nodes and edges for a graph database
    node_iterator : Callable
        Method to generate nodes from the transformed data

    edges : set
        Edges between nodes

    Parameters
    ----------
    config : Config
        User-mutable properties of Clinicaltrials.gov data processing
    fetcher : Fetcher
        Fetches Clinicaltrials.gov data from the REST API or a saved file
    storer : Storer
        Stores processed data to disk
    transformer : Transformer
        Transforms raw data into nodes and edges for a graph database
    """
    def __init__(
            self,
            config: BaseConfig,
            fetcher: BaseFetcher,
            storer: BaseStorer,
            transformer: BaseTransformer
    ):
        self.config = config
        self.fetcher = fetcher
        self.storer = storer
        self.transformer = transformer

        self.trials: list[Trial] = []

        self.nodes: list[Node] = []
        self.edges: list[Node] = []

    def load_data(self):
        self.fetcher.get_api_data()
        self.trials = self.fetcher.raw_data

    def set_nodes_and_edges(self):
        logger.info("Generating nodes and edges")
        self.set_nodes()
        self.set_edges()

    def set_edges(self):
        edges = tqdm(
            self.edge_iterator(),
            desc="Edge generation",
            unit_scale=True,
            unit="edge"
        )
        for edge in edges:
            self.edges.append(edge)

    def set_nodes(self):
        nodes = tqdm(
            self.node_iterator(),
            desc="Node generation",
            unit_scale=True,
            unit="node"
        )
        for node in nodes:
            self.nodes.append(node)

    def node_iterator(self) -> Iterator:
        """Iterates over nodes in the registry data and yields them for processing."""
        curie_to_trial = {}
        yielded_nodes = set()
        for trial in self.trials:
            curie = trial.curie

            self.transformer.transform_title(trial)
            self.transformer.transform_type(trial)
            self.transformer.transform_design(trial)
            self.transformer.transform_conditions(trial)
            self.transformer.transform_interventions(trial)
            self.transformer.transform_primary_outcome(trial)
            self.transformer.transform_secondary_outcome(trial)
            self.transformer.transform_secondary_ids(trial)

            curie_to_trial[curie] = trial

            for condition in trial.conditions:
                if condition:
                    if condition not in yielded_nodes:
                        yield condition
                        yielded_nodes.add(condition)

            for intervention in trial.interventions:
                if intervention:
                    if intervention not in yielded_nodes:
                        yield intervention
                        yielded_nodes.add(intervention)

        for trial in set(self.trials):
            if trial not in yielded_nodes:
                yield trial
                yielded_nodes.add(trial)

    def edge_iterator(self) -> Iterator:
        """Iterates over edges in the registry data and yields them for processing."""
        yielded_edge = set()

        # could be abstracted later to method for handling different edge types
        for edge in self.edges:
            if edge not in yielded_edge:
                yield edge
                yielded_edge.add(edge)


