from collections import defaultdict
import logging

from tqdm import tqdm

from .config import Config
from .fetch import Fetcher, load_saved_data
from .store import Storer
from .transform import Transformer
from .validate import validate_node_data, validate_edge_data


logger = logging.getLogger(__name__)


class Processor:

    def __init__(
        self,
        config: Config,
        fetcher: Fetcher,
        storer: Storer,
        transformer: Transformer,
    ):
        self.config = config
        self.fetcher = fetcher
        self.storer = storer
        self.transformer = transformer

        self.node_iterator = self.transformer.get_nodes
        self.nodes_by_type = defaultdict(list)
        self.edges = set()

    def ensure_api_response_data_saved(self, refresh: bool = False) -> None:
        logger.debug("Ensuring input data exists on disk")
        if self.config.unprocessed_file_path.is_file() and not refresh:
            self.fetcher.raw_data = load_saved_data(self.config.unprocessed_file_path)
        else:
            self.fetcher.get_api_data(self.config.api_url, self.config.api_parameters)
            self.storer.save_data(self.fetcher.raw_data, self.config.unprocessed_file_path)

    def clean_and_transform_data(self) -> None:
        logger.debug("Cleaning and transforming data")
        self.transformer.df = self.fetcher.raw_data
        self.transformer.clean_data_values()

    def set_nodes_and_edges(self) -> None:
        self.set_nodes_by_type()
        self.set_edges()

    def set_nodes_by_type(self) -> None:
        logger.debug("Setting nodes by type")
        nodes = tqdm(
            self.node_iterator(),
            desc="Node generation",
            unit_scale=True,
            unit="node"
        )

        for ix, node in enumerate(nodes):
            self.nodes_by_type[node["labels"][0]].append(node)

    def set_edges(self) -> None:
        logger.debug("Setting edges")
        self.edges = list(self.transformer.get_edges())
        if len(self.edges) == 0:
            raise RuntimeError(f"No relations were generated for {self.name}")

    def validate_data(self) -> None:
        logger.debug("Validating graph data")
        for nodes in self.nodes_by_type.values():
            validate_node_data(self.config.name, nodes)
        validate_edge_data(self.config.name, self.edges)

    def save_graph_data(self) -> None:
        logger.debug("Saving output data")
        self.storer.save_node_data()
        self.storer.save_edge_data(self.edges)

