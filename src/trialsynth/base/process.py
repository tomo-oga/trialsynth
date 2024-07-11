from collections import defaultdict
import logging

from tqdm import tqdm

from .config import BaseConfig
from .fetch import BaseFetcher
from .store import BaseStorer
from .transform import BaseTransformer

logger = logging.getLogger(__name__)


class BaseProcessor:
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

        self.node_iterator
        self.nodes_by_type = defaultdict(list)
        self.edges = set()

    def ensure_api_response_data_saved(self, refresh: bool = False) -> None:
        logger.info("Ensuring input data exists on disk")
        if self.config.raw_data_path.is_file() and not refresh:
            self.fetcher.load_saved_data()
        else:
            self.fetcher.get_api_data()
            self.storer.save_as_flat_file(self.fetcher.raw_data, self.config.raw_data_path)
