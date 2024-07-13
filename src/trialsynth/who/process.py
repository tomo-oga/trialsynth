from ..base.process import BaseProcessor

from .config import Config
from .fetch import Fetcher
from .store import Storer
from .transform import Transformer


class Processor(BaseProcessor):
    def __init__(self):
        config = Config()
        super().__init__(
            config=config,
            fetcher=Fetcher(config),
            storer=Storer(config),
            transformer=Transformer(config)
        )



