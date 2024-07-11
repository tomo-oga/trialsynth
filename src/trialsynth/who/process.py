from ..base.process import BaseProcessor

from .config import Config
from .fetch import Fetcher
from .store import Storer
from .transform import Transformer


class Processor(BaseProcessor):
    def __init__(self):
        super().__init__(
            config=Config(),
            fetcher=Fetcher(),
            storer=Storer(),
            transformer=Transformer
        )
