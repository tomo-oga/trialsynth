from ..base.store import BaseStorer
from .config import Config
from typing import Callable, Iterator


class Storer(BaseStorer):
    def __init__(self, node_iterator: Callable[[], Iterator], config: Config):
        super().__init__(node_iterator, config)