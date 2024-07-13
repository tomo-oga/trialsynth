from ..base.store import BaseStorer
from .config import Config
from typing import Callable, Iterator


class Storer(BaseStorer):
    def __init__(self, config: Config):
        super().__init__(config)