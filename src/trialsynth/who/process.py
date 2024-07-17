from bioregistry import curie_to_str
from tqdm.contrib.logging import logging_redirect_tqdm

from ..base.process import BaseProcessor

from .config import Config
from .fetch import Fetcher
from .store import Storer
from .transform import Transformer

import gilda


def ground_conditions(condition: str):
    name_spaces = ["MESH", "doid", "mondo", "go"]

    with logging_redirect_tqdm():
        # have gilda try and ground condition
        grounded_condition = gilda.ground(condition, namespaces=name_spaces)
        if len(grounded_condition) == 0:
            annotations = gilda.annotate(condition, namespaces=name_spaces)
            if len(annotations) == 0:
                return None
            else:
                return annotations[0].term.id
    return grounded_condition[0].term.get_curie()

class Processor(BaseProcessor):
    def __init__(self):
        config = Config()
        super().__init__(
            config=config,
            fetcher=Fetcher(config),
            conditions_grounder=ground_conditions,
            interventions_grounder=lambda x: x
        )
