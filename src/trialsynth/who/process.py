from typing import Iterator

import copy
import gilda

from ..base.models import BioEntity
from ..base.process import BaseProcessor

from .config import Config
from .fetch import Fetcher


def ground_condition(condition: BioEntity, namespaces: list[str] = None, trial_title: str = None) -> Iterator[BioEntity]:
    name_spaces = ["MESH", "doid", "mondo", "go"]

    # first try grounding condition with title context
    grounded = gilda.ground(condition.term, namespaces=namespaces, context=trial_title)

    # if the grounding doesn't work, yield with ner
    if len(grounded) == 0:
        annotations = gilda.annotate(condition.term, namespaces=name_spaces,
                                     context_text=trial_title)

        for term, match, *_ in annotations:
            condition = copy.deepcopy(condition)
            condition.term = term
            condition.curie = match.term.get_curie()
            yield condition
    else:
        condition.curie = grounded[0].term.get_curie()
        yield condition


def ground_intervention(intervention: BioEntity, namespaces: list[str] = None, trial_title: str = None) -> Iterator[BioEntity]:
    if intervention.term == 'NULL':
        yield
    try:
        *intervention_type, intervention_term = intervention.term.split(':')
    except Exception:
        intervention_type=[]
        intervention = intervention.term

    context = '.'.join(intervention_type) + trial_title

    # first try grounding intervention with type and title context
    grounded = gilda.ground(intervention_term, namespaces=namespaces, context=context)

    # if the grounding doesn't work try ner
    if len(grounded) == 0:
        annotations = gilda.annotate(intervention.term, namespaces=namespaces, context_text=context)
        for term, match, *_ in annotations:
            intervention = copy.deepcopy(intervention)
            intervention.term = term
            intervention.curie = match.term.get_curie()
    else:
        intervention.curie = grounded[0].term.get_curie()


class Processor(BaseProcessor):
    def __init__(self):
        config = Config()
        super().__init__(
            config=config,
            fetcher=Fetcher(config),
            conditions_grounder=ground_condition,
            interventions_grounder=ground_intervention,
            condition_namespaces=["MESH", "doid", "mondo", "go"]
        )
