import click

from ..base.config import Config
from ..base.models import BioEntity
from ..base.process import Processor

from .fetch import Fetcher

from typing import Iterator

import gilda
import copy


def ground_condition(condition: BioEntity, namespaces: list[str] = None, trial_title: str = None) -> Iterator[BioEntity]:

    # first try grounding condition with title context
    grounded = gilda.ground(condition.term, namespaces=namespaces, context=trial_title)

    # if the grounding doesn't work, yield with ner
    if len(grounded) == 0:
        annotations = gilda.annotate(condition.term, namespaces=namespaces,
                                     context_text=trial_title)

        for _, match, *_ in annotations:
            match = match.term
            grounded_condition = copy.deepcopy(condition)
            grounded_condition.term = match.entry_name
            grounded_condition.ns = match.db
            grounded_condition.id = match.id
            yield grounded_condition
    else:
        match = grounded[0].term
        grounded_condition = copy.deepcopy(condition)
        grounded_condition.ns = match.db
        grounded_condition.id = match.id
        yield grounded_condition


def ground_intervention(intervention: BioEntity, namespaces: list[str] = None, trial_title: str = None) -> Iterator[BioEntity]:
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
            match = match.term
            grounded_intervention = copy.deepcopy(intervention)
            grounded_intervention.term = match.entry_name
            grounded_intervention.ns = match.db
            grounded_intervention.id = match.id
            yield grounded_intervention
    else:
        match = grounded[0].term
        grounded_intervention = copy.deepcopy(intervention)
        grounded_intervention.term = match.entry_name
        grounded_intervention.ns = match.db
        grounded_intervention.id = match.id
        yield grounded_intervention


@click.command()
@click.option('--reload', default=False)
def main(reload: bool):
    config = Config(registry='who')
    processor = Processor(
        config=config,
        fetcher=Fetcher(config),
        conditions_grounder=ground_condition,
        interventions_grounder=ground_intervention,
        condition_namespaces=["MESH", "doid", "mondo", "go"],
        reload_api_data=reload
    )

    processor.run()

if __name__ == '__main__':
        main()
