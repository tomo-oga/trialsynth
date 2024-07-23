import copy
import logging
from typing import Iterator, Optional

import click
import gilda
from indra.databases import mesh_client

from ..base.models import BioEntity
from ..base.config import Config
from ..base.process import Processor
from .fetch import Fetcher

logger = logging.getLogger(__name__)


def ground_entity(entity: BioEntity, namespaces: list[str] = None, trial_title: str = None) -> Iterator[
    BioEntity]:

    # if already a grounded mesh term, ensure that it is right
    if entity.ns == 'MESH' and entity.id:
        if mesh_client.get_mesh_name(entity.id, offline=True):
            # if it is correct yield
            yield entity
        else:
            # if it is not correct, see if we can look up correct MESH id using gilda
            matches = gilda.ground(entity.term, namespaces=['MESH'])

            # if one is found, correct ns, id, and term, then yield
            if matches:
                match = matches[0].term
                entity.ns, entity.id, entity.term = match.db, match.id, match.entry_name
                yield entity
    else:
        # if not grounded term, ground using gilda
        matches = gilda.ground(entity.term, namespaces=namespaces, context=trial_title)

        # if a match is found, use and yield:
        if matches:
            match = matches[0].term
            entity.ns, entity.id, entity.term = match.db, match.id, match.entry_name
            yield entity
        # if no match is found, try ner:
        else:
            annotations = gilda.annotate(entity.term, namespaces=namespaces, context_text=trial_title)
            for _, match, *_ in annotations:
                match = match.term
                annotated_entity = copy.deepcopy(entity)
                annotated_entity.term = match.entry_name
                annotated_entity.ns = match.db
                annotated_entity.id = match.id
                yield annotated_entity


@click.command()
@click.option('--reload', default=False)
def main(reload: bool):
    config = Config(registry='clinicaltrials')
    processor = Processor(
        config=config,
        fetcher=Fetcher(config),
        conditions_grounder=ground_entity,
        interventions_grounder=ground_entity,
        reload_api_data=reload
    )

    processor.run()


if __name__ == "__main__":
    main()
