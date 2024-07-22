import logging
from typing import Iterator, Optional

import click
import gilda
from indra.databases import mesh_client

from src.trialsynth.base.models import BioEntity
from ..base.config import Config
from ..base.process import Processor
from .fetch import Fetcher

logger = logging.getLogger(__name__)


def get_correct_mesh_id(mesh_id: str, mesh_term: Optional[str] = None) -> str:
    """
    Get a correct MeSH ID from a possibly incorrect one.

    Parameters
    ----------
    mesh_id : str
        The MeSH ID to correct.
    mesh_term : Optional[str]
        The MeSH term corresponding to the MeSH ID. Default: None

    Returns
    -------
    str
        The corrected MeSH ID.
    """
    # A proxy for checking whether something is a valid MeSH term is
    # to look up its name
    name = mesh_client.get_mesh_name(mesh_id, offline=True)
    if name:
        return mesh_id

    if mesh_id.startswith('CHEBI:'):
        return mesh_client.get_mesh_id_from_db_id('CHEBI:', mesh_id)
    # A common issue is with zero padding, where 9 digits are used
    # instead of the correct 6, and we can remove the extra zeros
    # to get a valid ID
    else:
        short_id = mesh_id[0] + mesh_id[4:]
        name = mesh_client.get_mesh_name(short_id, offline=True)
        if name:
            return short_id
    # Another pattern is one where the MeSH ID is simply invalid but the
    # corresponding MeSH term allows us to get a valid ID via reverse
    # ID lookup - done here as grounding just to not have to assume
    # perfect / up to date naming conventions in the source data.
    if mesh_term:
        matches = gilda.ground(mesh_term, namespaces=["MESH"])
        if len(matches) == 1:
            for k, v in matches[0].get_groundings():
                if k == "MESH":
                    return v
    return None


def ground_condition(condition: BioEntity, namespaces: list[str] = None, trial_title: str = None) -> Iterator[
    BioEntity]:
    matches = gilda.ground(condition.name, namespaces=namespaces, context=trial_title)

    if matches:
        match = matches[0]
        yield BioEntity(
            ns=match.term.db,
            id=match.term.id,
            term=match.term.entry_name,
            origin=condition.origin
        )
    else:
        correct_mesh_id = get_correct_mesh_id(condition.id, condition.term)
        if not correct_mesh_id:
            yield
        else:
            yield BioEntity(
                ns="MESH",
                id=correct_mesh_id,
                term=condition.term,
                origin=condition.origin
            )

def ground_intervention(intervention: BioEntity, namespaces: list[str] = None, trial_title: str = None) -> Iterator[BioEntity]:

    # if ID maps right, just yield the intervention
    if intervention.id:
        term = mesh_client.get_mesh_name(mesh_id=intervention.id)
        if term is not None:
            yield intervention
    if intervention.type == 'Drug':
        matches = gilda.ground(intervention.name, namespaces=namespaces, context=trial_title)

        if matches:
            match = matches[0]
            yield BioEntity(
                ns=match.term.db,
                id=match.term.id,
                term=match.term.entry_name,
                origin=intervention.origin
            )


@click.command()
@click.option('--reload', default=False)
def main(reload: bool):
    config = Config(registry='clinicaltrials')
    processor = Processor(
        config=config,
        fetcher=Fetcher(config),
        conditions_grounder=ground_condition,
        interventions_grounder=ground_intervention,
        condition_namespaces=['MESH', 'DOID', 'EFO', 'HP', 'GO'],
        reload_api_data=reload
    )

    processor.run()


if __name__ == "__main__":
    main()
