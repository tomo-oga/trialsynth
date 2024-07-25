import gilda
import copy

from typing import Callable, Iterator, Optional
from indra.databases import mesh_client

from .models import BioEntity

Grounder = Callable[[BioEntity, list[str], str], Iterator[BioEntity]]

condition_namespaces = ['MESH', 'DOID', 'EFO', 'HP', 'GO']
intervention_namespaces = ['CHEBI', 'MESH', 'EFO', 'HGNC']

PreProcessor = Callable[[BioEntity], BioEntity]


def ground_entity(
        entity: BioEntity,
        preprocessor: PreProcessor,
        namespaces: Optional[list[str]] = None,
        context: Optional[str] = None
) -> Iterator[BioEntity]:
    """Ground a BioEntity using Gilda and NER to a namespace and id

    Parameters
    ----------
    entity : BioEntity
        The entity to ground
    preprocessor : PreProcessor
        A function to preprocess the entity before grounding
    namespaces : list[str]
        The namespaces to ground to (default: None).
    context : Optional[str]
        The context to use for grounding (default: None).

    Yields
    ------
    BioEntity
        The grounded entity
    """
    # if already a grounded mesh term, ensure that it is right
    entity = preprocessor(entity)
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
        # if not grounded term, ground using gilda and ner
        matches = gilda.ground(entity.term, namespaces=namespaces, context=context)

        # if a match is found, use and yield:
        if matches:
            match = matches[0].term
            entity.ns, entity.id, entity.term = match.db, match.id, match.entry_name
            yield entity
        # if no match is found, try ner:
        else:
            annotations = gilda.annotate(entity.term, namespaces=namespaces, context_text=context)
            for _, match, *_ in annotations:
                match = match.term
                annotated_entity = copy.deepcopy(entity)
                annotated_entity.term = match.entry_name
                annotated_entity.ns = match.db
                annotated_entity.id = match.id
                yield annotated_entity

