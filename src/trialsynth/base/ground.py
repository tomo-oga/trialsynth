import copy
from typing import Iterator, Optional

import gilda
from indra.databases import mesh_client

from .models import BioEntity, Gene
from .util import CONDITION_NS, GENE_NS, INTERVENTION_NS, must_override


class Grounder:
    """A callable class that grounds a BioEntity to a database identifier.

    Parameters
    ----------
    namespaces : Optional[list[str]]
        A list of namespaces to consider for grounding (default: None).

    Attributes
    ----------
    namespaces : Optional[list[str]]
        A list of namespaces to consider for grounding.
    """

    def __init__(self, namespaces: Optional[list[str]] = None):
        self.namespaces: Optional[list[str]] = namespaces

    @must_override
    def preprocess(self, entity: BioEntity, *kwargs) -> BioEntity:
        """Preprocess the BioEntity before grounding.

        This method can be overridden by subclasses to perform any necessary preprocessing
        steps on the BioEntity before grounding it.

        Parameters
        ----------
        entity : BioEntity
            The BioEntity to preprocess.
        *kwargs
            Additional keyword arguments.

        Returns
        -------
        BioEntity
            The preprocessed BioEntity.
        """

    def ground(self, entity: BioEntity, context: Optional[str]) -> Iterator[BioEntity]:
        """Ground a BioEntity to a CURIE.

        Parameters
        ----------
        entity : BioEntity
            The BioEntity to ground.
        context : Optional[str]
            The context of the grounding to aid with disambiguation.

        Yields
        ------
        BioEntity
            The grounded BioEntity.
        """
        entity = self.preprocess(entity)
        if entity.ns and entity.ns.upper() == "MESH" and entity.id:
            if mesh_client.get_mesh_name(entity.id, offline=True):
                yield entity
            else:
                matches = gilda.ground(entity.term, namespaces=['MESH'])
                if matches:
                    match = matches[0].term
                    entity.ns, entity.id, entity.term = match.db, match.id, match.entry_name
                    yield entity
        else:
            matches = gilda.ground(entity.term, namespaces=self.namespaces, context=context)
            if matches:
                match = matches[0].term
                entity.ns, entity.id, entity.text = (
                    match.db,
                    match.id,
                    match.entry_name,
                )
                yield entity
            else:
                annotations = gilda.annotate(entity.term, namespaces=self.namespaces, context_text=context)
                for _, match, *_ in annotations:
                    match = match.term
                    annotated_entity = copy.deepcopy(entity)
                    annotated_entity.text = match.entry_name
                    annotated_entity.ns = match.db
                    annotated_entity.id = match.id
                    yield annotated_entity

    def __call__(self, entity: Node, context: Optional[str] = None) -> Iterator[Node]:
        return self.ground(entity, context)


class ConditionGrounder(Grounder):
    """
    A class that represents a condition grounder.

    This class inherits from the :class:`Grounder` class and provides functionality specific to condition grounding.
    """

    def __init__(self):
        super().__init__(namespaces=CONDITION_NS)


class InterventionGrounder(Grounder):
    """
    A class that represents an intervention grounder.

    This class inherits from the :class:`Grounder` class and provides additional functionality specific to intervention grounding.
    """

    def __init__(self):
        super().__init__(namespaces=INTERVENTION_NS)


class GeneGrounder(Grounder):
    """
    A class that represents a criteria grounder.

    This class inherits from the `Grounder` class and provides additional functionality specific to criteria grounding.
    """

    def __init__(self):
        super().__init__(namespaces=GENE_NS)
