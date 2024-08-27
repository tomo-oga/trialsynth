import copy
import logging
from typing import Iterator, Optional

import gilda
from gilda.grounder import Annotation
from indra.databases import mesh_client

from .models import BioEntity
from .util import (CONDITION_NS, INTERVENTION_NS, must_override,
                   suppress_logging_info, suppress_warnings)

with suppress_logging_info():
    import scispacy
    import spacy
    from scispacy.abbreviation import AbbreviationDetector
    from scispacy.linking import EntityLinker

logger = logging.getLogger(__name__)


class Annotator:
    def __init__(
        self,
        model: str = "gilda",
        *,
        namespaces: Optional[list[str]] = ["MESH"],
    ):

        self.namespaces = namespaces
        if model == "gilda":
            self.model = model
            return
        with suppress_logging_info(), suppress_warnings():
            try:
                self.model = spacy.load(model)
            except OSError:
                logger.info("spaCy model not found. Defaulting to gilda for NER.")
                self.model = "gilda"

    def __call__(self, text: str) -> list[Annotation]:
        if self.model == "gilda":
            return gilda.annotate(text, namespaces=self.namespaces)
        return self.annotate(text)

    def annotate(self, text: str, *, context: str = None):
        context_text = context if context is not None else text
        doc = self.model(text)

        annotations: list[Annotation] = []
        for entity in doc.ents:
            matches = gilda.ground(
                entity.text, namespaces=self.namespaces, context=context_text
            )
            if matches:
                annotations.append(
                    Annotation(entity.text, matches, entity.start_char, entity.end_char)
                )
        return annotations


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

    def __init__(
        self,
        *,
        namespaces: Optional[list[str]] = None,
        restrict_mesh_prefix: list[str] = None,
        annotator: Callable[[str], list[Tuple[Annotation]]] = Annotator(),
    ):
        self.namespaces: Optional[list[str]] = namespaces
        self.restrict_mesh_prefix = restrict_mesh_prefix
        self.annotator = annotator

    @must_override
    def preprocess(self, entity: BioEntity) -> BioEntity:
        """Preprocess the BioEntity before grounding.

        This method can be overridden by subclasses to perform any necessary preprocessing
        steps on the BioEntity before grounding it.

        Parameters
        ----------
        entity : BioEntity
            The BioEntity to preprocess.

        Returns
        -------
        BioEntity
            The preprocessed BioEntity.
        """

    def __call__(
        self, entity: BioEntity, context: Optional[str] = None
    ) -> Iterator[BioEntity]:
        return self.ground(entity, context)

    def ground(
        self, entity: BioEntity, context: Optional[str] = None
    ) -> Iterator[BioEntity]:
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
        if entity.ns and entity.ns.upper() == "MESH" and entity.ns_id:
            mesh_name = mesh_client.get_mesh_name(entity.ns_id, offline=True)
            if mesh_name:
                entity.grounded_term = mesh_name
                yield entity
            else:
                # if entity.text not in grounded_terms:
                matches = gilda.ground(entity.text, namespaces=["MESH"])
                if matches:
                    match = matches[0]
                    for db, id in match.get_groundings():
                        if db == "MESH":
                            entity.ns, entity.ns_id, entity.grounded_term = (
                                db,
                                id,
                                match.term.norm_text,
                            )
                            yield entity
        else:
            matches = gilda.ground(
                entity.text, namespaces=self.namespaces, context=context
            )
            if matches:
                match = matches[0]
                for db, id in match.get_groundings():
                    if db == "MESH":
                        if self.restrict_mesh_prefix:
                            if any(
                                mesh_client.has_tree_prefix(id, prefix)
                                for prefix in self.restrict_mesh_prefix
                            ):
                                grounded_entity = copy.deepcopy(entity)
                                (
                                    grounded_entity.ns,
                                    grounded_entity.ns_id,
                                    grounded_entity.grounded_term,
                                ) = (
                                    db,
                                    id,
                                    match.term.norm_text,
                                )
                                yield grounded_entity
                                break
                        else:
                            grounded_entity = copy.deepcopy(entity)
                            (
                                grounded_entity.ns,
                                grounded_entity.ns_id,
                                grounded_entity.grounded_term,
                            ) = (
                                db,
                                id,
                                match.term.norm_text,
                            )
                            yield grounded_entity
                            break
            else:
                annotations = self.annotator(entity.text)
                for annotation in annotations:
                    for db, id in annotation.matches[0].get_groundings():
                        if db == "MESH":
                            if self.restrict_mesh_prefix:
                                if any(
                                    mesh_client.has_tree_prefix(id, prefix)
                                    for prefix in self.restrict_mesh_prefix
                                ):
                                    annotated_entity = copy.deepcopy(entity)
                                    (
                                        annotated_entity.ns,
                                        annotated_entity.ns_id,
                                        annotated_entity.grounded_term,
                                    ) = (
                                        db,
                                        id,
                                        annotation.matches[0].term.norm_text,
                                    )
                                    yield annotated_entity
                                    break
                            else:
                                annotated_entity = copy.deepcopy(entity)
                                (
                                    annotated_entity.ns,
                                    annotated_entity.ns_id,
                                    annotated_entity.grounded_term,
                                ) = (
                                    db,
                                    id,
                                    annotation.matches[0].term.norm_text,
                                )
                                yield annotated_entity
                                break

    def __call__(
        self, entity: BioEntity, context: Optional[str] = None
    ) -> Iterator[BioEntity]:
        return self.ground(entity, context)


class ConditionGrounder(Grounder):
    def __init__(self):
        super().__init__(namespaces=CONDITION_NS)


class InterventionGrounder(Grounder):
    def __init__(self):
        super().__init__(namespaces=INTERVENTION_NS)
