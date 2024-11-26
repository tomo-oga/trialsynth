import copy
import logging
import warnings
from typing import Iterator, Optional, Callable, Tuple 

nmslib_logger = logging.getLogger('nmslib')
nmslib_logger.setLevel(logging.ERROR)
warnings.simplefilter('ignore')

import gilda
from gilda.grounder import Annotation, ScoredMatch
from indra.databases import mesh_client
import scispacy
import spacy
from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker

from .models import BioEntity
from .util import must_override



logger = logging.getLogger(__name__)




class Annotator:
    """Annotator class that sets the behavior for annotating text.

    Parameters
    ----------
    namespaces : Optional[list[str]], optional
        The namespaces to ground to, by default ["MESH"]
    grounder : Optional[gilda.Grounder], optional
        The grounder to use, by default None, which calls the default `gilda.Grounder()`
    """
    def __init__(
        self,
        *,
        namespaces: Optional[list[str]] = ["MESH"],
        grounder: Optional[gilda.Grounder] = None
    ):
        self.namespaces = namespaces

        if not grounder:
            grounder = gilda.Grounder()

        self.grounder = grounder

    def __call__(self, text: str, *, context: str = None) -> list[Annotation]:
        return self.annotate(text, context=context)

    @must_override
    def annotate(self, text: str, *, context: str = None) -> list[Annotation]:
        pass

class GildaAnnotator(Annotator):
    def annotate(self, text: str, *, context: str = None) -> list[Annotation]:
        """Annotates and grounds entities to unique concept identifiers using Gilda.

        Parameters
        ----------
        text : str
            The text to annotate.
        context : str, optional
            The text to use for disambiguation, by default None

        Returns
        -------
        list[Annotation]    
            The list of annotations for the text.
        """
        return gilda.annotate(text=text, context_text=context, namespaces=self.namespaces)

class SciSpacyAnnotator(Annotator):
    def __init__(self, *, model: str, namespaces: Optional[list[str]] = None, grounder: Optional[gilda.Grounder] = None):
        super().__init__(namespaces=namespaces, grounder=grounder)
        try:
            self.model = spacy.load(model)
        except OSError:
            logger.info("spaCy model not found")
            raise

    def annotate(self, text: str, *, context: str = None):
        context_text = context if context is not None else text
        doc = self.model(text)

        annotations: list[Annotation] = []
        for entity in doc.ents:
            matches = self.grounder.ground(entity.text, namespaces=self.namespaces, context=context_text)
            if matches:
                annotations.append(
                    Annotation(entity.text, matches, entity.start_char, entity.end_char)
                )
            return annotations




class Grounder:
    """A callable class that annotates and grounds BioEntities to unique concept identifiers.

    Parameters
    ----------
    restrict_mesh_prefix : list[str], optional
        The mesh prefixes to restrict by, by default None
    annotator : Callable[[str], list[Tuple[Annotation]]], optional
        The annotator to use for grounding, by default Annotator()
    """
    def __init__(
        self,
        *,
        restrict_mesh_prefix: list[str] = None,
        annotator: Callable[[str], list[Tuple[Annotation]]] = Annotator(),
    ):
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
        pass

    def __call__(
        self, entity: BioEntity, context: Optional[str] = None
    ) -> Iterator[BioEntity]:
        return self.ground(entity, context)
    
    def _create_grounded_entity(
        self, entity: BioEntity, *, mesh_id: str, norm_text: str
    ) -> BioEntity:
        grounded_entity = copy.deepcopy(entity)
        grounded_entity.ns = 'MESH'
        grounded_entity.ns_id = mesh_id
        grounded_entity.grounded_term = norm_text
        return grounded_entity

    def _yield_entity(
        self, entity: BioEntity, match: ScoredMatch
    ) -> Iterator[BioEntity]:
        groundings_dict = dict(match.get_groundings())
        mesh_id = groundings_dict.get('MESH')
        
        if mesh_id:
            if self.restrict_mesh_prefix and any(mesh_client.has_tree_prefix(mesh_id, prefix) for prefix in self.restrict_mesh_prefix):
                yield self._create_grounded_entity(
                    entity, mesh_id=mesh_id, norm_text=match.term.entry_name
                )
            if not self.restrict_mesh_prefix:
                yield self._create_grounded_entity(
                    entity, mesh_id=mesh_id, norm_text=match.term.entry_name
                )

    def ground(
        self, entity: BioEntity, context: Optional[str] = None
    ) -> Iterator[BioEntity]:
        """Ground a BioEntity to a CURIE."""
        entity = self.preprocess(entity)
        if entity.ns and entity.ns.upper() == "MESH" and entity.ns_id:
            mesh_name = mesh_client.get_mesh_name(entity.ns_id, offline=True)
            if mesh_name:
                entity.grounded_term = mesh_name
                yield entity
            else:
                matches = gilda.ground(entity.text, namespaces=["MESH"])
                if matches:
                    yield from self._yield_entity(entity, matches[0])
        else:
            matches = gilda.ground(
                entity.text, namespaces=['MESH'], context=context
            )
            if matches:
                yield from self._yield_entity(entity, matches[0])
            else:
                annotations = self.annotator(entity.text)
                for annotation in annotations:
                    yield from self._yield_entity(entity, annotation.matches[0])
                    


class ConditionGrounder(Grounder):
    def __init__(self):
        super().__init__(restrict_mesh_prefix=['C', 'F'], annotator=GildaAnnotator())


class InterventionGrounder(Grounder):
    def __init__(self):
        super().__init__(restrict_mesh_prefix=['D', 'E'], annotator=GildaAnnotator())
