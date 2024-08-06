import copy
import os
import logging
from typing import Iterator, Optional, Callable, Tuple

import gilda
from gilda.grounder import ScoredMatch, Annotation
from indra.databases import mesh_client
from overrides import overrides
import scispacy
import spacy
from scispacy.linking import EntityLinker
from scispacy.abbreviation import AbbreviationDetector
import torch

from .models import BioEntity
from .util import CONDITION_NS, GENE_NS, INTERVENTION_NS, must_override


logger = logging.getLogger(__name__)
from typing import Optional
from gilda.grounder import Annotation

class Annotator:
    def __init__(
            self, 
            model: str = 'en_core_sci_lg', 
            *, 
            entity_linker: Optional[str] = 'umls', 
            detect_abbreviations: bool = True,
            namespaces: Optional[list[str]] = None,
            restrict_types: Optional[list[str]] = None
            ):
        
        self.namespaces = namespaces
        self.restrict_types = restrict_types
        self.entity_linker = entity_linker

        try:
            self.model = spacy.load(model)
        except OSError:
            raise
        
        if detect_abbreviations:
            self.model.add_pipe('abbreviation_detector')
        
        if self.entity_linker:
            self.model.add_pipe("scispacy_linker", config={"resolve_abbreviations": detect_abbreviations, "linker_name": entity_linker})

        
    
    def annotate(self, text: str, *, context: str = None):
        context_text = context if context is not None else text
        doc = self.model(text)

        if self.entity_linker:
            linker = self.model.get_pipe('scispacy_linker')
        annotations: list[Annotation] = []
        for entity in doc.ents:
            if self.entity_linker:
                for kb_ent in entity._.kb_ents:
                    kb_ent = linker.kb.cui_to_entity[kb_ent[0]]
                    if not self.restrict_types or any(type in kb_ent.types for type in self.restrict_types):
                        matches = gilda.ground(kb_ent.canonical_name, namespaces=self.namespaces, context=context_text)
                        if matches:
                            annotations.append(Annotation(entity.text, matches, entity.start_char, entity.end_char))
        
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

    def __init__(self, namespaces: Optional[list[str]] = None, annotator: Callable[[str], list[Tuple[Annotation]]] = gilda.annotate):
        self.namespaces: Optional[list[str]] = namespaces
        self.annotator = annotator
        # self.nlp.add_pipe("abbreviation_detector")
        # self.nlp.add_pipe("umls_linker", config={"resolve_abbreviations": True, "linker_name": "umls"})

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

    def __call__(self, entity: BioEntity, context: Optional[str] = None) -> Iterator[BioEntity]:
        return self.ground(entity, context)
    
    def ground(self, entity: BioEntity, context: Optional[str] = None) -> Iterator[BioEntity]:
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

        spacy.require_gpu()
        entity = self.preprocess(entity)
        if entity.ns and entity.ns.upper() == "MESH" and entity.ns_id:
            mesh_name = mesh_client.get_mesh_name(entity.ns_id, offline=True)
            if mesh_name:
                entity.grounded_term = mesh_name
                yield entity
            else:
                # if entity.text not in grounded_terms:
                matches = gilda.ground(entity.text, namespaces=['MESH'])
                if matches:
                    match = matches[0].term
                    entity.ns, entity.ns_id, entity.grounded_term = match.db, match.id, match.norm_text
                    yield entity
        else:
            matches = gilda.ground(entity.text, namespaces=self.namespaces, context=context)
            if matches:
                match = matches[0].term
                entity.ns, entity.ns_id, entity.grounded_term = (
                    match.db,
                    match.id,
                    match.norm_text,
                )
                yield entity
            else:
                doc = self.nlp(entity.text)
                # TODO: add entitylinker for better grounding
                
                for ent in doc.ents:
                    matches = gilda.ground(ent.text, namespaces=self.namespaces, context=context)
                    if matches:
                        matched_entity = copy.deepcopy(entity)
                        match = matches[0].term
                        matched_entity.ns, matched_entity.ns_id, matched_entity.grounded_term = (match.db, match.id, match.norm_text)
                        yield matched_entity
                


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

    This class inherits from the `Grounder` class and provides additional functionality specific to gene grounding.
    """

    def __init__(self):
        super().__init__(namespaces=GENE_NS)

class GeneVariantGrounder(Grounder):
    def __init__(self):
        super().__init__(namespaces='umls')
    
    # @overrides
    # def ground(self, entity: BioEntity, context: str):
    #     nlp = spacy.load('en_core_sci_sm')
