import copy
from typing import Iterator, Optional

import gilda
from gilda.process import normalize
from gilda.ner import stop_words
from indra.databases import mesh_client
from nltk.tokenize import PunktSentenceTokenizer, TreebankWordTokenizer

from .models import BioEntity
from .util import CONDITION_NS, GENE_NS, INTERVENTION_NS, must_override

grounder = gilda.Grounder()

def annotate(text: str, namespaces: Optional[list[str]] = None, context: Optional[str] = None, pre_grounded_terms: set[str] = None) -> list[list[gilda.ScoredMatch]]:
    if not pre_grounded_terms:
        pre_grounded_terms = set()
    grounded_terms: list[list[gilda.ScoredMatch]] = []

    context = context if context else text

    for sentence in PunktSentenceTokenizer().tokenize(text):
        raw_word_coords = list(TreebankWordTokenizer().span_tokenize(sentence.rstrip('.')))

        raw_words = [sentence[start:end] for start, end in raw_word_coords]
        words = [normalize(w) for w in raw_words]

        skip_until = 0
        for i, word in enumerate(words):
            if i < skip_until:
                continue
            if word in stop_words:
                continue
            spans = grounder.prefix_index.get(word, set())
            if not spans:
                continue

            applicable_spans = {span for span in spans if i + span <= len(words)}

            for span in sorted(applicable_spans, reverse=True):
                raw_span = ''
                for rw, c in zip(raw_words[i:i+span], raw_word_coords[i:i+span]):
                    spaces = ' ' * (c[0] - len(raw_span) - raw_word_coords[i][0])
                    raw_span += spaces + rw
                if len(raw_span) <= 1:
                    continue
                if raw_span.lower() in pre_grounded_terms:
                    skip_until = i + span
                    break

                matches = grounder.ground(raw_span, context=context, namespaces=namespaces)

                if matches:
                    pre_grounded_terms.add(raw_span.lower())

                    grounded_terms.append(matches)

                    skip_until = i + span
                    break

    return grounded_terms



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

    def ground(self, entity: BioEntity, context: Optional[str] = None, grounded_terms: set[str] = None) -> Iterator[BioEntity]:
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

        if not grounded_terms:
            grounded_terms = set()

        entity = self.preprocess(entity)
        if entity.ns and entity.ns.upper() == "MESH" and entity.id:
            mesh_name = mesh_client.get_mesh_name(entity.id, offline=True)
            if mesh_name:
                grounded_terms.add(mesh_name.lower())
                yield entity
            else:
                if entity.text not in grounded_terms:
                    matches = grounder.ground(entity.text, namespaces=['MESH'])
                    if matches:
                        grounded_terms.add(entity.text.lower())
                        match = matches[0].term
                        entity.ns, entity.id, entity.text = match.db, match.id, match.entry_name
                        yield entity
        else:
            if entity.text not in grounded_terms:
                matches = grounder.ground(entity.text, namespaces=self.namespaces, context=context)
                if matches:
                    grounded_terms.add(entity.text.lower())
                    match = matches[0].term
                    entity.ns, entity.id, entity.text = (
                        match.db,
                        match.id,
                        match.entry_name,
                    )
                    yield entity
                else:
                    matches = annotate(entity.text, namespaces=self.namespaces, context=context, pre_grounded_terms=grounded_terms)
                    for match in matches:
                        match = match[0].term
                        annotated_entity = copy.deepcopy(entity)
                        annotated_entity.text = match.entry_name
                        annotated_entity.ns = match.db
                        annotated_entity.id = match.id
                        yield annotated_entity

    def __call__(self, entity: BioEntity, context: Optional[str] = None, grounded_terms: set[str] = None) -> Iterator[BioEntity]:
        return self.ground(entity, context, grounded_terms)


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
