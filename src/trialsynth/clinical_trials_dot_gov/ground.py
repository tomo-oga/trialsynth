import os
import re
from overrides import overrides
from typing import Optional

import gilda
from gilda.process import normalize
from abbreviations.schwartz_hearst import extract_abbreviation_definition_pairs

from ..base.ground import ConditionGrounder, InterventionGrounder, Annotator
from ..base.models import BioEntity

class CosmicGeneAnnotator(Annotator):
    """Annotator that uses Gilda to annotate genes in text, filtering by the COSMIC gene census. It also aims to 
    resolve abbreviations in text by using the Schwartz-Hearst algorithm to minimize misgroundings.

    Parameters
    ----------
    namespaces : Optional[list[str]]
        A list of namespaces to consider for grounding (default: ['HGNC']).
    grounder : Optional[gilda.Grounder]
        A Gilda Grounder object to use for grounding (default: None). If None, a new Gilda Grounder object is created.
    
    Attributes
    ----------
    gene_census : list[str]
        A list of genes in the COSMIC gene census.
    ignore_terms : list[str]
        A list of terms to ignore during annotation.
    
    """
    def __init__(self, *, namespaces: Optional[list[str]] = ['HGNC'], grounder: Optional[gilda.Grounder] = None):
        super().__init__(namespaces=namespaces, grounder=grounder)
        self.gene_census = self._get_gene_census()
        self.ignore_terms = self._get_ignore_terms()

    @staticmethod
    def _get_gene_census() -> list[str]:
        """Get the list of genes in the COSMIC gene census.
        
        Returns
        -------
        list[str]
            A list of genes in the COSMIC gene census.
        """
        with open(os.path.join(os.path.dirname(__file__), 'resources/cosmic_genes.csv')) as f:
            return f.read().splitlines()
        
    @staticmethod
    def _get_ignore_terms() -> list[str]:
        """Get the list of terms to ignore during annotation.
        
        Returns
        -------
        list[str]
            A list of terms to ignore during annotation.
        """
        with open(os.path.join(os.path.dirname(__file__), 'resources/ignore.csv')) as f:
            ignore = [normalize(line.strip()) for line in f]
            ignore.extend(['ago', 'wish', 'warts', 'oasis', 'thc'])
        return ignore
    
    @staticmethod
    def preprocess_criteria(criteria: str) -> str:
        """Preprocess the criteria text by removing leading numbers and bullet points for compatibility with Schwartz-Hearst algorithm.
        
        Parameters
        ----------
        criteria : str
            The criteria text to preprocess.
        
        Returns
        -------
        str
            The preprocessed criteria text.
        """
        sentences = re.split(r'\n\n|\n', criteria)
        cleaned_sentences = []
        for sentence in sentences:
            cleaned_sentence = sentence.strip()
            cleaned_sentence = re.sub(r'^\*|^[0-9].', '', cleaned_sentence)
            if cleaned_sentence:
                cleaned_sentences.append(cleaned_sentence.strip())
        
        return '\n'.join(cleaned_sentences)
    
    def annotate(self, text: str, *, context: str = None):

        context_text = context if context is not None else text
        abbr_resolved_text = context_text

        cleaned_text = self.preprocess_criteria(text)
        abbreviations = extract_abbreviation_definition_pairs(doc_text=cleaned_text, first_definition=True)
        for sf, lf in abbreviations.items():
            abbr_resolved_text = abbr_resolved_text.replace(sf, lf)
        
        annotations = gilda.annotate(abbr_resolved_text, context_text=text, namespaces=self.namespaces)
        filtered_annotations = []
        for annotation in annotations:
            entry_name = annotation.matches[0].term.entry_name
            if entry_name in self.gene_census:
                norm_text = annotation.matches[0].term.entry_name
                if norm_text in self.ignore_terms:
                    continue
                try:
                    norm_text = int(norm_text) # ignore integers
                except ValueError:
                    filtered_annotations.append(annotation)
        return filtered_annotations

class CTConditionGrounder(ConditionGrounder):
    @overrides
    def preprocess(self, entity: BioEntity, *kwargs) -> BioEntity:
        return entity


class CTInterventionGrounder(InterventionGrounder):
    @overrides
    def preprocess(self, entity: BioEntity, *kwargs) -> BioEntity:
        return entity

