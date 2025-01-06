import re
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from indra.databases import mesh_client

from ..base.process import Processor
from ..base.models import Gene
from .config import CTConfig
from .fetch import CTFetcher
from .ground import CTConditionGrounder, CTInterventionGrounder, CosmicGeneAnnotator
from .transform import CTTransformer
from .validate import CTValidator

def _split_criteria(criteria: str) -> str:
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
        
        return cleaned_sentences


class CTProcessor(Processor):
    def __init__(self, reload_api_data: bool, store_samples: bool, validate: bool, device: str):
        super().__init__(
            config=CTConfig(),
            fetcher=CTFetcher(CTConfig()),
            transformer=CTTransformer(),
            validator=CTValidator(),
            sentence_split_fun=_split_criteria,
            grounders=(CTConditionGrounder(), CTInterventionGrounder()),
            reload_api_data=reload_api_data,
            store_samples=store_samples,
            validate=validate,
            device=device
        )
        self.gene_extractor = CosmicGeneAnnotator()
    
    # TODO: with preprocess criteria gone, must do here
    def extract_gene_mentions_from_criteria(self):
        entity_iter = tqdm(self.trials, desc=f'Extracting genes', unit='gene', unit_scale=True)
        for trial in entity_iter:
            if isinstance(trial.criteria, str) or not trial.criteria:
                continue

            if not any(mesh_client.has_tree_prefix(entity.ns_id, 'C04') for entity in trial.entities):
                continue
            
            with logging_redirect_tqdm():
                for annotation in self.gene_extractor(trial.criteria.inclusion):
                    match = annotation.matches[0]
                    gene = Gene(text = match.term.entry_name, id = match.term.id, origin=trial.curie, source=self.config.registry)
                    trial.entities.append(gene)



    def process_bioentities(self):
        super().process_bioentities()
        self.extract_gene_mentions_from_criteria()

