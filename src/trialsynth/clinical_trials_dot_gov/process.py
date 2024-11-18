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


class CTProcessor(Processor):
    def __init__(self, reload_api_data: bool, store_samples: bool, validate: bool):
        super().__init__(
            config=CTConfig(),
            fetcher=CTFetcher(CTConfig()),
            transformer=CTTransformer(),
            validator=CTValidator(),
            grounders=(CTConditionGrounder(), CTInterventionGrounder()),
            reload_api_data=reload_api_data,
            store_samples=store_samples,
            validate=validate,
        )
        self.gene_extractor = CosmicGeneAnnotator()
    
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

