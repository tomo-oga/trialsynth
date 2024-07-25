import logging

from pathlib import Path
from typing import Dict, Optional, Callable

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from .config import Config
from .fetch import BaseFetcher
from .models import Trial, Edge, BioEntity
from .ground import ground_entity, PreProcessor
from .validate import Validator

from . import store
from . import transform

import gilda

logger = logging.getLogger(__name__)


class Processor:
    """Processes registry data using Config and Fetcher objects to graph data.

    Attributes
    ----------
    config : Config
        User-mutable properties of registry data processing
    fetcher : Fetcher
        Fetches registry data from the REST API or a saved file
    condition_namespaces : Optional[list[str]]
        Namespaces to use for grounding conditions
    intervention_namespaces : Optional[list[str]]
        Namespaces to use for grounding
    conditions : list[BioEntity]
        List of conditions from the trials to be grounded
    interventions : list[BioEntity]
        List of interventions from the trials to be grounded
    edges : list[Edge]
        List of edges connecting trials to conditions and interventions
    reload_api_data : bool
        Whether to reload the API data
    store_samples : bool
        Whether to store samples of the data

    Parameters
    ----------
    config : Config
        User-mutable properties of registry data processing
    fetcher : Fetcher
        Fetches registry data from the REST API or a saved file
    condition_preprocessor : PreProcessor
        Logic to preprocess conditions before grounding
    intervention_preprocessor : PreProcessor
        Logic to preprocess interventions before grounding
    condition_namespaces : Optional[list[str]]
        Namespaces to use for grounding conditions
    intervention_namespaces : Optional[list[str]]
        Namespaces to use for grounding
    reload_api_data : bool
        Whether to reload the API data (default: False).
    store_samples : bool
        Whether to store samples of the data (default: False).
    validate : bool
        Whether to validate the data (default: True).
    """

    def __init__(
            self,
            config: Config,
            fetcher: BaseFetcher,
            condition_preprocessor: PreProcessor = lambda x: x,
            intervention_preprocessor: PreProcessor = lambda x: x,
            condition_namespaces: Optional[list[str]] = None,
            intervention_namespaces: Optional[list[str]] = None,
            reload_api_data: bool = False,
            store_samples: bool = False,
            validate: bool = True,
    ):
        self.config = config
        self.fetcher = fetcher
        self.validator = Validator()

        self.trials: list[Trial] = []

        self.curie_to_trial: Dict[str, Trial] = {}

        self.condition_preprocessor: PreProcessor = condition_preprocessor
        self.interventions_preprocessor: PreProcessor = intervention_preprocessor

        self.condition_namespaces: Optional[list[str]] = condition_namespaces
        self.intervention_namespaces: Optional[list[str]] = intervention_namespaces

        self.conditions: list[BioEntity] = []
        self.interventions: list[BioEntity] = []

        self.edges: list[Edge] = []

        self.reload_api_data: bool = reload_api_data
        self.store_samples: bool = store_samples
        self.validate: bool = validate

    def run(self):
        """Processes registry data into a graph structure."""
        self.fetcher.get_api_data(reload=self.reload_api_data)
        self.trials = self.fetcher.raw_data
        #  ground and process bioentities for storing
        self.get_bioentities()
        self.process_bioentities()

        # remove duplicate trial entries, using this instead of curie_trial_dict to avoid accessing hash structure
        self.trials = list(set(self.trials))

        # create edges
        self.create_edges()

        # save processed data
        self.save_data()

        # validate data
        if self.validate:
            self.validate_data()

    def get_bioentities(self):
        """Extracts bioentities from trials and creates a dictionary of trial CURIEs to trials."""
        iterated_trials = set()

        for trial in self.trials:

            # should be refactored later to accept various connection types. i.e. criteria
            self.conditions.extend([condition for condition in trial.conditions])
            self.interventions.extend([intervention for intervention in trial.interventions])

            # clearing trial of entities for grounding
            trial.conditions = []
            trial.interventions = []

            if trial.curie not in iterated_trials:
                iterated_trials.add(trial.curie)
                self.curie_to_trial[trial.curie] = trial

    def process_bioentities(self):
        """Processes bioentities by grounding them."""
        logger.info('Warming up grounder...')
        gilda.ground("stuff")
        logger.info('Done.')
        self.process_conditions()
        self.process_interventions()

    def process_conditions(self):
        """Processes conditions by grounding them using the condition preprocessor."""
        condition_iter = tqdm(self.conditions, desc="Grounding Conditions", unit="condition", unit_scale=True)

        for condition in condition_iter:
            with logging_redirect_tqdm():
                trial = self.curie_to_trial[condition.origin]
                conditions = list(
                    ground_entity(
                        condition,
                        preprocessor=self.condition_preprocessor,
                        namespaces=self.condition_namespaces,
                        context=trial.title
                    )
                )
                trial.conditions.extend(conditions)

    def process_interventions(self):
        """Processes interventions by grounding them using the intervention preprocessor."""
        intervention_iter = tqdm(self.interventions, desc="Grounding Interventions", unit="intervention",
                                 unit_scale=True)

        for intervention in intervention_iter:
            with logging_redirect_tqdm():
                trial = self.curie_to_trial[intervention.origin]
                interventions = list(
                    ground_entity(
                        intervention,
                        preprocessor=self.interventions_preprocessor,
                        namespaces=self.intervention_namespaces,
                        context=trial.title
                ))

                trial.interventions.extend(interventions)

    def create_edges(self):
        """Creates edges connecting trials to conditions and interventions."""
        for trial in tqdm(self.trials, desc="Generating edges from trial", unit='trial', unit_scale=True):
            self.edges.extend([
                Edge(condition.curie, trial.curie, 'has_condition', self.config.registry)
                for condition in set(trial.conditions) if condition
            ])

            self.edges.extend([
                Edge(intervention.curie, trial.curie, 'has_intervention', self.config.registry)
                for intervention in set(trial.interventions) if intervention
            ])

    def save_trial_data(self, path: Path, sample_path: Optional[Path] = None) -> None:
        """Saves processed trial data to a compressed tsv file.

        Parameters
        ----------
        path : Path
            The path to save the processed trial data
        sample_path
            The path to save the sample trial data (default: None).

        Returns
        -------
        None
        """
        data = [transform.flatten_trial_data(trial) for trial in self.trials]

        headers = [
            'curie:CURIE',
            'title:string',
            'labels:LABEL[]',
            'design:DESIGN',
            'conditions:CURIE[]',
            'interventions:CURIE[]',
            'primary_outcome:OUTCOME[]',
            'secondary_outcome:OUTCOME[]',
            'secondary_ids:CURIE[]',
            'source_registry:string'
            ]

        store.save_data_as_flatfile(
            data,
            path=path,
            headers=headers,
            sample_path=sample_path,
            num_samples=self.config.num_sample_entries
        )

    def save_bioentities(self, path: Path, sample_path: Optional[Path] = None) -> None:
        """Saves processed bioentities to a compressed tsv file.

        Parameters
        ----------
        path : Path
            The path to save the processed bioentities
        sample_path
            The path to save the sample bioentities (default: None).

        Returns
        -------
        None
        """
        entities = []
        for trial in self.trials:
            entities.extend([condition for condition in trial.conditions])
            entities.extend([intervention for intervention in trial.interventions])

        entities = list(set(entities))
        entities = [transform.flatten_bioentity(entity) for entity in entities if entity]
        store.save_data_as_flatfile(
            entities,
            path=path,
            headers=['curie:CURIE', 'term:string', 'labels:LABEL[]', 'source_registry:string'],
            sample_path=sample_path,
            num_samples=self.config.num_sample_entries
        )

    def save_edges(self, path: Path, sample_path: Optional[Path] = None):
        """Saves processed edges to a compressed tsv file.

        Parameters
        ----------
        path : Path
            The path to save the processed edges
        sample_path
            The path to save the sample edges (default: None).

        Returns
        -------
        None
        """
        edges = [transform.flatten_edge(edge) for edge in self.edges]

        store.save_data_as_flatfile(
            sorted(edges),
            path=path,
            headers=['from:CURIE', 'to:CURIE', 'rel_type:string', 'rel_curie:CURIE', 'source_registry:string'],
            sample_path=sample_path,
            num_samples=self.config.num_sample_entries
        )

    def save_data(self):
        """Saves processed data to compressed tsv files."""

        if not self.config.sample_dir.is_dir():
            self.config.sample_dir.mkdir()

        # save processed trial data to compressed tsv
        logger.info(f'Serializing and storing processed trial data to {self.config.trials_path}')
        self.save_trial_data(
            self.config.trials_path,
            sample_path=self.config.trials_sample_path if self.store_samples else None
        )

        # save processed bioentity data to compressed tsv
        logger.info(f'Serializing and storing grounded bioentities to {self.config.bio_entities_path}')
        self.save_bioentities(
            self.config.bio_entities_path,
            sample_path=self.config.bio_entities_sample_path if self.store_samples else None
        )

        # save edges to compressed tsv
        logger.info(f'Serializing and storing edges to {self.config.edges_path}')
        self.save_edges(
            self.config.edges_path,
            sample_path=self.config.edges_sample_path if self.store_samples else None
        )

    def validate_data(self):
        """Validates the processed data using the Validator object."""
        logger.info(f'Validating trial data.')
        self.validator.validate(self.config.trials_path)
        logger.info(f'Validating bioentity data.')
        self.validator.validate(self.config.bio_entities_path)
        logger.info(f'Validating edge data.')
        self.validator.validate(self.config.edges_path)
