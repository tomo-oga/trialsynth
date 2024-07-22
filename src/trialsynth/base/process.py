import logging
from pathlib import Path
from typing import Dict, Optional, Callable, Iterator

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from .config import Config
from .fetch import BaseFetcher
from .models import Trial, Edge, BioEntity
from . import store
from . import transform

import gilda

logger = logging.getLogger(__name__)

Grounder = Callable[[BioEntity, list[str], str], Iterator[BioEntity]]


class Processor:
    """Processes registry data using Config, Fetcher, Storer, and Transformer objects.

    Attributes
    ----------
    config : Config
        User-mutable properties of registry data processing
    fetcher : Fetcher
        Fetches registry data from the REST API or a saved file
    trials : list[Trial]
        Raw data from the API or saved file
    curie_to_trial : Dict[str, Trial]
        Mapping of trial CURIEs to Trial objects
    conditions_grounder : Grounder
        Grounds conditions to BioEntities
    interventions_grounder : Grounder
        Grounds interventions to BioEntities
    condition_namespaces : Optional[list[str]]
        Namespaces to use for grounding conditions
    intervention_namespaces : Optional[list[str]]
        Namespaces to use for grounding interventions
    conditions : list[BioEntity]
        List of conditions extracted from trials
    interventions : list[BioEntity]
        List of interventions extracted from trials
    edges : list[Edge]
        List of edges for the graph database

    Parameters
    ----------
    config : Config
        User-mutable properties of registry data processing
    fetcher : Fetcher
        Fetches registry data from the REST API or a saved file
    conditions_grounder : Grounder
        Grounds conditions to BioEntities
    interventions_grounder : Grounder
        Grounds interventions to BioEntities
    condition_namespaces : Optional[list[str]]
        Namespaces to use for grounding conditions
    intervention_namespaces : Optional[list[str]]
        Namespaces to use for grounding
    """

    def __init__(
            self,
            config: Config,
            fetcher: BaseFetcher,
            conditions_grounder: Grounder,
            interventions_grounder: Grounder,
            condition_namespaces: Optional[list[str]] = None,
            intervention_namespaces: Optional[list[str]] = None,
    ):
        self.config = config
        self.fetcher = fetcher

        self.trials: list[Trial] = []

        self.curie_to_trial: Dict[str, Trial] = {}

        self.conditions_grounder: Grounder = conditions_grounder
        self.interventions_grounder: Grounder = interventions_grounder

        self.condition_namespaces: Optional[list[str]] = condition_namespaces
        self.intervention_namespaces: Optional[list[str]] = intervention_namespaces

        self.conditions: list[BioEntity] = []
        self.interventions: list[BioEntity] = []

        self.edges: list[Edge] = []

    def run(self):
        self.fetcher.get_api_data()
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


    def get_bioentities(self):
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
        logging.info('Warming up grounder...')
        gilda.ground("stuff")
        logger.info('Done.')
        self.process_conditions()
        self.process_interventions()

    def process_conditions(self):
        condition_iter = tqdm(self.conditions, desc="Grounding Conditions", unit="condition", unit_scale=True)

        for condition in condition_iter:
            with logging_redirect_tqdm():
                trial = self.curie_to_trial[condition.origin]
                conditions = list(self.conditions_grounder(condition, namespaces=self.condition_namespaces,
                                                           trial_title=trial.title))
                trial.conditions.extend(conditions)

    def process_interventions(self):
        intervention_iter = tqdm(self.interventions, desc="Grounding Interventions", unit="intervention",
                                 unit_scale=True)

        for intervention in intervention_iter:
            with logging_redirect_tqdm():
                trial = self.curie_to_trial[intervention.origin]
                interventions = list(self.interventions_grounder(
                    intervention,
                    namespaces=self.intervention_namespaces,
                    trial_title=self.curie_to_trial[intervention.origin].title
                ))

                trial.interventions.extend(interventions)

    def create_edges(self):
        for trial in tqdm(self.trials, desc="Generating edges from trial", unit='trial'):
            self.edges.extend([
                Edge(condition.curie, trial.curie, 'has_condition', self.config.registry)
                for condition in trial.conditions if condition
            ])

            self.edges.extend([
                Edge(intervention.curie, trial.curie, 'has_intervention', self.config.registry)
                for intervention in trial.interventions if intervention
            ])

    def save_trial_data(self, path: Path, sample_path: Optional[Path] = None):
        data = [transform.flatten_trial_data(trial) for trial in self.trials]

        headers = [
            ':CURIE',
            ':TITLE',
            ':TYPE',
            ':DESIGN',
            ':CONDITIONS',
            ':INTERVENTIONS',
            ':PRIMARY_OUTCOME',
            ':SECONDARY_OUTCOME',
            ':SECONDARY_IDS',
            ':SOURCE_REGISTRY'
            ]

        store.save_data_as_flatfile(
            data,
            path=path,
            headers=headers,
            sample_path=sample_path,
            num_samples=self.config.num_sample_entries
        )

    def save_bioentities(self, path: Path, sample_path: Optional[Path] = None):
        entities = []
        for trial in self.trials:
            entities.extend([condition for condition in trial.conditions])
            entities.extend([intervention for intervention in trial.interventions])

        entities = list(set(entities))
        entities = [transform.flatten_bioentity(entity) for entity in entities if entity]
        store.save_data_as_flatfile(
            entities,
            path=path,
            headers=[':CURIE', ':TERM', ':SOURCE_REGISTRY'],
            sample_path=sample_path,
            num_samples=self.config.num_sample_entries
        )

    def save_edges(self, path: Path, sample_path: Optional[Path] = None):
        edges = [transform.flatten_edge(edge) for edge in self.edges]

        store.save_data_as_flatfile(
            edges,
            path=path,
            headers=[':FROM', ':TO', ':REL_TYPE', ':REL_CURIE', ':SOURCE_REGISTRY'],
            sample_path=sample_path,
            num_samples=self.config.num_sample_entries
        )

    def save_data(self):
        save_samples = bool(self.config.store_samples)

        if not self.config.sample_dir.is_dir():
            self.config.sample_dir.mkdir()

        # save processed trial data to compressed tsv
        logger.info(f'Serializing and storing processed trial data to {self.config.trials_path}')
        self.save_trial_data(
            self.config.trials_path,
            sample_path=self.config.trials_sample_path if save_samples else None
        )

        # save processed bioentity data to compressed tsv
        logger.info(f'Serializing and storing grounded bioentities to {self.config.bio_entities_path}')
        self.save_bioentities(
            self.config.bio_entities_path,
            sample_path=self.config.bio_entities_sample_path if save_samples else None
        )

        # save edges to compressed tsv
        logger.info(f'Serializing and storing edges to {self.config.edges_path}')
        self.save_edges(
            self.config.edges_path,
            sample_path=self.config.edges_sample_path if save_samples else None
        )
