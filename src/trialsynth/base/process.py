import logging
from typing import Iterator, Tuple, Callable, Dict, Optional

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from .config import BaseConfig
from .fetch import BaseFetcher
from .models import Trial, Edge, Node, BioEntity
from .store import BaseStorer
from .transform import BaseTransformer
from .util import Grounder

import gilda

logger = logging.getLogger(__name__)


class BaseProcessor:
    """Processes registry data using Config, Fetcher, Storer, and Transformer objects.

    Attributes
    ----------
    config : Config
        User-mutable properties of registry data processing
    fetcher : Fetcher
        Fetches registry data from the REST API or a saved file
    storer : Storer
        Stores processed data to disk
    transformer : Transformer
        Transforms raw data into nodes and edges for a graph database
    node_iterator : Callable
        Method to generate nodes from the transformed data

    edges : set
        Edges between nodes

    Parameters
    ----------
    config : Config
        User-mutable properties of Clinicaltrials.gov data processing
    fetcher : Fetcher
        Fetches Clinicaltrials.gov data from the REST API or a saved file
    storer : Storer
        Stores processed data to disk
    transformer : Transformer
        Transforms raw data into nodes and edges for a graph database
    """

    def __init__(
            self,
            config: BaseConfig,
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

        self.transformer = BaseTransformer(self.config)
        self.storer = BaseStorer(self.config)

        self.conditions: list[BioEntity] = []
        self.interventions: list[BioEntity] = []

        self.nodes: list[Node] = []
        self.edges: list[Node] = []

    def run(self):
        self.fetcher.get_api_data()
        self.trials = self.fetcher.raw_data
        self.get_bioentities()
        self.process_bioentities()

    def save_data(self):
        data = [self.transformer.flatten_trial_data(trial) for trial in self.trials]
        self.storer.save_trial_data(data)

    def get_bioentities(self):
        iterated_trials = set()

        for trial in tqdm(self.trials, desc='Pulling BioEntities from Trials', total=len(self.trials), unit='trial'):

            # should be refactored later to accept various connection types. i.e. criteria
            self.conditions.extend([condition for condition in trial.conditions])
            self.interventions.extend([intervention for intervention in trial.interventions])

            if trial.curie not in iterated_trials:
                iterated_trials.add(trial.curie)
                self.curie_to_trial[trial.curie] = trial

    def process_bioentities(self):
        logger.info('Warming up grounder...')
        for _ in range(5):
            gilda.ground("stuff")
        logger.info('Done.')
        self.process_conditions()
        self.process_interventions()

    def process_conditions(self):
        grounded_conditions = []

        condition_iter = tqdm(self.conditions, desc="Grounding Conditions", unit="condition", unit_scale=True)

        for condition in condition_iter:
            with logging_redirect_tqdm():
                grounded_conditions.extend(self.conditions_grounder(condition, namespaces=self.condition_namespaces,
                                                                    trial_title=self.curie_to_trial[condition.origin].title))
        self.conditions = grounded_conditions

    def process_interventions(self):
        grounded_interventions = []

        intervention_iter = tqdm(self.interventions, desc="Grounding Interventions", unit="intervention", unit_scale=True)

        for intervention in intervention_iter:
            with logging_redirect_tqdm():
                grounded_interventions.extend(
                    self.interventions_grounder(intervention,
                                                namespaces=self.intervention_namespaces,
                                                trial_title=self.curie_to_trial[intervention.origin].title)
                )
        self.interventions=grounded_interventions



    def set_nodes_and_edges(self):
        logger.info("Generating nodes and edges")
        self.set_nodes()
        self.set_edges()

    def set_edges(self):
        edges = tqdm(
            self.edge_iterator(),
            desc="Edge generation",
            unit_scale=True,
            unit="edge"
        )
        for edge in edges:
            self.edges.append(edge)

    def set_nodes(self):
        nodes = tqdm(
            self.node_iterator(),
            desc="Node generation",
            unit_scale=True,
            unit="node"
        )
        for node in nodes:
            self.nodes.append(node)

    def node_iterator(self) -> Iterator:
        """Iterates over nodes in the registry data and yields them for processing."""
        curie_to_trial = {}
        yielded_nodes = set()
        for trial in self.trials:
            curie = trial.curie

            self.transformer.transform_title(trial)
            self.transformer.transform_type(trial)
            self.transformer.transform_design(trial)
            self.transformer.transform_conditions(trial)
            self.transformer.transform_interventions(trial)
            self.transformer.transform_primary_outcome(trial)
            self.transformer.transform_secondary_outcome(trial)
            self.transformer.transform_secondary_ids(trial)

            curie_to_trial[curie] = trial

            for condition in trial.conditions:
                if condition:
                    if condition not in yielded_nodes:
                        yield condition
                        yielded_nodes.add(condition)

            for intervention in trial.interventions:
                if intervention:
                    if intervention not in yielded_nodes:
                        yield intervention
                        yielded_nodes.add(intervention)

        for trial in set(self.trials):
            if trial not in yielded_nodes:
                yield trial
                yielded_nodes.add(trial)

    def edge_iterator(self) -> Iterator:
        """Iterates over edges in the registry data and yields them for processing."""
        yielded_edge = set()

        # could be abstracted later to method for handling different edge types
        for edge in self.edges:
            if edge not in yielded_edge:
                yield edge
                yielded_edge.add(edge)
