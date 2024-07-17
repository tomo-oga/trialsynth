import logging
from typing import Iterator, Tuple, Callable

import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from .config import BaseConfig
from .fetch import BaseFetcher
from .models import Trial, Edge, Node
from .store import BaseStorer
from .transform import BaseTransformer

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
            conditions_grounder: Callable,
            interventions_grounder: Callable
    ):
        self.config = config
        self.fetcher = fetcher

        self.trials: list[Trial] = []

        self.curie_to_trial_info: dict = {}

        self.conditions_grounder: Callable = conditions_grounder
        self.interventions_grounder: Callable = interventions_grounder

        self.transformer = BaseTransformer(self.config)
        self.storer = BaseStorer(self.config)

        self.conditions: pd.DataFrame = None
        self.interventions: pd.DataFrame = None

        self.nodes: list[Node] = []
        self.edges: list[Node] = []

    def load_data(self):
        self.fetcher.get_api_data()
        self.trials = self.fetcher.raw_data
        self.process_data()

    def process_data(self):
        conditions = []
        interventions = []
        iterated_trials = set()

        for trial in tqdm(self.trials, desc='Processing trial data', total=len(self.trials), unit='trial'):

            # should be refactored later to accept various connection types. i.e. criteria
            conditions.extend([(trial.curie, trial.title, condition.term, condition.curie) for condition in trial.conditions])
            interventions.extend([(trial.curie, trial.title, intervention.term, intervention.curie) for intervention in trial.interventions])

            if trial.curie not in iterated_trials:
                iterated_trials.add(trial.curie)
                self.curie_to_trial_info[trial.curie] = self.transformer.transform_trial_data(trial)

        conditions = pd.DataFrame(conditions, columns=['trial_id', 'trial_title', 'term', 'curie'])
        interventions = pd.DataFrame(interventions, columns=['trial_id', 'trial_title', 'term', 'curie'])

        tqdm.pandas(desc="Grounding conditions", unit='condition', unit_scale=True)

        name_spaces = ["MESH", "doid", "mondo", "go"]

        iter_conditions = tqdm(conditions.iterrows(), total=len(conditions), desc='Grounding conditions', unit='condition', unit_scale=True)

        for _, condition in iter_conditions:
            with logging_redirect_tqdm():
                grounded = gilda.ground(condition['term'], namespaces=name_spaces, context=condition['trial_title'])
                if len(grounded) == 0:
                    annotations=gilda.annotate(condition['term'], namespaces=name_spaces, context_text=condition['trial_title'])
                    if len(annotations) == 0:
                        condition['curie'] = condition['curie']

                    for ix, (term, match, *_) in enumerate(annotations):
                        new_condition = condition.copy()
                        new_condition['term'] = term
                        new_condition['curie'] = match.term.get_curie()
                        if ix == 0:
                            condition.update(new_condition)
                        else:
                            conditions.loc[len(conditions)] = new_condition
                else:
                    condition['curie'] = grounded[0].term.get_curie()
        tqdm.pandas(desc="Grounding interventions")
        interventions['curie'] = interventions.progress_apply(self.interventions_grounder, axis=1)

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
