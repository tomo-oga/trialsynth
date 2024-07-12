import logging
from typing import Iterator

from tqdm import tqdm

from .config import BaseConfig
from .fetch import BaseFetcher
from .models import Trial, Edge
from .store import BaseStorer
from .transform import BaseTransformer

logger = logging.getLogger(__name__)


class BaseProcessor:
    def __init__(
            self,
            config: BaseConfig,
            fetcher: BaseFetcher,
            storer: BaseStorer,
            transformer: BaseTransformer
    ):
        self.config = config
        self.fetcher = fetcher
        self.storer = storer
        self.transformer = transformer

        self.trials: list[Trial] = []

        # edge creation lists
        self.has_condition_trial_curie: list[str] = []
        self.has_intervention_trial_curie: list[str] = []
        self.has_condition: list[str] = []
        self.has_intervention: list[str] = []

    def get_nodes(self) -> Iterator:
        curie_to_trial = {}
        yielded_nodes = set()
        for trial in tqdm(self.trials, total=len(self.trials)):
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

            for condition in self.transform_conditions(trial):
                if condition:
                    self.has_condition_trial_curie.append(trial.id)
                    self.has_condition.append(condition.curie)
                    if condition not in yielded_nodes:
                        yield condition
                        yielded_nodes.add(condition)

            for intervention in self.transform_interventions(trial):
                if intervention:
                    self.has_intervention_trial_curie.append(trial.id)
                    self.has_intervention.append(intervention.curie)
                    if intervention not in yielded_nodes:
                        yield intervention
                        yielded_nodes.add(intervention)

        for curie in set(self.has_condition_trial_curie) or set(self.has_intervention_trial_curie):
            clinical_trial = curie_to_trial[curie]
            if clinical_trial not in yielded_nodes:
                yield clinical_trial
                yielded_nodes.add(clinical_trial)

    def get_edges(self) -> Iterator:
        added = set()

        # could be abstracted later to method for handling different edge types
        for condition, trial in zip(self.has_condition, self.has_condition_trial_curie):
            if (condition, trial) in added:
                continue
            added.add((condition, trial))
            yield Edge(condition, trial, "has_condition")

        added = set()
        for intervention, trial in zip(self.has_intervention, self.has_intervention_trial_curie):
            if (intervention, trial) in added:
                continue
            added.add((intervention, trial))
            yield Edge(intervention, trial, "has_intervention")


