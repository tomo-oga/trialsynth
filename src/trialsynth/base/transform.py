import pickle

import pandas as pd
from typing import Iterator, Optional
from tqdm import tqdm
from .models import Trial, Edge

from bioregistry import curie_to_str

from .config import BaseConfig

import gilda
from indra.databases import mesh_client


def or_na(x):
    """Return None if x is NaN, otherwise return x"""
    return None if pd.isna(x) else x


def is_na(x):
    """Check if a value is NaN or None."""
    return True if pd.isna(x) or not x else False





def get_correct_mesh_id(mesh_id: str, mesh_term: Optional[str] = None) -> str:
    """
    Get a correct MeSH ID from a possibly incorrect one.

    Parameters
    ----------
    mesh_id : str
        The MeSH ID to correct.
    mesh_term : Optional[str]
        The MeSH term corresponding to the MeSH ID. Default: None

    Returns
    -------
    str
        The corrected MeSH ID.
    """
    # A proxy for checking whether something is a valid MeSH term is
    # to look up its name
    name = mesh_client.get_mesh_name(mesh_id, offline=True)
    if name:
        return mesh_id
    # A common issue is with zero padding, where 9 digits are used
    # instead of the correct 6, and we can remove the extra zeros
    # to get a valid ID
    else:
        short_id = mesh_id[0] + mesh_id[4:]
        name = mesh_client.get_mesh_name(short_id, offline=True)
        if name:
            return short_id
    # Another pattern is one where the MeSH ID is simply invalid but the
    # corresponding MeSH term allows us to get a valid ID via reverse
    # ID lookup - done here as grounding just to not have to assume
    # perfect / up to date naming conventions in the source data.
    if mesh_term:
        matches = gilda.ground(mesh_term, namespaces=["MESH"])
        if len(matches) == 1:
            for k, v in matches[0].get_groundings():
                if k == "MESH":
                    return v
    return None


class BaseTransformer:
    def __init__(self, config: BaseConfig):
        self.config = config
        self.trials: list[Trial]
        self.df = pd.DataFrame()

        self.has_condition_trial_curie: list[str] = []
        self.has_intervention_trial_curie: list[str] = []
        self.has_condition: list[str] = []
        self.has_intervention: list[str] = []

    @staticmethod
    def transform_title(trial: Trial):
        trial.title = trial.title.strip()

    @staticmethod
    def transform_type(trial: Trial) -> str:
        trial.study_type = trial.study_type.strip()

    @staticmethod
    def transform_design(trial: Trial):
        trial.design = (f'Purpose: {trial.design.purpose.strip()}; Allocation: {trial.design.allocation.strip()};'
                        f'Masking: {trial.design.masking.strip()}; Assignment: {trial.design.assignment.strip()}')

    @staticmethod
    def transform_conditions(trial: Trial):
        # use bio registry or something to standardize curies
        trial.conditions = [condition.curie for condition in trial.conditions]

    @staticmethod
    def transform_interventions(trial: Trial):
        # use bio registry or something to standardize curies
        trial.interventions = [intervention.curie for intervention in trial.interventions]

    @staticmethod
    def transform_primary_outcome(trial: Trial):
        trial.primary_outcome = (f'Measure: {trial.primary_outcome.measure.strip()}; '
                                 f'Time Frame: {trial.primary_outcome.time_frame.strip()}')

    @staticmethod
    def transform_secondary_outcome(trial: Trial):
        trial.secondary_outcome = (f'Measure: {trial.secondary_outcome.measure.strip()}; '
                                   f'Time Frame: {trial.secondary_outcome.time_frame.strip()}')

    @staticmethod
    def transform_secondary_ids(trial: Trial) -> list[str]:
        trial.secondary_ids = [id.curie for id in trial.secondary_ids]

    def get_nodes(self) -> Iterator:
        curie_to_trial = {}
        yielded_nodes = set()
        for trial in tqdm(self.trials, total=len(self.trials)):
            curie = trial.curie

            self.transform_title(trial)
            self.transform_type(trial)
            self.transform_design(trial)
            self.transform_conditions(trial)
            self.transform_interventions(trial)
            self.transform_primary_outcome(trial)
            self.transform_secondary_outcome(trial)
            self.transform_secondary_ids(trial)

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

    def get_edges(self):
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
