from typing import Tuple, Iterator

import pandas as pd
import copy

from .models import Trial, BioEntity

from .config import BaseConfig

import gilda

def or_na(x):
    """Return None if x is NaN, otherwise return x"""
    return None if pd.isna(x) else x


def is_na(x):
    """Check if a value is NaN or None."""
    return True if pd.isna(x) or not x else False


class BaseTransformer:
    def __init__(self, config: BaseConfig):
        self.config = config

    def flatten_trial_data(self, trial: Trial) -> Tuple:
        """Flattens trial data into a tuple of strings.

        Parameters
        ----------
        trial: Trial
            The trial to transform

        Returns
        -------
        transformed_data: Tuple
            A tuple of the transformed data. In order of title, type, design, conditions, interventions,
            primary_outcome, secondary_outcome, secondary_ids.

        """
        return (
            self.transform_title(trial),
            self.transform_type(trial),
            self.transform_design(trial),
            self.transform_conditions(trial),
            self.transform_interventions(trial),
            self.transform_primary_outcome(trial),
            self.transform_secondary_outcome(trial),
            self.transform_secondary_ids(trial)
        )

    @staticmethod
    def transform_title(trial: Trial) -> str:
        trial.title = trial.title.strip()
        return trial.title

    @staticmethod
    def transform_type(trial: Trial) -> str:
        trial.type = trial.type.strip()
        return trial.type

    @staticmethod
    def transform_design(trial: Trial) -> str:
        if trial.design.fallback:
            trial.design = trial.design.fallback
        else:
            trial.design = (f'Purpose: {trial.design.purpose.strip() if trial.design.purpose else ""}; '
                            f'Allocation: {trial.design.allocation.strip() if trial.design.allocation else ""};'
                            f'Masking: {trial.design.masking.strip() if trial.design.masking else ""}; '
                            f'Assignment: {trial.design.assignment.strip() if trial.design.assignment else ""}')
        return trial.design

    @staticmethod
    def transform_conditions(trial: Trial) -> list[str]:
        trial.conditions = [condition.curie for condition in trial.conditions]
        return trial.conditions

    @staticmethod
    def transform_interventions(trial: Trial) -> list[str]:
        trial.interventions = [intervention.curie for intervention in trial.interventions]
        return trial.interventions

    @staticmethod
    def flatten_bioentity(entity: BioEntity):
        return entity.origin, entity.curie, entity.term

    @staticmethod
    def transform_primary_outcome(trial: Trial) -> list[str]:
        trial.primary_outcome = (f'Measure: {trial.primary_outcome.measure.strip()}; '
                                 f'Time Frame: {trial.primary_outcome.time_frame.strip()}')
        return trial.primary_outcome

    @staticmethod
    def transform_secondary_outcome(trial: Trial) -> list[str]:
        trial.secondary_outcome = (f'Measure: {trial.secondary_outcome.measure.strip()}; '
                                   f'Time Frame: {trial.secondary_outcome.time_frame.strip()}')
        return trial.secondary_outcome

    @staticmethod
    def transform_secondary_ids(trial: Trial) -> list[str]:
        trial.secondary_ids = [id.curie for id in trial.secondary_ids]
        return trial.secondary_ids
