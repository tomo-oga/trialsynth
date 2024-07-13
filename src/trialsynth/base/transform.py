import pandas as pd
from .models import Trial

from .config import BaseConfig

def or_na(x):
    """Return None if x is NaN, otherwise return x"""
    return None if pd.isna(x) else x


def is_na(x):
    """Check if a value is NaN or None."""
    return True if pd.isna(x) or not x else False


class BaseTransformer:
    def __init__(self, config: BaseConfig):
        self.config = config
        self.trials: list[Trial]

    @staticmethod
    def transform_title(trial: Trial):
        trial.title = trial.title.strip()

    @staticmethod
    def transform_type(trial: Trial) -> str:
        trial.type = trial.type.strip()

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
