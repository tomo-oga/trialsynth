from ..base.models import Trial, BioEntity
from ..base.transform import BaseTransformer
from .config import Config

import gilda
from tqdm.contrib.logging import logging_redirect_tqdm


class Transformer(BaseTransformer):
    def __init__(self, config: Config):
        super().__init__(config)

    @staticmethod
    def transform_conditions(trial: Trial):
        name_spaces = ["MESH", "doid", "mondo", "go"]
        conditions: list[BioEntity] = []
        for condition in trial.conditions:
            with logging_redirect_tqdm():
                # have gilda try and ground condition
                annotations = gilda.annotate(condition.term)
                for _text, match, _, _ in annotations:
                    condition = BioEntity(match.term.ns, match.term.id, _text)
                    condition.standardize(name_spaces)
                    condition.create_curie()
                    conditions.append(condition)
        trial.conditions = conditions


    @staticmethod
    def transform_interventions(trial: Trial):
        interventions: list[BioEntity] = []
        for intervention in trial.interventions:
            with logging_redirect_tqdm():
                intervention_type, intervention = intervention.term.split(':')
                # first try grounding intervention with type context
                int_ground = gilda.ground(intervention.strip(), context=intervention_type.strip())

                # if the grounding doesn't work try ner
                if int_ground is None:
                    annotations = gilda.annotate(intervention.term)
                    for _text, match, _, _ in annotations:
                        intervention = BioEntity(match.term.ns, match.term.id, _text)
                        intervention.standardize()
                        intervention.create_curie()
                        interventions.append(intervention)
        trial.append(interventions)






