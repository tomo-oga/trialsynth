from ..base.models import Trial, BioEntity, DesignInfo
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
                for _text, matches, _, _ in annotations:
                    condition = BioEntity(matches[0].term.ns,
                                          matches[0].term.id, _text)
                    condition.standardize(name_spaces)
                    if not condition.is_standardized:
                        continue
                    condition.create_curie()
                    conditions.append(condition)
        trial.conditions = conditions

    @staticmethod
    def transform_interventions(trial: Trial):
        interventions: list[BioEntity] = []
        for intervention in trial.interventions:
            with logging_redirect_tqdm():
                if intervention.term == 'NULL':
                    continue
                try:
                    *intervention_type, intervention = intervention.term.split(':')
                except Exception:
                    continue
                # first try grounding intervention with type context
                int_ground = gilda.ground(intervention.strip(), context=' '.join(intervention_type))

                # if the grounding doesn't work try ner
                if int_ground is None:
                    annotations = gilda.annotate(intervention.term)
                    for _text, match, _, _ in annotations:
                        intervention = BioEntity(match.term.ns, match.term.id, _text)
                        intervention.standardize()
                        intervention.create_curie()
                        interventions.append(intervention)
        trial.interventions = interventions

    @staticmethod
    def transform_design(trial: Trial):
        transformed_design = DesignInfo()

        def _clean_design(design: str, type: str) -> str:
            if design.lower().startswith(type+':'):
                return design.removeprefix(type).strip()
            return ''

        for design in trial.design:
            transformed_design.allocation = _clean_design(design, "allocation")
            transformed_design.purpose = _clean_design(design, "purpose")
            transformed_design.masking = _clean_design(design, "masking")
            transformed_design.assignment = _clean_design(design, "assignment")

        trial.design = transformed_design


