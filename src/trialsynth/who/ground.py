from overrides import overrides

from ..base.ground import ConditionGrounder, InterventionGrounder
from ..base.models import BioEntity


class WhoConditionGrounder(ConditionGrounder):
    @overrides
    def preprocess(self, entity: BioEntity, *kwargs) -> BioEntity:
        return entity


class WhoInterventionGrounder(InterventionGrounder):
    @overrides
    def preprocess(self, entity: BioEntity, *kwargs) -> BioEntity:
        try:
            *_, intervention_term = entity.text.split(":")
        except Exception:
            intervention_term = entity.text

        entity.text = intervention_term
        return entity

