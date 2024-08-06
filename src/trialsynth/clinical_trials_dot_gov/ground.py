from overrides import overrides

from ..base.ground import ConditionGrounder, InterventionGrounder, GeneGrounder
from ..base.models import BioEntity


class CTConditionGrounder(ConditionGrounder):
    @overrides
    def preprocess(self, entity: BioEntity) -> BioEntity:
        return entity


class CTInterventionGrounder(InterventionGrounder):
    @overrides
    def preprocess(self, entity: BioEntity) -> BioEntity:
        return entity

class CTGeneGrounder(GeneGrounder):
    @overrides
    def preprocess(self, entity: BioEntity) -> BioEntity:
        return entity
