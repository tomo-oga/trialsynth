from overrides import overrides

from ..base.ground import ConditionGrounder, GeneGrounder, InterventionGrounder
from ..base.models import BioEntity


class WhoConditionGrounder(ConditionGrounder):
    @overrides
    def preprocess(self, entity: BioEntity, *kwargs) -> BioEntity:
        return entity


class WhoInterventionGrounder(InterventionGrounder):
    @overrides
    def preprocess(self, entity: BioEntity, *kwargs) -> BioEntity:
        try:
<<<<<<< HEAD
            *_, intervention_term = entity.term.split(":")
=======
            *_, intervention_term = entity.text.split(":")
>>>>>>> ad9fdc8 (adding BioEntity types and linting/formatting with trunk)
        except Exception:
            intervention_term = entity.text

        entity.text = intervention_term
        return entity


class WhoCriteriaGrounder(GeneGrounder):
    @overrides
    def preprocess(self, entity: BioEntity, *kwargs) -> BioEntity:
        return entity
