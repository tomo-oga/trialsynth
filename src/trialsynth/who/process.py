from ..base.process import Processor
from .config import WhoConfig
from .fetch import WhoFetcher
from .transform import WhoTransformer
from .validate import WhoValidator
from .ground import WhoConditionGrounder, WhoInterventionGrounder, WhoGeneGrounder


class WhoProcessor(Processor):
    def __init__(
        self, reload_api_data: bool, store_samples: bool, validate: bool
    ):
        super().__init__(
            config=WhoConfig(),
            fetcher=WhoFetcher(),
            transformer=WhoTransformer(),
            validator=WhoValidator(),
            condition_grounder=WhoConditionGrounder(),
            intervention_grounder=WhoInterventionGrounder(),
            genes_grounder=WhoGeneGrounder(),
            reload_api_data=reload_api_data,
            store_samples=store_samples,
            validate=validate,
        )
