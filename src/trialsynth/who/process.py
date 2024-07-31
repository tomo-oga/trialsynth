<<<<<<< HEAD
<<<<<<< HEAD
from ..base.process import Processor
from .config import WhoConfig
from .fetch import WhoFetcher
from .ground import WhoConditionGrounder, WhoInterventionGrounder
=======
from .ground import WhoConditionGrounder, WhoInterventionGrounder, WhoCriteriaGrounder
>>>>>>> bbf3f7f (structuring inclusion/exclusion criteria through gilda grounder)
=======
from ..base.process import Processor
from .config import WhoConfig
from .fetch import WhoFetcher
from .ground import (WhoConditionGrounder, WhoCriteriaGrounder,
                     WhoInterventionGrounder)
>>>>>>> ad9fdc8 (adding BioEntity types and linting/formatting with trunk)
from .transform import WhoTransformer
from .validate import WhoValidator


class WhoProcessor(Processor):
    def __init__(
        self, reload_api_data: bool, store_samples: bool, validate: bool
    ):
        super().__init__(
            config=WhoConfig(),
            fetcher=WhoFetcher(WhoConfig()),
            transformer=WhoTransformer(),
            validator=WhoValidator(),
            condition_grounder=WhoConditionGrounder(),
            intervention_grounder=WhoInterventionGrounder(),
            genes_grounder=WhoCriteriaGrounder(),
            reload_api_data=reload_api_data,
            store_samples=store_samples,
            validate=validate,
        )
