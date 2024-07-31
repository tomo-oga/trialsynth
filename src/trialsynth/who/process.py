from ..base.process import Processor
from .config import WhoConfig
from .fetch import WhoFetcher
from .ground import WhoConditionGrounder, WhoGeneGrounder, WhoInterventionGrounder
from .transform import WhoTransformer
from .validate import WhoValidator


class WhoProcessor(Processor):
    def __init__(
        self, reload_api_data: bool, store_samples: bool, validate: bool
    ):
        super().__init__(
            config=WhoConfig(),
            fetcher=WhoFetcher(),
            transformer=WhoTransformer(),
            validator=WhoValidator(),
            grounders=(
                WhoConditionGrounder(),
                WhoInterventionGrounder(),
                WhoGeneGrounder()
            ),
            reload_api_data=reload_api_data,
            store_samples=store_samples,
            validate=validate,
        )