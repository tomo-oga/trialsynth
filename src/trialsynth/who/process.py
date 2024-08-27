from ..base.process import Processor
from .config import WhoConfig
from .fetch import WhoFetcher
from .ground import WhoConditionGrounder, WhoInterventionGrounder
from .transform import WhoTransformer
from .validate import WhoValidator


class WhoProcessor(Processor):
    def __init__(
        self, reload_api_data: bool, store_samples: bool, validate: bool
    ):
        config = WhoConfig()
        super().__init__(
            config=config,
            fetcher=WhoFetcher(config),
            transformer=WhoTransformer(),
            validator=WhoValidator(),
            grounders=(
                WhoConditionGrounder(),
                WhoInterventionGrounder(),
            ),
            reload_api_data=reload_api_data,
            store_samples=store_samples,
            validate=validate,
        )