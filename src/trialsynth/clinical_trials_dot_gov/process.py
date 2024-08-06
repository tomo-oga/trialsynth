from ..base.process import Processor
from .config import CTConfig
from .fetch import CTFetcher
from .ground import CTConditionGrounder, CTInterventionGrounder, CTGeneGrounder
from .transform import CTTransformer
from .validate import CTValidator


class CTProcessor(Processor):
    def __init__(
        self, reload_api_data: bool, store_samples: bool, validate: bool
    ):
        super().__init__(
            config=CTConfig(),
            fetcher=CTFetcher(),
            transformer=CTTransformer(),
            validator=CTValidator(),
            grounders=(CTConditionGrounder(), CTInterventionGrounder(), CTGeneGrounder),
            reload_api_data=reload_api_data,
            store_samples=store_samples,
            validate=validate,
        )
