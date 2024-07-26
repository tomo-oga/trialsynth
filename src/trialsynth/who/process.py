from .ground import WhoConditionGrounder, WhoInterventionGrounder
from.transform import WhoTransformer
from .fetch import WhoFetcher
from .config import WhoConfig

from ..base.process import Processor


class WhoProcessor(Processor):
    def __init__(self, reload_api_data: bool, store_samples: bool, validate: bool):
        super().__init__(
            config=WhoConfig(),
            fetcher=WhoFetcher(WhoConfig()),
            transformer=WhoTransformer(),
            condition_grounder=WhoConditionGrounder(),
            intervention_grounder=WhoInterventionGrounder(),
            reload_api_data=reload_api_data,
            store_samples=store_samples,
            validate=validate
       )