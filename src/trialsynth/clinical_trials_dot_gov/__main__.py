import logging

import click

from ..base.config import Config
from ..base.process import Processor
from ..base.ground import ground_entity, condition_namespaces, intervention_namespaces

from .fetch import Fetcher

logger = logging.getLogger(__name__)


@click.command()
@click.option('--reload', default=False)
def main(reload: bool):
    config = Config(registry='clinicaltrials')
    processor = Processor(
        config=config,
        fetcher=Fetcher(config),
        condition_namespaces=condition_namespaces,
        intervention_namespaces=intervention_namespaces,
        reload_api_data=reload
    )

    processor.run()


if __name__ == "__main__":
    main()
