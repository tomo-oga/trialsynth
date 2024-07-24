import logging

import click

from ..base.config import Config
from ..base.process import Processor
from ..base.ground import condition_namespaces, intervention_namespaces

from .fetch import Fetcher

logger = logging.getLogger(__name__)


@click.command()
@click.option('-r', '--reload', default=False)
@click.option('-s', '--store-samples', default=False)
@click.option('-v', '--validate', default=True)
def main(reload: bool, store_samples: bool, validate: bool):
    config = Config(registry='clinicaltrials')
    processor = Processor(
        config=config,
        fetcher=Fetcher(config),
        condition_namespaces=condition_namespaces,
        intervention_namespaces=intervention_namespaces,
        reload_api_data=reload,
        validate=validate,
        store_samples=store_samples
    )

    processor.run()


if __name__ == "__main__":
    main()
