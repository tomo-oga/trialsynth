import click

from ..base.config import Config
from ..base.models import BioEntity
from ..base.process import Processor
from ..base.ground import PreProcessor
from ..base.ground import intervention_namespaces, condition_namespaces

from .fetch import WhoFetcher


def _preprocess_intervention(intervention: BioEntity) -> BioEntity:
    try:
        *_, intervention_term = intervention.term.split(':')
    except Exception:
        intervention_term = intervention.term

    intervention.term = intervention_term
    return intervention


def preprocess_intervention() -> PreProcessor:
    return _preprocess_intervention


@click.command()
@click.option('-r', '--reload', default=False)
@click.option('-s', '--store-samples', default=False)
@click.option('-v', '--validate', default=True)
def main(reload: bool, store_samples: bool, validate: bool):
    config = Config(registry='who')
    processor = Processor(
        config=config,
        fetcher=WhoFetcher(config),
        condition_preprocessor=preprocess_intervention(),
        condition_namespaces=condition_namespaces,
        intervention_namespaces=intervention_namespaces,
        reload_api_data=reload,
        store_samples=store_samples,
        validate=validate
    )

    processor.run()

if __name__ == '__main__':
        main()
