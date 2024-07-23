import click

from ..base.config import Config
from ..base.models import BioEntity
from ..base.process import Processor
from ..base.ground import condition_namespaces, PreProcessor

from .fetch import Fetcher


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
@click.option('--reload', default=False)
def main(reload: bool):
    config = Config(registry='who')
    processor = Processor(
        config=config,
        fetcher=Fetcher(config),
        condition_preprocessor=preprocess_intervention(),
        condition_namespaces=["MESH", "doid", "mondo", "go"],
        reload_api_data=reload
    )

    processor.run()

if __name__ == '__main__':
        main()
