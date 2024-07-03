import logging

import click

from .config import CONFIG
from .fetch import Fetcher
from .store import Storer
from .transform import Transformer
from .process import Processor


logger = logging.getLogger(__name__)


@click.command()
def main():
    click.secho("Processing clincaltrials.gov data", fg="green", bold=True)
    fetcher = Fetcher(
        url=CONFIG.api_url,
        request_parameters=CONFIG.api_parameters
    )
    transformer = Transformer()
    storer = Storer(
        node_iterator=transformer.get_nodes,
        node_types=CONFIG.node_types,
        data_directory=CONFIG.data_dir,
        sample_directory=CONFIG.sample_dir
    )
    clinical_trials_processor = Processor(
        config=CONFIG,
        fetcher=fetcher,
        storer=storer,
        transformer=transformer
    )
    clinical_trials_processor.ensure_api_response_data_saved()
    clinical_trials_processor.clean_and_transform_data()
    clinical_trials_processor.set_nodes_and_edges()
    clinical_trials_processor.validate_data()
    clinical_trials_processor.save_graph_data()


if __name__ == "__main__":
    main()
