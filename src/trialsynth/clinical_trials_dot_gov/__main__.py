import click

from .process import CTProcessor


@click.command()
@click.option('-r', '--reload', default=False)
@click.option('-s', '--store-samples', default=False)
@click.option('-v', '--validate', default=True)
def main(reload: bool, store_samples: bool, validate: bool):

    CTProcessor(reload_api_data=reload, store_samples=store_samples, validate=validate).run()


if __name__ == "__main__":
    main()
