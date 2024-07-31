<<<<<<< HEAD
=======
import click

>>>>>>> ad9fdc8 (adding BioEntity types and linting/formatting with trunk)
from ..base.process import run_processor
from .process import WhoProcessor


@run_processor
def main(reload: bool, store_samples: bool, validate: bool):
    WhoProcessor(reload, store_samples, validate).run()


if __name__ == "__main__":
    main()
