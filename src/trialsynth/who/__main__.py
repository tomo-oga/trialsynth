import click

from .process import WhoProcessor
from ..base.process import run_processor


@run_processor
def main(reload: bool, store_samples: bool, validate: bool):
    WhoProcessor(reload, store_samples, validate).run()


if __name__ == '__main__':
    main()
