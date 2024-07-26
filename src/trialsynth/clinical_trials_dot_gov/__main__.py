from .process import CTProcessor
from ..base.process import run_processor


@run_processor
def main(reload: bool, store_samples: bool, validate: bool):

    CTProcessor(reload_api_data=reload, store_samples=store_samples, validate=validate).run()


if __name__ == "__main__":
    main()
