from ..base.process import run_processor
from .process import CTProcessor


@run_processor
def main(reload: bool, store_samples: bool, validate: bool, device: str):

    CTProcessor(
        reload_api_data=reload, store_samples=store_samples, validate=validate, device=device
    ).run()


if __name__ == "__main__":
    main()
