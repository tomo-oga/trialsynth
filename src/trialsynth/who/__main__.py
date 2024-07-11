from .fetch import Fetcher
from .store import Storer
from .config import Config
from .transform import Transformer


def main():
    config = Config()
    fetch = Fetcher(config=config)
    fetch.get_api_data()

if __name__ == '__main__':
    main()
