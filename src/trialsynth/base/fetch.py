import logging
import requests

import gzip

from .config import BaseConfig
from .models import Trial
import pickle
logger = logging.getLogger(__name__)


class BaseFetcher:
    """Fetches data from an API and formats the response as a list of Trial objects

    Attributes
    ----------
    raw_data: list[trialsynth.base.models.Trial]
        Data fetched from an API in an intermediary format
    url: str
        The API endpoint. Default: ''
    api_parameters: dict
        The API parmaters to pass with the request. Default: {}
    config: trialsynth.base.BaseConfig


    """
    def __init__(self):
        self.raw_data: list[Trial] = list()
        self.url: str = ''
        self.api_parameters: dict = {}

        self.config: BaseConfig = None

    def get_api_data(self) -> None:
        """
        Fetches data from an API, and transforms it into a list of `Trial`s
        Parameters
        ----------
        url : str
            URL of the API endpoint
        request_parameters: dict
            Parameters to send with API request
        """
        raise NotImplementedError("Must be defined in subclass")

        self.save_raw_data()

    def save_raw_data(self):
        logger.info(f'Pickling raw trial data to {self.config.raw_data_path}')
        with gzip.open(self.config.raw_data_path, 'wb') as file:
            pickle.dump(self.raw_data, file)

    def send_request(self) -> dict:
        """ Sends a request to the API and returns the response as JSON

        Returns
        -------
        dict
            JSON response from API
        """
        try:
            response = requests.get(self.url, self.params)
            return response.json()
        except Exception:
            logger.exception(f"Error with request to {self.url} using params {self.params}")
            raise

    def load_saved_data(self) -> list[Trial]:
        logger.info(f"Loading saved data from {self.config.raw_data_path}. This may take a bit.")
        with gzip.open(self.config.raw_data_path, 'r') as file:
            self.raw_data = pickle.load(file)
