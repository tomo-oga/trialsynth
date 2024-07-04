import logging
from pathlib import Path

import pandas as pd
import requests
from tqdm import trange
from typing import Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)
class BaseFetcher:
    def __init__(self, url, api_parameters):
        self.raw_data = pd.DataFrame()
        self.url = url
        self.api_parameters = api_parameters
        self.total_pages = 0

    def get_api_data(self, url: str, request_parameters: dict) -> None:
        """
        Fetches data from an API
        Parameters
        ----------
        url : str
            URL of the API endpoint
        request_parameters: dict
            Parameters to send with API request
        """
        raise NotImplementedError("Must be defined in subclass.")

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
