import pandas as pd
from typing import Iterator, Optional
from tqdm import tqdm

from addict import Dict

from .rest_api_response_models import BaseTrial
from .config import FIELDS

import gilda
from indra.databases import mesh_client


def or_na(x):
    """Return None if x is NaN, otherwise return x"""
    return None if pd.isna(x) else x


def is_na(x):
    """Check if a value is NaN or None."""
    return True if pd.isna(x) or not x else False


def get_correct_mesh_id(mesh_id: str, mesh_term: Optional[str] = None) -> str:
    """
    Get a correct MeSH ID from a possibly incorrect one.

    Parameters
    ----------
    mesh_id : str
        The MeSH ID to correct.
    mesh_term : Optional[str]
        The MeSH term corresponding to the MeSH ID. Default: None

    Returns
    -------
    str
        The corrected MeSH ID.
    """
    # A proxy for checking whether something is a valid MeSH term is
    # to look up its name
    name = mesh_client.get_mesh_name(mesh_id, offline=True)
    if name:
        return mesh_id
    # A common issue is with zero padding, where 9 digits are used
    # instead of the correct 6, and we can remove the extra zeros
    # to get a valid ID
    else:
        short_id = mesh_id[0] + mesh_id[4:]
        name = mesh_client.get_mesh_name(short_id, offline=True)
        if name:
            return short_id
    # Another pattern is one where the MeSH ID is simply invalid but the
    # corresponding MeSH term allows us to get a valid ID via reverse
    # ID lookup - done here as grounding just to not have to assume
    # perfect / up to date naming conventions in the source data.
    if mesh_term:
        matches = gilda.ground(mesh_term, namespaces=["MESH"])
        if len(matches) == 1:
            for k, v in matches[0].get_groundings():
                if k == "MESH":
                    return v
    return None


class BaseTransformer:
    def __init__(self):

        # maps trialsynth headers to registry specific headers
        self.norm_column_headers = Dict()

        self.df = pd.DataFrame()

    def format_raw_data(self):
        self.df = self.df[[header for header in self.norm_column_headers.values()]]
        self.df.columns = [self.get_header(column) for column in self.df.columns]

    def get_header(self, header: str):
        reg_header = self.norm_column_headers[header]

        if not reg_header:
            raise NotImplementedError("Must provide norm_headers in subclass")

        return reg_header

    def get_nodes(self) -> Iterator:

        id_to_data = {}
        yielded_nodes = set()
        for _, row in tqdm(self.df.iterrows(), total=len(self.df)):
            id_to_data[row[FIELDS.id]] = {
                FIELDS.type: or_na(row[FIELDS.type]),
                FIELDS.conditions: self._transform_conditions(),
                FIELDS.interventions: self._transform_interventions(),
            }