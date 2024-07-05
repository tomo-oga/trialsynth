import pandas as pd
from typing import Iterator, Optional
from tqdm import tqdm

from addict import Dict

from .config import CONFIG

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
        self.condition_curie = []
        self.condition_trial_curie = []

        self.intervention_curie = []
        self.intervention_trial_curie = []

        self.problematic_mesh_ids = []

        # maps trialsynth headers to registry specific headers
        self.norm_headers = {
            "curie: ID": None,
            ":NAME": None,
            ":TYPE": None,
            ":LABEL": None
        }

        self.df = pd.DataFrame()

    def clean_raw_data(self) -> None:
        """Clean up values from raw data"""

        raise NotImplementedError("Must be defined in subclass.")

    def get_nodes(self) -> Iterator:
        raise NotImplementedError("Must be defined in subclass")

    def get_edges(self):
        """Get edges from the DataFrame and transformed data"""
        added = set()
        for cond_curie, trial_curie in zip(
                self.condition_curie, self.condition_trial_curie
        ):
            if (cond_curie, trial_curie) in added:
                continue
            added.add((cond_curie, trial_curie))
            yield Dict(
                source_curie=cond_curie,
                target_curie=trial_curie,
                rel_type="has_condition",
                rel_curie=CONFIG.has_condition_curie,
                data={}
            )

        added = set()
        for int_curie, trial_curie in zip(
                self.intervention_curie, self.intervention_trial_curie
        ):
            if (int_curie, trial_curie) in added:
                continue
            added.add((int_curie, trial_curie))
            yield Dict(
                source_curie=int_curie,
                target_curie=trial_curie,
                rel_type="has_intervention",
                rel_id=CONFIG.has_intervention_curie,
                data={}
            )

    def get_header(self, header: str):
        reg_header = self.norm_headers[header]

        if not reg_header:
            raise NotImplementedError("Must provide norm_headers in subclass")

        return reg_header
