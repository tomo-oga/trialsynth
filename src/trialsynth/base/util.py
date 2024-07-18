from typing import Optional, Callable, Iterator, Tuple
import gilda
from indra.databases import mesh_client

from src.trialsynth.base.models import BioEntity

Grounder = Callable[[BioEntity, list[str], str], Iterator[BioEntity]]

def must_override(method: Callable):
    method._must_override=True
    return method


def list_from_string(data: str, delimiter: str = ",") -> list[str]:
    return [item.strip() for item in data.split(delimiter)]


def get_correct_mesh_id(mesh_id: str, mesh_term: Optional[str] = None) -> Optional[str]:
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
    # perfect / up-to-date naming conventions in the source data.
    if mesh_term:
        matches = gilda.ground(mesh_term, namespaces=["MESH"])
        if len(matches) == 1:
            for k, v in matches[0].get_groundings():
                if k == "MESH":
                    return v
    return None
