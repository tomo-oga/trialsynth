import re
from typing import Optional

import bioregistry
import logging
from .ground import condition_namespaces, intervention_namespaces

logger = logging.getLogger(__name__)


ct_namespaces = {
    # -- Clinical trial registries -- #
    "ISRCTN": "isrctn",
    "ACTRN": "anzctr",
    "ANZCTR": "anzctr",  # xxx
    "NCT": "clinicaltrials",
    "ClinicalTrials.gov": "clinicaltrials",  # xx
    "DRKS": "drks",
    "German Clinical Trials Register": "drks",  # xxx
    "RBR": "rebec",
    "REBEC": "rebec",  ###
    "CRIS": "kcris",  ###
    "PACTR": "pactr",
    "TCTR": "tctr",
    "RPCEC": "rpcec",
    "EUCTR": "euclinicaltrials",
    "EU Clinical Trials Register": "euclinicaltrials",  ###
    "JPRN-jRCT": "jrct",
    "JPRN-UMIN": "uminctr",  # University hospital Medical Information Network
    "JPRN-C": "uminctr",  # new ID format starting with C
    "Clinical Trials Information System": "ctis",
    "CTIS": "ctis",  # site broken
    "LBCTR": "lctr",  # Lebanon Clinical Trials Registry
    "ITMCTR": "itmctr",  # International Traditional Medicine Clinical Trial Registry
    "IRCT": "irct",  # Iranian clinical trials - IDs don't match web page
    "KCT": "kcris",
    "CTRI": "ctri",  # Indian clinical trials - IDs don't match web page
    "ChiCTR": "chictr",  # Chinese Clinical Trial Registry - same
    "SLCTR": "slctr",  # need to change slashes to dashes to resolve
    "IFV": "rpcec",  # Cuba: Registro Público Cubano de Ensayos Clínicos
    "jRCT": "jrct",  # Japan Registry of Clinical Trials
    "PHRR": "phrr",  # Philippines trial registry
    "NL": "ictrp",  # Netherlands old registry, find at trialsearch.who.int using NLXXXX
    "PER": "repec",  # Clinical Trials Peruvian Registry
}


def get_namespaces() -> dict:
    """Get the namespaces for the clinical trial registries and bioentity ontologies"""
    entity_namespaces = list(set(condition_namespaces + intervention_namespaces))

    for ns in entity_namespaces:
        ct_namespaces[ns] = ns.lower()
    return ct_namespaces


NAMESPACES = get_namespaces()


def get_patterns() -> dict:
    """Get compiled regular expression patterns for the prefixes of the namespaces"""
    rv = {}
    for k, v in NAMESPACES.items():
        if not v:
            continue
        pattern = bioregistry.get_pattern(v)
        if not pattern:
            logger.info(f"missing pattern for {v} in bioregistry")
            continue
        rv[k] = re.compile(pattern)
    return rv


PATTERNS = get_patterns()


def make_list(s: Optional[str], delimeter: str = '.') -> list:
    """Create a list of values from an element joined by a dilemeter

    Parameters
    ----------
    s : Optional[str]
        The string to split
    delimeter : str
        The delimeter to split by

    Returns
    -------
    list
        The list of values
    """

    if s:
        s = s.removeprefix('"').removesuffix('"')
        return sorted(x for x in {x.strip() for x in s.split(delimeter)} if x)
    return []


def make_str(s: str) -> Optional[str]:
    """Return a stripped string if it is not empty

    Parameters
    ----------
    s : str
        The string to check

    Returns
    -------
    str
        The string if it is not empty
    """
    if s:
        s = s.removeprefix('"').removesuffix('"')
        return s.strip()
    return
