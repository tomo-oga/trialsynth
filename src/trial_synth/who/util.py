import re

from lxml import etree
import pandas as pd
from tqdm import tqdm

import bioregistry


PREFIXES = {
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


def get_patterns() -> dict:
    """
    Get compiled regular expression patterns for the prefixes
    """
    rv = {}
    for k, v in PREFIXES.items():
        if not v:
            continue
        pattern = bioregistry.get_pattern(v)
        if not pattern:
            tqdm.write(f"missing pattern for {v}")
            continue
        rv[k] = re.compile(pattern)
    return rv


PATTERNS = get_patterns()


def findtext(trial: etree.Element, k: str) -> str:
    """
    Find the text of a child element of a trial

    Parameters
    ----------
    trial: etree.Element
        The trial element

    k: str
        The key of the child element

    Returns
    -------
    str
        The text of the child element
    """
    if v := trial.find(k):
        if v.text:
            return v.text.replace("<br>", "\n").strip()
    return ""


def findlist(el: etree.Element, k: str) -> list:
    """
    Find a list of values from an element joined by semicolons

    Parameters
    ----------
    el: etree.Element
        The element to search

    k: str
        The key of the child element

    Returns
    -------
    list
        The list of values
    """
    v = findtext(el, k)
    if v:
        return sorted(x for x in {x.strip() for x in v.split(";")} if x)
    return []


def matches_pattern(s: str) -> str | None:
    """
    Matches a string to a pattern and returns the prefix

    Parameters
    ----------
    s: str
        The string to match

    Returns
    -------
    str | None
        The prefix

    """
    for prefix, pattern in PATTERNS.items():
        if pattern.match(s):
            return PREFIXES[prefix]


def transform_mappings(s: str) -> list[str] | None:
    """
    Transforms a string of mappings into a list of CURIEs

    Parameters
    ----------
    s: str
        The string of mappings

    Returns
    -------
    list[str] | None
        The list of CURIEs
    """
    if pd.isna(s) or not s:
        return None
    curies = []
    for x in s.split(";"):
        x = x.strip()
        if x.lower() in {"nil", "nil known", "none"}:
            continue
        prefix = matches_pattern(x)
        if not prefix:
            continue
        curies.append(bioregistry.curie_to_str(prefix, x))
    if not curies:
        return None
    return curies
