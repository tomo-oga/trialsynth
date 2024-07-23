import re
import csv
import bioregistry
import pandas as pd

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from ..base.fetch import BaseFetcher, logger
from ..base.config import Config
from ..base.models import Trial as WhoTrial
from ..base.models import BioEntity, Outcome, SecondaryId, DesignInfo

from typing import Optional


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
            tqdm.write(f"missing pattern for {v} in bioregistry")
            continue
        rv[k] = re.compile(pattern)
    return rv


PATTERNS = get_patterns()


def make_list(s: Optional[str], delimeter: str = '.') -> list:
    """Find a list of values from an element joined by semicolons

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

    if s and not pd.isna(s):
        s = s.removeprefix('"').removesuffix('"')
        return sorted(x for x in {x.strip() for x in s.split(delimeter)} if x)
    return []


def make_str(s: str):
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
    if s and not pd.isna(s):
        return s.strip()

    return ''


def matches_pattern(s: str) -> Optional[str]:
    """Matches a string to a pattern and returns the prefix if it matches.

    Parameters
    ----------
    s : str
        The string to match

    Returns
    -------
    Optional[str]
        The prefix

    """
    for prefix, pattern in PATTERNS.items():
        if pattern.match(s):
            return PREFIXES[prefix]

def is_valid(pattern: str, trial_id: str) -> bool:
    """Validates a trial ID against a pattern

    Parameters
    ----------
    pattern : str
        The pattern to validate against
    trial_id : str
        The trial ID to validate

    Returns
    -------
    bool
        Whether the trial ID is valid
    """
    if pattern in PATTERNS and not PATTERNS[pattern].match(trial_id):
        return False
    return True


class Fetcher(BaseFetcher):
    def __init__(self, config: Config):
        super().__init__(config)

    def get_api_data(self, reload: bool = False):
        trial_path = self.config.raw_data_path
        if trial_path.is_file() and not reload:
            self.load_saved_data()
            return
        path = self.config.get_data_path('ICTRP.csv')
        with open(path, 'r') as file:
            trials = [trial for trial in file]

            for trial in tqdm(
                    csv.reader(trials),
                    desc="Reading CSV WHO data",
                    total=len(trials), unit='trials'
            ):
                with logging_redirect_tqdm():
                    trial_id = trial[0].strip()
                    trial_id = trial_id.replace('\ufeff', '')
                    for p, prefix in PREFIXES.items():
                        if trial_id.startswith(p) or trial_id.startswith(p.lower()):
                            break
                    else:
                        msg = f"could not identify {trial_id}"
                        raise ValueError(msg)

                    if trial_id.startswith("EUCTR"):
                        trial_id = trial_id.removeprefix("EUCTR")
                        trial_id = "-".join(trial_id.split("-")[:3])

                        # handling inconsistencies with ChiCTR trial IDs
                    if trial_id.lower().startswith("chictr-"):
                        trial_id = "ChiCTR-" + trial_id.lower().removeprefix("chictr-").upper()

                    trial_id = trial_id.removeprefix("JPRN-").removeprefix("CTIS").removeprefix("PER-")

                    if not is_valid(p, trial_id):
                        tqdm.write(f'Failed validation: {trial_id}')


                    who_trial = WhoTrial(prefix, trial_id)

                    who_trial.title = make_str(trial[3])
                    who_trial.type = make_str(trial[18])

                    design_list = [design.strip() for design in make_list(trial[19], '.')]
                    design_dict = {}

                    try:
                        for design_attr in design_list:
                            key, value = design_attr.split(':', 1)
                            design_dict[key.strip().lower()] = value.strip()
                            who_trial.design = DesignInfo(
                                allocation=design_dict.get('allocation'),
                                assignment=design_dict.get('intervention model'),
                                masking=design_dict.get('masking'),
                                purpose=design_dict.get('primary purpose')
                            )
                    except Exception:
                        logger.debug(f"Error in design attribute for curie: {who_trial.curie} using fallback")
                        pass

                    if who_trial.design is None:
                        who_trial.design = DesignInfo(fallback=trial[19])

                    who_trial.conditions = [
                        BioEntity(
                            term=condition,
                            origin=who_trial.curie,
                            type='Condition',
                            source=self.config.registry
                        )
                        for condition in make_list(trial[29], ';')
                    ]
                    who_trial.interventions = [
                        BioEntity(
                            term=intervention,
                            origin=who_trial.curie,
                            type='Intervention',
                            source=self.config.registry
                        )
                        for intervention in make_list(trial[30], ';') if intervention != 'NULL'
                    ]
                    who_trial.primary_outcomes = [Outcome(measure=make_str(trial[36]))]
                    who_trial.secondary_outcomes = [Outcome(measure=make_str(trial[37]))]
                    who_trial.secondary_ids = [SecondaryId(curie=curie) for curie in make_list(trial[2], ';')]
                    who_trial.source = self.config.registry
                    self.raw_data.append(who_trial)
        self.save_raw_data()
