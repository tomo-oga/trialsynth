import logging

from lxml import etree
import pandas as pd
from tqdm import tqdm

import bioregistry

from .config import FIELDS
from .trial_model import WHOTrial
from .util import PREFIXES, findtext, findlist, transform_mappings
from .validate import is_valid


logger = logging.getLogger(__name__)


def transform_xml_data(tree: etree.Element) -> pd.DataFrame:
    """
    Transforms WHO data from XML into a DataFrame

    Parameters
    ----------
    tree : etree.Element
        Parsed XML data

    Returns
    -------
    DataFrame
        Transformed WHO data
    """
    logger.info("Transforming WHO data from XML into dataframe")
    rows = []
    for trial in tqdm(tree, unit_scale=True, unit="trial"):
        trial_id = trial.find("TrialID").text.strip()
        link = trial.find("web_address").text.strip()
        for p, prefix in PREFIXES.items():
            if trial_id.startswith(p):
                break
        else:
            msg = f"could not identify {trial_id} {link}"
            raise ValueError(msg)
        if trial_id.startswith("EUCTR"):
            trial_id = trial_id.removeprefix("EUCTR")
            trial_id = "-".join(trial_id.split("-")[:3])

        trial_id = trial_id.removeprefix("JPRN-").removeprefix("CTIS")

        if not is_valid(p, trial_id):
            tqdm.write(f"Failed validation: {trial_id}")

        trial = WHOTrial(
            curie=bioregistry.curie_to_str(prefix, trial_id),
            name=findtext(trial, "Public_title"),
            study_type=findtext(trial, "Study_type").strip().lower(),
            study_design=findlist(trial, "Study_design"),
            countries=findlist(trial, "Countries"),
            conditions=findlist(trial, "Condition"),
            intervention=findlist(trial, "Intervention"),
            primary_outcome=findtext(trial, "Primary_outcome"),
            secondary_outcome=findtext(trial, "Secondary_outcome"),
            mappings=findtext(trial, "Secondary_ID")
        )

        rows.append(
            (
                trial.curie,
                trial.name,
                trial.study_type,
                trial.study_design,
                trial.countries,
                trial.conditions,
                trial.intervention,
                trial.primary_outcome,
                trial.secondary_outcome,
                trial.mappings,
            )
        )

    df = pd.DataFrame(
        rows,
        columns=FIELDS
    ).sort_values("curie")

    df["mappings"] = df.mappings.map(transform_mappings)
    df["name"] = df["name"].strip()
    for key in ["interventions", "conditions"]:
        df[key] = df[key].map(lambda l: [x.strip() for x in l], na_action="ignore")
    return df
