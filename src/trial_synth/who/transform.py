import logging

from lxml import etree
import pandas as pd
from tqdm import tqdm
from pathlib import Path

import bioregistry

from .config import FIELDS, CSV_COLUMN_PATH
from .trial_model import WHOTrial
from .util import PREFIXES, makelist, transform_mappings, make_str
from .validate import is_valid

logger = logging.getLogger(__name__)


def transform_csv_data(path: Path) -> pd.DataFrame:
    """Transforms WHO data from CSV into a DataFrame

    Parameters
    ----------
    path : Path
        CSV data file path
    
    Returns
    -------
    pd.DataFrame
        Transformed WHO data
    """
    logger.info("Reading CSV WHO data into DataFrame")
    df = pd.read_csv(path)
    df.columns = list(pd.read_csv(CSV_COLUMN_PATH))
    logger.info("Transforming data...")
    rows = []
    for _, trial in tqdm(df.iterrows(), unit_scale=True, unit="trial"):
        trial_id = trial['TrialID']

        for p, prefix in PREFIXES.items():
            if trial_id.startswith(p) or trial_id.startswith(p.lower()):
                break
        else:
            msg = f"could not identify {trial_id}"
            raise ValueError(msg)

        if trial_id.startswith("EUCTR"):
            trial_id = trial_id.removeprefix("EUCTR")
            trial_id = "-".join(trial_id.split("-")[:3])

        trial_id = trial_id.removeprefix("JPRN-").removeprefix("CTIS").removeprefix("PER-")

        if not is_valid(p, trial_id):
            tqdm.write(f"Failed validation: {trial_id}")

        who_trial = WHOTrial(
            curie=bioregistry.curie_to_str(prefix, trial_id),
            name=make_str(trial["public_title"]),
            study_type=make_str(trial["study_type"]),
            study_design=makelist(trial["study_design"]),
            countries=makelist(trial["Countries"], '.'),
            conditions=makelist(trial["Conditions"], ";"),
            intervention=makelist(trial["Interventions"], ";"),
            primary_outcome=make_str(trial["Primary_Outcome"]),
            secondary_outcome=make_str(trial["Secondary_Outcomes"]),
            mappings=make_str(trial["SecondaryIDs"])
        )

        rows.append(
            (
                who_trial.curie,
                who_trial.name,
                who_trial.study_type,
                who_trial.study_design,
                who_trial.countries,
                who_trial.conditions,
                who_trial.intervention,
                who_trial.primary_outcome,
                who_trial.secondary_outcome,
                who_trial.mappings,
            )
        )
    df = pd.DataFrame(
        rows,
        columns=FIELDS
    ).sort_values("curie")

    df["mappings"] = df.mappings.map(transform_mappings)
    df["name"] = df["name"].str.strip()
    for key in ["interventions", "conditions"]:
        df[key] = df[key].map(lambda l: [x.strip() for x in l], na_action="ignore")
    return df