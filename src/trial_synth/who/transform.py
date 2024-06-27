import logging

import csv
import pandas as pd
from tqdm import tqdm
from pathlib import Path

import bioregistry

from .config import FIELDS, CSV_COLUMN_PATH, Config
from .trial_model import WHOTrial
from .util import PREFIXES, makelist, transform_mappings, make_str
from .validate import is_valid
from .fetch import load_saved_pickled_data

logger = logging.getLogger(__name__)

def ensure_df(config: Config = Config()) -> pd.DataFrame:
    """
    Ensure that the DataFrame is loaded from a saved pickle file or from the CSV file

    Parameters
    ----------
    config: Config
        Configuration for WHO data processing. Default: Config()

    Returns
    -------
    pd.DataFrame
        The DataFrame of the WHO data
    """
    if config.parsed_pickle_path.is_file():
        return load_saved_pickled_data(config.parsed_pickle_path)

    df = transform_csv_data(config.csv_path)
    df.to_pickle(config.parsed_pickle_path)
    return df

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
    with open(path, mode='r') as file:
        lines = [line for line in file]
        current_line = 1

        for row in tqdm(csv.reader(lines), desc="Reading CSV WHO data", total=len(lines), unit='lines'):
            current_line += 1
    # manual progress bar as pandas does not support tqdm natively
    n_lines = sum(1 for line in open(path))
    with tqdm(total=n_lines, desc="Reading CSV WHO data", unit='lines') as pbar:
        chunks = []
        for chunk in pd.read_csv(path, chunksize=1000):
            chunks.append(chunk)
            pbar.update(len(chunk))
    df = pd.concat(chunks, ignore_index=True)
    df.columns = list(pd.read_csv(CSV_COLUMN_PATH))
    logger.info("Transforming data...")
    rows = []
    for _, trial in tqdm(df.iterrows(), unit_scale=True, unit="trial"):
        trial_id = trial['TrialID'].strip()

        for p, prefix in PREFIXES.items():
            if trial_id.startswith(p) or trial_id.startswith(p.lower()):
                break
        else:
            msg = f"could not identify {trial_id}"
            raise ValueError(msg)

        if trial_id.startswith("EUCTR"):
            trial_id = trial_id.removeprefix("EUCTR")
            trial_id = "-".join(trial_id.split("-")[:3])

        # handling incosistensies with ChiCTR trial IDs
        if trial_id.lower().startswith("chictr-"):
            trial_id = "ChiCTR-" + trial_id.lower().removeprefix("chictr-").upper()

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