"""Clinicaltrials.gov processor configuration"""

from dataclasses import dataclass
import logging
import os
from pathlib import Path


LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO")

PROCESSOR_NAME = os.environ.get(
    "CLINICAL_TRIALS_DOT_GOV_PROCESSOR_NAME",
    "clinicaltrials"
)
API_URL = os.environ.get(
    "CLINICAL_TRIALS_DOT_GOV_API_URL",
    "https://clinicaltrials.gov/api/v2/studies"
)

HOME_DIR = os.environ.get("HOME_DIRECTORY", Path.home())
PARENT_DIR_STR = os.environ.get("BASE_DIRECTORY", ".data")
DATA_DIR_STR = os.environ.get("DATA_DIRECTORY", "clinicaltrials")
DATA_DIR = Path(HOME_DIR, PARENT_DIR_STR, DATA_DIR_STR)
UNPROCESSED_FILE_PATH_STR = os.environ.get(
    "CLINICAL_TRIALS_RAW_DATA",
    "clinical_trials.tsv.gz"
)
NODES_FILE_NAME_STR = os.environ.get("NODES_FILE", "nodes.tsv.gz")
NODES_INDRA_FILE_NAME_STR = os.environ.get("NODES_INDRA_FILE", "nodes.pkl")
EDGES_FILE_NAME_STR = os.environ.get("EDGES_FILE", "edges.tsv.gz")

FIELDS = [
    "NCTId",
    "BriefTitle",
    "Condition",
    "ConditionMeshTerm",
    "ConditionMeshId",
    "InterventionName",
    "InterventionType",
    "InterventionMeshTerm",
    "InterventionMeshId",
    "StudyType",
    "DesignAllocation",
    "OverallStatus",
    "Phase",
    "WhyStopped",
    "SecondaryIdType",
    "SecondaryId",
    "StartDate",  # Month [day], year: "November 1, 2023", "May 1984" or NaN
    "StartDateType",  # "Actual" or "Anticipated" (or NaN)
    "ReferencePMID"  # these are tagged as relevant by the author, but not necessarily about the trial
]

root = logging.getLogger()
root.setLevel(LOGGING_LEVEL)


@dataclass
class Config:
    """
    User-mutable properties of Clinicaltrials.gov data processing

    """

    name = PROCESSOR_NAME
    api_url = API_URL
    api_parameters = {
        "fields": ",".join(FIELDS),  # actually column names, not fields
        "pageSize": 1000,
        "countTotal": "true"
    }

    unprocessed_file_path = Path(DATA_DIR, UNPROCESSED_FILE_PATH_STR)
    nodes_path = Path(DATA_DIR, NODES_FILE_NAME_STR)
    nodes_indra_path = Path(DATA_DIR, NODES_INDRA_FILE_NAME_STR)
    edges_path = Path(DATA_DIR, EDGES_FILE_NAME_STR)
    node_types = ["BioEntity", "ClinicalTrial"]
