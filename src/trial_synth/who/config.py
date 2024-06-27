from dataclasses import dataclass
import os
from pathlib import Path


INTERVENTION_RELATION = os.environ.get("INTERVENTION_RELATION", "has_intervention")
INTERVENTION_CURIE = os.environ.get("INTERVENTION_CURIE", "debio:0000035")
CONDITION_RELATION = os.environ.get("CONDITION_RELATION", "has_condition")
CONDITION_CURIE = os.environ.get("CONDITION_CURIE", "debio:0000036")

HERE = Path(__file__).parent.resolve()
HOME_DIR = os.environ.get("HOME_DIRECTORY", Path.home())
PARENT_DIR_STR = os.environ.get("BASE_DIRECTORY", ".data")
DATA_DIR_STR = os.environ.get("DATA_DIRECTORY", "who-ictrp")
DATA_DIR = Path(HOME_DIR, PARENT_DIR_STR, DATA_DIR_STR)

CSV_PATH = HERE.joinpath("ICTRP-Results.csv")
CSV_COLUMN_PATH = HERE.joinpath("ictrp_headers.csv")
PARSED_PICKLE_PATH = DATA_DIR.joinpath("processed.pkl.gz")
SAMPLE_PATH = DATA_DIR.joinpath("sample.tsv")
NODES_PATH = DATA_DIR.joinpath("nodes.tsv")
EDGES_PATH = DATA_DIR.joinpath("edges.tsv.gz")
MAPPINGS_PATH = DATA_DIR.joinpath("mappings.tsv")

SOURCE_KEY = "who"

FIELDS = [
    "curie",
    "name",
    "type",
    "design",
    "countries",
    "conditions",
    "interventions",
    "primary_outcome",
    "secondary_outcome",
    "mappings"
]


@dataclass
class Config:
    """
    User-mutable properties of WHO data processing
    """

    intervention_relation = INTERVENTION_RELATION
    intervention_curie = INTERVENTION_CURIE
    condition_relation = CONDITION_RELATION
    condition_curie = CONDITION_CURIE

    current_path = HERE
    data_dir_path = DATA_DIR
    csv_path = CSV_PATH
    csv_column_path = CSV_COLUMN_PATH
    parsed_pickle_path = PARSED_PICKLE_PATH
    sample_path = SAMPLE_PATH
    nodes_path = NODES_PATH
    edges_path = EDGES_PATH
    mappings_path = MAPPINGS_PATH

    source_key = SOURCE_KEY
