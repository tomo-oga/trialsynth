from dataclasses import dataclass
from pathlib import Path

from .config_environ import get_config


HERE = Path(__file__).parent.resolve()
DATA_DIR = HERE.joinpath("data")
SAMPLE_DIR = DATA_DIR.joinpath("samples")
RESOURCE_DIR = HERE.joinpath("resources")

CSV_PATH = RESOURCE_DIR.joinpath("ICTRP-Results.csv")
PARSED_PICKLE_PATH = DATA_DIR.joinpath("processed.pkl.gz")
SAMPLE_PATH = SAMPLE_DIR.joinpath("sample.tsv")
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

    intervention_relation = get_config("INTERVENTION_RELATION")
    intervention_curie = get_config("INTERVENTION_CURIE")
    condition_relation = get_config("CONDITION_RELATION")
    condition_curie = get_config("CONDITION_CURIE")

    current_path = HERE
    data_dir_path = DATA_DIR
    csv_path = CSV_PATH
    parsed_pickle_path = PARSED_PICKLE_PATH
    sample_dir_path = SAMPLE_DIR
    sample_path = SAMPLE_PATH
    nodes_path = NODES_PATH
    edges_path = EDGES_PATH
    mappings_path = MAPPINGS_PATH

    source_key = SOURCE_KEY
