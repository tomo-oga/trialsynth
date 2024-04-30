from dataclasses import dataclass
import os
from pathlib import Path


INTERVENTION_RELATION = os.environ.get("INTERVENTION_RELATION", "has_intervention")
INTERVENTION_CURIE = os.environ.get("INTERVENTION_CURIE", "debio:0000035")
CONDITION_RELATION = os.environ.get("CONDITION_RELATION", "has_condition")
CONDITION_CURIE = os.environ.get("CONDITION_CURIE", "debio:0000036")

HERE = Path(__file__).parent.resolve()
XML_PATH = HERE.joinpath("ICTRP-Results.xml.gz")
PARSED_PICKLE_PATH = HERE.joinpath("processed.pkl.gz")
SAMPLE_PATH = HERE.joinpath("sample.tsv")
NODES_PATH = HERE.joinpath("nodes.tsv")
EDGES_PATH = HERE.joinpath("edges.tsv.gz")
MAPPINGS_PATH = HERE.joinpath("mappings.tsv")

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
    xml_path = XML_PATH
    parsed_pickle_path = PARSED_PICKLE_PATH
    sample_path = SAMPLE_PATH
    nodes_path = NODES_PATH
    edges_path = EDGES_PATH
    mappings_path = MAPPINGS_PATH

    source_key = SOURCE_KEY
