from typing import Union
import logging
from indra.ontology.standardize import standardize_name_db_refs
from indra.statements.agent import get_grounding

from bioregistry import curie_to_str

logger = logging.getLogger(__name__)


class SecondaryId:
    name_space: str
    id: str
    curie: str


class DesignInfo:
    purpose: str
    allocation: str
    masking: str
    assignment: str


class Outcome:
    measure: str
    time_frame: str


class Node:
    def __init__(self, ns: str, id: str):
        self.ns: str = ns
        self.id: str = id
        self.standardize()

        self.curie: str = curie_to_str(self.ns, self.id)

    def standardize(self):
        """Standardizes namespace and identifier"""

        standard_name, db_refs = standardize_name_db_refs({self.ns: self.id})
        db_ns, db_id = get_grounding(db_refs)
        if db_ns is not None and db_id is not None:
            self.ns = db_ns
            self.id = db_id


class Trial(Node):
    def __init__(self, ns: str, id: str, ):
        super(Node, self).__init__(ns, id)
        self.title: str = None
        self.type: str = None
        self.design: Union[DesignInfo, str] = None
        self.conditions: list[BioEntity] = list()
        self.interventions: list[BioEntity] = list()
        self.primary_outcome: Union[Outcome, str] = list()
        self.secondary_outcome: Union[Outcome, str] = list()
        self.secondary_ids: Union[list[SecondaryId], list[str]] = list()


class BioEntity(Node):
    def __init__(self, ns: str, id: str, term: str):
        super(BioEntity, self).__init__(ns, id)
        self.term = str


class Edge:
    def __init__(self, bio_ent_curie: str, trial_curie: str, rel_type: str):
        self.bio_ent_curie = bio_ent_curie
        self.trial_curie = trial_curie

        rel_type_to_curie = {
            "has_condition": "debio:0000036",
            "has_intervention": "debio:0000035"
        }
        if rel_type not in rel_type_to_curie.keys():
            logger.warning(f'Relationship type: {rel_type} not defined. Defaulting to empty string for curie')
            self.rel_type_curie = ''
        else:
            self.rel_type_curie = rel_type_to_curie[rel_type]


