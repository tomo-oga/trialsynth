from typing import Union, Optional
import logging
from indra.ontology.standardize import standardize_name_db_refs
from indra.statements.agent import get_grounding

from bioregistry import curie_to_str

logger = logging.getLogger(__name__)


class SecondaryId:
    name_space: str
    id: str
    curie: str

    def __init__(self, name_space: str = '', id: str = '', curie: str = ''):
        self.name_space = name_space
        self.id = id
        self.curie = curie

    def __eq__(self, other):
        if isinstance(other, SecondaryId):
            return (
                    self.name_space == other.name_space and
                    self.id == other.id and
                    self.curie == other.curie
            )
        return False

    def __hash__(self):
        return hash((self.name_space, self.id, self.curie))


class DesignInfo:

    def __init__(self, purpose=None, allocation=None, masking=None, assignment=None, fallback: str=None):
        self.purpose: str = purpose
        self.allocation: str = allocation
        self.masking: str = masking
        self.assignment: str = assignment
        self.fallback: str = fallback


    def __eq__(self, other):
        if isinstance(other, DesignInfo):
            return (
                    self.purpose == other.purpose and
                    self.allocation == other.allocation and
                    self.masking == other.masking and
                    self.assignment == other.assignment
            )
        return False

    def __hash__(self):
        return hash((self.purpose, self.allocation, self.masking, self.assignment))


class Outcome:
    def __init__(self, measure: str = '', time_frame: str = ''):
        self.measure = measure
        self.time_frame = time_frame

    def __eq__(self, other):
        if isinstance(other, Outcome):
            return self.measure == other.measure and self.time_frame == other.time_frame
        return False

    def __hash__(self):
        return hash((self.measure, self.time_frame))


class Node:
    def __init__(self, ns: str, ns_id: str):
        self.ns: str = ns
        self.id: str = ns_id
        if ns and ns_id:
            self.curie: str = curie_to_str(self.ns, self.id)
        else:
            self.curie: str = None


class Trial(Node):
    def __init__(self, ns: str, id: str):
        super().__init__(ns, id)
        self.title: str = None
        self.type: str = None
        self.design: Union[DesignInfo, str] = None
        self.conditions: list = list()
        self.interventions: list = list()
        self.primary_outcome: Union[Outcome, str] = list()
        self.secondary_outcome: Union[Outcome, str] = list()
        self.secondary_ids: Union[list[SecondaryId], list[str]] = list()

    def __eq__(self, other):
        if isinstance(other, Trial):
            return self.curie == other.curie
        return False

    def __hash__(self):
        return hash(self.curie)


class BioEntity(Node):
    def __init__(self, ns: str = '', id: str = '', term: str = '', origin: str = ''):
        super(BioEntity, self).__init__(ns, id)
        self.term = term
        self.origin = origin

    def __eq__(self, other):
        if isinstance(other, BioEntity):
            return self.curie == other.curie
        return False

    def __hash__(self):
        return hash(self.curie)


class Edge:
    def __init__(self, bio_ent_curie: str, trial_curie: str, rel_type: str):
        self.bio_ent_curie = bio_ent_curie
        self.trial_curie = trial_curie
        self.rel_type = rel_type

        rel_type_to_curie = {
            "has_condition": "debio:0000036",
            "has_intervention": "debio:0000035"
        }
        if rel_type not in rel_type_to_curie.keys():
            logger.warning(f'Relationship type: {rel_type} not defined. Defaulting to empty string for curie')
            self.rel_type_curie = ''
        else:
            self.rel_type_curie = rel_type_to_curie[rel_type]

    def __eq__(self, other):
        if isinstance(other, Edge):
            return all(
                getattr(self, attr) == getattr(other, attr)
                for attr in vars(self)
            )
        return False

    def __hash__(self):
        return hash(tuple(getattr(self, attr) for attr in vars(self)))
