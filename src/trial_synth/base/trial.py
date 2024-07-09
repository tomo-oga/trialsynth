from typing import Union
import logging

logger = logging.getLogger(__name__)


class ClinicalTrial:
    ns: str
    id: str
    curie: str


class BioEntity:
    ns: str
    id: str
    curie: str
    term: str


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


class TrialModel:
    id: str
    db: str
    curie: str
    title: str
    type: str
    design: Union[DesignInfo, str]
    conditions: list[BioEntity]
    interventions: list[BioEntity]
    primary_outcome: Union[Outcome, str]
    secondary_outcome: Union[Outcome, str]
    secondary_ids: Union[list[SecondaryId], list[str]]


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


