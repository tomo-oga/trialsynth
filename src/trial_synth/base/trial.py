from typing import Union


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
    title: str
    type: str
    design: Union[DesignInfo, str]
    conditions: list[BioEntity]
    interventions: list[BioEntity]
    primary_outcome: Union[Outcome, str]
    secondary_outcome: Union[Outcome, str]
    secondary_ids: Union[list[SecondaryId], list[str]]
