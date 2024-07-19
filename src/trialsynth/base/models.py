from typing import Union, Optional
import logging

from bioregistry import curie_to_str

logger = logging.getLogger(__name__)


class SecondaryId:
    """Secondary ID for a trial

    Attributes
    ----------
    name_space : str
        The namespace of the secondary ID
    id : str
        The ID of the secondary ID
    curie : str
        The CURIE of the secondary ID
    """
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
    """Design information for a trial

    Attributes
    ----------
    purpose : str
        The purpose of the design
    allocation : str
        The allocation of the design
    masking : str
        The masking of the design
    assignment : str
        The assignment of the design
    fallback : Optional[str]
        The fallback design information, if the design information is not in the expected format

    Parameters
    ----------
    purpose : str
        The purpose of the design
    allocation : str
        The allocation of the design
    masking : str
        The masking of the design
    assignment : str
        The assignment of the design
    fallback : Optional[str]
        The fallback design information, if the design information is not in the expected format
    """
    def __init__(self, purpose=None, allocation=None, masking=None, assignment=None, fallback: Optional[str] = None):
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
    """Outcome for a trial

    Attributes
    ----------
    measure : str
        The measure of the outcome
    time_frame : str
        The time frame of the outcome

    Parameters
    ----------
    measure : str
        The measure of the outcome
    time_frame : str
        The time frame of the outcome
    """
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
    """Node for a trial or bioentity

    Attributes
    ----------
    ns : str
        The namespace of the node
    id : str
        The ID of the node
    curie : str
        The CURIE of the node
    source : Optional[str]
        The source registry of the node

    Parameters
    ----------
    ns : Optional[str]
        The namespace of the node
    ns_id : Optional[str]
        The ID of the node
    """
    def __init__(self, ns: Optional[str], ns_id: Optional[str]):
        self.ns: str = ns
        self.id: str = ns_id
        self.source: Optional[str] = None

        if ns and ns_id:
            self.curie: str = curie_to_str(self.ns, self.id)
        else:
            self.curie: str = None


class Trial(Node):
    """Holds information about a clinical trial

    Attributes
    ----------
    ns: str
        The namespace of the trial
    id: str
        The ID of the trial
    curie: str
        The CURIE of the trial
    source: Optional[str]
        The source registry of the trial
    title: str
        The title of the trial
    type: str
        The type of the trial
    design: Union[DesignInfo, str]
        The design information of the trial
    conditions: list
        The conditions targeted in the trial
    interventions: list
        The interventions used in the trial
    primary_outcome: Union[Outcome, str]
        The primary outcome of the trial
    secondary_outcome: Union[Outcome, str]
        The secondary outcome of the trial
    secondary_ids: Union[list[SecondaryId], list[str]]
        The secondary IDs of the trial

    Parameters
    ----------
    ns: str
        The namespace of the trial
    id: str
        The ID of the trial
    """
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
    """Holds information about a biological entity

    Attributes
    ----------
    ns: str
        The namespace of the bioentity
    id: str
        The ID of the bioentity
    curie: str
        The CURIE of the bioentity
    source: Optional[str]
        The source registry of the bioentity
    term: str
        The freetext term of the bioentity
    origin: Optional[str]
        The trial CURIE that the bioentity is associated with

    Parameters
    ----------
    ns: str
        The namespace of the bioentity
    id: str
        The ID of the bioentity
    term: str
        The freetext term of the bioentity
    origin: Optional[str]
        The trial CURIE that the bioentity is associated with
    source: Optional[str]
        The source registry of the bioentity
    """
    def __init__(
            self,
            ns: Optional[str] = None,
            id: Optional[str] = None,
            term: Optional[str] = None,
            origin: Optional[str] = None,
            source: Optional[str] = None,
    ):
        super(BioEntity, self).__init__(ns, id)
        self.term = term
        self.origin = origin
        self.source = source

    def __eq__(self, other):
        if isinstance(other, BioEntity):
            return self.curie == other.curie
        return False

    def __hash__(self):
        return hash(self.curie)


class Edge:
    """Edge between a trial and a bioentity

    Attributes
    ----------
    bio_ent_curie: str
        The CURIE of the bioentity
    trial_curie: str
        The CURIE of the trial
    rel_type: str
        The type of relationship between the bioentity and the trial
    rel_type_curie: str
        The CURIE of the relationship type
    source: str
        The source of the relationship
    """
    def __init__(self, bio_ent_curie: str, trial_curie: str, rel_type: str, source: str):
        self.bio_ent_curie = bio_ent_curie
        self.trial_curie = trial_curie
        self.rel_type = rel_type
        self.source = source

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
