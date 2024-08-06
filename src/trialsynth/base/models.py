import logging
from typing import Optional, Union

import indra.statements.agent as agent
from bioregistry import curie_to_str
from indra.ontology.standardize import standardize_name_db_refs

logger = logging.getLogger(__name__)


class TrialSynthIdGenerator:
    def __init__(self):
        self.num = 0
    def __call__(self) -> str:
        self.num += 1
        return '{:07d}'.format(self.num)

IDGenerator = TrialSynthIdGenerator()

class SecondaryId:
    """Secondary ID for a trial

    Attributes
    ----------
    ns : str
        The secondary ID's namespace
    id : str
        The ID of the secondary ID
    """

    def __init__(self, ns: str = None, id: str = None):
        self.ns = ns
        self.id = id

    @property
    def curie(self) -> str:
        """Creates a CURIE from the namespace and ID

        Returns
        -------
        str
            The CURIE
        """
        std_name, db_ref = standardize_name_db_refs({self.ns: self.id})
        ns, id = agent.get_grounding(db_ref)
        if ns and id:
            self.ns = ns
            self.id = id

        return curie_to_str(self.ns, self.id)


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

    def __init__(
        self,
        purpose=None,
        allocation=None,
        masking=None,
        assignment=None,
        fallback: Optional[str] = None,
    ):
        self.purpose: str = purpose
        self.allocation: str = allocation
        self.masking: str = masking
        self.assignment: str = assignment
        self.fallback: str = fallback


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

    def __init__(self, measure: str = None, time_frame: str = None):
        self.measure = measure
        self.time_frame = time_frame


# types of all nodes should be standardized to a class holding enumerations in the future.


class Node:
    """Node for a trial or bioentity

    Attributes
    ----------
    ns : str
        The namespace of the node
    id : str
        The ID of the node
    labels : list[str]
        The labels of the node (default: []).
    source : Optional[str]
        The source registry of the node

    Parameters
    ----------
    ns : str
        The namespace of the node (default: None).
    id : str
        The ID of the node (default: None).
    """

    def __init__(
        self,
        source: str,
        ns: str = None,
        ns_id: str = None,
    ):
        self.ns: str = ns
        self.ns_id: str = ns_id
        self.labels: list[str] = []
        self.source: str = source

    @property
    def curie(self) -> str:
        """Creates a CURIE from the namespace and ID"""
        _, db_ref = standardize_name_db_refs({self.ns: self.ns_id})
        ns, id = agent.get_grounding(db_ref)
        if ns and id:
            self.ns = ns
            self.ns_id = id

        return curie_to_str(self.ns.lower(), self.ns_id)

    @curie.setter
    def curie(self, curie: str):
        self.ns, self.ns_id = curie.split(":")


class BioEntity(Node):
    """Holds information about a biological entity

    Attributes
    ----------
    ns: str
        The namespace of the bioentity
    id: str
        The ID of the bioentity
    source: Optional[str]
        The source registry of the bioentity
    text: str
        The free-text of the bioentity
    grounded_term: str
        The entry-term for the grounded bioentity from the given namespace
    origin: Optional[str]
        The trial CURIE that the bioentity is associated with

    Parameters
    ----------
    term: str
        The text term of the bioentity from the given namespace
    labels: list[str]
        The labels of the bioentity
    origin: str
        The trial CURIE that the bioentity is associated with
    source: Optional[str]
        The source registry of the bioentity.
    ns: Optional[str]
        The namespace of the bioentity (default: None).
    id: Optional[str]
        The ID of the bioentity (default: None).
    """

    def __init__(
        self,
        text: str,
        labels: list[str],
        origin: str,
        source: str,
        ns: Optional[str] = None,
        id: Optional[str] = None,
        grounded_term: Optional[str] = None
    ):
        super().__init__(ns=ns, ns_id=id, source=source)
        self.labels = ["bioentity"]
        self.labels.extend(labels)
        self.text: str = text
        self.origin: str = origin
        self.grounded_term = grounded_term


class Condition(BioEntity):
    """
    Represents a condition.

    Parameters
    ----------
    term: str
        The text term of the bioentity from the given namespace
    labels: list[str]
        The labels of the bioentity
    origin: str
        The trial CURIE that the bioentity is associated with
    source: Optional[str]
        The source registry of the bioentity.
    ns: Optional[str]
        The namespace of the bioentity (default: None).
    id: Optional[str]
        The ID of the bioentity (default: None).
    """

    def __init__(
        self,
        text: str,
        origin: str,
        source: str,
        labels: Optional[list[str]] = None,
        ns: Optional[str] = None,
        id: Optional[str] = None,
    ):
        super().__init__(text=text, labels=['condition'], origin=origin, source=source, ns=ns, id=id)
        if labels:
            self.labels.extend(labels)


class Intervention(BioEntity):
    def __init__(
        self,
        text: str,
        origin: str,
        source: str,
        labels: Optional[list[str]] = None,
        ns: Optional[str] = None,
        id: Optional[str] = None,
    ):
        super().__init__(text=text, labels=['intervention'], origin=origin, source=source, ns=ns, id=id)
        if labels:
            self.labels.extend(labels)



class Gene(BioEntity):
    def __init__(
        self,
        text: str,
        origin: str,
        source: str,
        labels: Optional[list[str]] = None,
        ns: Optional[str] = None,
        id: Optional[str] = None,
    ):
        super().__init__(text=text, labels=['gene'], origin=origin, source=source, ns=ns, id=id)
        if labels:
            self.labels.extend(labels)


class Criteria(Node):
    def __init__(self, source: str):
        super().__init__(source=source, ns='trialsynth', ns_id=IDGenerator())


class Trial(Node):
    """Holds information about a clinical trial

    Attributes
    ----------
    ns: str
        The namespace of the trial
    id: str
        The ID of the trial
    labels: list[str]
        The labels of the trial (default: ['clinicaltrial']).
    source: Optional[str]
        The source registry of the trial (default: None).
    title: str
        The title of the trial
    design: Union[DesignInfo, str]
        The design information of the trial
    conditions: list
        The conditions targeted in the trial
    interventions: list
        The interventions used in the trial
    primary_outcomes: Union[Outcome, str]
        The primary outcome of the trial
    secondary_outcomes: Union[Outcome, str]
        The secondary outcome of the trial
    secondary_ids: Union[list[SecondaryId], list[str]]
        The secondary IDs of the trial

    Parameters
    ----------
    ns : str
        The namespace of the trial
    id : str
        The ID of the trial
    labels : Optional[list[str]]
        The labels of the trial (default: None).
    source : Optional[str]
        The source registry of the trial (default: None).
    """

    def __init__(
        self,
        ns: str,
        id: str,
        labels: Optional[list[str]] = None,
        source: Optional[str] = None,
    ):
        super().__init__(source=source, ns=ns, ns_id=id)
        self.labels: list[str] = ["clinicaltrial"]

        if labels:
            self.labels.extend(labels)

        self.title: Optional[str] = None
        self.design: DesignInfo = DesignInfo()
        self.entities: list[BioEntity] = []
        self.primary_outcomes: list[Union[Outcome, str]] = []
        self.secondary_outcomes: list[Union[Outcome, str]] = []
        self.secondary_ids: list[SecondaryId] = []

    @property
    def conditions(self) -> list[Condition]:
        return [entity for entity in self.entities if isinstance(entity, Condition)]

    @property
    def interventions(self) -> list[Intervention]:
        return [entity for entity in self.entities if isinstance(entity, Intervention)]

    @property
    def genes(self) -> list[Gene]:
        return [entity for entity in self.entities if isinstance(entity, Gene)]


class Edge:
    """Edge between a trial and a bioentity

    Attributes
    ----------
    trial: Trial
        The trial that has a relation to an entity
    entity: BioEntity
        The bioentity that is related to the trial.
    rel_type: str
        The type of relation.
    """

    def __init__(self, trial: Trial, entity: BioEntity,source: str):
        self.trial = trial
        self.entity = entity
        self.source = source

        self.rel_type = f'has_{type(entity).__name__.lower()}'
