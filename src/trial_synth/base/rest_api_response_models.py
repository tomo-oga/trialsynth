from pydantic import BaseModel, Field


class Mesh(BaseModel):
    """Stores MeSH term and ID"""
    term: str
    mesh_id: str = Field(alias="id")


class ID(BaseModel):
    """Stores curie of trial, with title"""
    id: str = Field(alias='id')
    title: str = Field(alias='title')
    secondary_ids: list[str] = Field(default=[])


class SecondaryID(BaseModel):
    """Stores Secondary ID"""
    id_type: str = Field(alias="type")
    secondary_id: str = Field(alias="id")


class Conditions(BaseModel):
    """Stores conditions of the trial"""
    conditions: list[Mesh] = Field(default=[])


class Interventions(BaseModel):
    """Represents an intervention in a clinical trial."""
    interventions: list[Mesh] = Field(default=[])


class DesignInfo(BaseModel):
    design_purpose: str = Field(alias='purpose', default=None)
    design_allocation: str = Field(alias='allocation', default=None)
    design_masking: str = Field(alias='masking', default=None)
    design_assignment: str = Field(alias='assignment', default=None)


class Outcome(BaseModel):
    primary_outcome: str = Field(alias='primary')
    secondary_outcome: str = Field(alias='secondary', default=None)


class BaseTrial(BaseModel):
    id: ID
    study_type: str
    design: DesignInfo
    conditions: Conditions
    interventions: Interventions
    outcome: Outcome

