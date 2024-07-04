from pydantic import BaseModel, Field


class SecondaryID(BaseModel):
    """Stores Secondary ID"""
    id_type: str = Field(alias="type")
    secondary_id: str = Field(alias="id")


class IDModule(BaseModel):
    """Stores curie of trial, with title and secondary ID curies"""
    id: str = Field(alias='id')
    title: str = Field(alias='title')
    secondary_ids: str = Field(alis='secondaryIDs', default=[])


class ConditionsModule(BaseModel):
    """Stores conditions of the trial"""
    conditions: list[str] = Field(default=[])


class Intervention(BaseModel):
    """Represents an intervention in a clinical trial."""
    name: str = Field(default=None)
    intervention_type: str = Field(alias="type")


class ArmsInterventionsModule(BaseModel):
    """Stores arms and interventions of the trial."""
    arms_interventions: list[Intervention] = Field(alias="interventions", default=[])


class Mesh(BaseModel):
    """Stores MeSH term and ID"""
    term: str
    mesh_id: str = Field(alias="id")
