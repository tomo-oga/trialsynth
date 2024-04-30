from pydantic import BaseModel


class WHOTrial(BaseModel):
    curie: str
    name: str
    study_type: str
    study_design: list
    countries: list
    conditions: list
    intervention: list
    primary_outcome: str
    secondary_outcome: str
    mappings: str
