from pydantic import BaseModel, Field



class SecondaryID(BaseModel):

    id_type: str = Field(alias="type")
    secondary_id: str = Field(alias="id")


class IDModule(BaseModel):

    nct_id: str = Field(alias="nctId")
    brief_title: str = Field(alias="briefTitle")
    secondary_ids: list[SecondaryID] = Field(alias="secondaryIds", default=[])


class ConditionsModule(BaseModel):

    conditions: list[str] = Field(default=[])


class StartDateStruct(BaseModel):

    date: str = Field(default=None)
    date_type: str = Field(alias="type", default=None)


class StatusModule(BaseModel):

    start_date_struct: StartDateStruct = Field(alias="startDateStruct", default=StartDateStruct())
    overall_status: str = Field(alias="overallStatus")
    why_stopped: str = Field(alias="whyStopped", default=None)


class DesignInfo(BaseModel):

    allocation: str = Field(alias="allocation", default=None)


class DesignModule(BaseModel):

    study_type: str = Field(alias="studyType", default=None)
    phases: list[str] = Field(alias="phases", default=[])
    design_info: DesignInfo = Field(alias="designInfo", default=DesignInfo())


class Reference(BaseModel):

    pmid: str  # these are tagged as relevant by the author, but not necessarily about the trial


class ReferencesModule(BaseModel):

    references: list[Reference | dict] = Field(alias="references", default=[])


class Intervention(BaseModel):

    name: str = Field(default=None)
    intervention_type: str = Field(alias="type")


class ArmsInterventionsModule(BaseModel):

    arms_interventions: list[Intervention] = Field(alias="interventions", default=[])


class Mesh(BaseModel):

    term: str
    mesh_id: str = Field(alias="id")


class InterventionBrowseModule(BaseModel):

    intervention_meshes: list[Mesh] = Field(alias="meshes", default=[])


class ConditionBrowseModule(BaseModel):

    condition_meshes: list[Mesh] = Field(alias="meshes", default=[])


class ProtocolSection(BaseModel):

    id_module: IDModule = Field(alias="identificationModule")
    conditions_module: ConditionsModule = Field(alias="conditionsModule", default=ConditionsModule())
    status_module: StatusModule = Field(alias="statusModule")
    design_module: DesignModule = Field(alias="designModule", default=DesignModule())
    references_module: ReferencesModule = Field(alias="referencesModule", default=ReferencesModule())
    arms_interventions_module: ArmsInterventionsModule = Field(alias="armsInterventionsModule", default=ArmsInterventionsModule())


class DerivedSection(BaseModel):

    condition_browse_module: ConditionBrowseModule = Field(alias="conditionBrowseModule", default=ConditionBrowseModule())
    intervention_browse_module: InterventionBrowseModule = Field(alias="interventionBrowseModule", default=InterventionBrowseModule())


class UnflattenedTrial(BaseModel):
    """
        Clinicaltrials.gov trial data from REST API response
    """

    protocol_section: ProtocolSection = Field(alias="protocolSection")
    derived_section: DerivedSection = Field(alias="derivedSection")
