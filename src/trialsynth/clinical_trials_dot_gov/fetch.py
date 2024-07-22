"""Gets Clinicaltrials.gov data from REST API or saved file"""
import requests

from .rest_api_response_models import UnflattenedTrial
from ..base.fetch import BaseFetcher, logger
from ..base.config import Config

from itertools import zip_longest

from tqdm import trange

from ..base.models import Trial, BioEntity, SecondaryId, DesignInfo, Outcome

from bioregistry import curie_to_str


class Fetcher(BaseFetcher):
    def __init__(self, config: Config):
        super().__init__(config)
        self.api_parameters = {
            "fields": self.config.api_fields,  # actually column names, not fields
            "pageSize": 1000,
            "countTotal": "true"
        }
        self.total_pages = 0

    def get_api_data(self, reload: bool = False) -> None:
        trial_path = self.config.raw_data_path
        if trial_path.is_file() and not reload:
            self.load_saved_data()
            return

        logger.info(f"Fetching Clinicaltrials.gov data from {self.url} with parameters {self.api_parameters}")

        try:
            self._read_next_page()

            pages = 1 + self.total_pages
            for page in trange(1, pages, unit='page', desc='Downloading ClinicalTrials.gov data'):
                self._read_next_page()

        except Exception:
            logger.exception(f'Could not fetch data from {self.url}')
            raise

        self.save_raw_data()

    def _read_next_page(self):
        response = requests.get(self.url, self.api_parameters)
        response.raise_for_status()
        json_data = response.json()

        studies = json_data.get('studies', [])
        trials = self._json_to_trials(studies)
        self.raw_data.extend(trials)
        self.api_parameters['pageToken'] = json_data.get('nextPageToken')

        if not self.total_pages:
            self.total_pages = json_data.get('totalCount') // self.api_parameters.get('pageSize')

    def _json_to_trials(self, data: dict) -> list[Trial]:
        trials = []

        for study in data:
            rest_trial = UnflattenedTrial(**study)

            trial = Trial(ns='clinicaltrials', id=rest_trial.protocol_section.id_module.nct_id)

            trial.title = rest_trial.protocol_section.id_module.brief_title

            trial.type = rest_trial.protocol_section.design_module.study_type
            design_info = rest_trial.protocol_section.design_module.design_info
            trial.design = DesignInfo(
                purpose=design_info.purpose,
                allocation=design_info.allocation,
                masking=design_info.masking_info.masking,
                assignment=design_info.intervention_assignment
                if design_info.intervention_assignment else design_info.observation_assignment
            )

            condition_meshes = rest_trial.derived_section.condition_browse_module.condition_meshes
            conditions = rest_trial.protocol_section.conditions_module.conditions
            trial.conditions = [
                BioEntity(
                    ns='MESH' if mesh else None,
                    id=mesh.mesh_id if mesh else None,
                    term=mesh.term if mesh else None,
                    name=condition,
                    origin=trial.curie
                ) for condition, mesh in zip_longest(conditions, condition_meshes, fillvalue=None)
            ]

            intervention_arms = rest_trial.protocol_section.arms_interventions_module.arms_interventions
            intervention_meshes = rest_trial.derived_section.intervention_browse_module.intervention_meshes

            trial.interventions = [
                BioEntity(
                    ns='MESH' if mesh else None,
                    id=mesh.mesh_id if mesh else None,
                    term=mesh.term if mesh else None,
                    name=i.name if i else None,
                    type=i.intervention_type.capitalize() if i else None,
                    origin=trial.curie
                ) for i, mesh in zip_longest(intervention_arms, intervention_meshes, fillvalue=None)
            ]

            primary_outcomes = rest_trial.protocol_section.outcomes_module.primary_outcome
            trial.primary_outcomes = [Outcome(o.measure, o.time_frame) for o in primary_outcomes]

            secondary_outcomes = rest_trial.protocol_section.outcomes_module.secondary_outcome
            trial.secondary_outcomes = [Outcome(o.measure, o.time_frame) for o in secondary_outcomes]

            secondary_info = rest_trial.protocol_section.id_module.secondary_ids
            trial.secondary_ids = [
                SecondaryId(
                    ns=s.id_type,
                    id=s.secondary_id,
                    curie=curie_to_str(s.id_type, s.secondary_id)
                ) for s in secondary_info
            ]

            trials.append(trial)

        return trials
