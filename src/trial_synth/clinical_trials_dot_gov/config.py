"""Clinicaltrials.gov processor configuration"""

import logging
from pathlib import Path
from ..base.config import BaseConfig

class Config(BaseConfig):
    """
    User-mutable properties of Clinicaltrials.gov data processing
    """
    def __init__(self):
        self.registry = 'clinicaltrials'
        super().__post_init__()

        self.api_parameters = {
            "fields": ",".join(self.fields),  # actually column names, not fields
            "pageSize": 1000,
            "countTotal": "true"
        }

        self.nodes_indra_path = self.get_data_path(self.get_config('NODES_INDRA_FILE'))
        self.node_types = ["BioEntity", "ClinicalTrial"]

CONFIG = Config()
FIELDS = CONFIG.fields