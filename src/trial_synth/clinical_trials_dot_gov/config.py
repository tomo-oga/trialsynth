"""Clinicaltrials.gov processor configuration"""

import logging
from pathlib import Path
from ..base.config import BaseConfig

class Config(BaseConfig):
    """
    User-mutable properties of Clinicaltrials.gov data processing
    """
    def __init__(self):
        super(Config, self).__init__(registry='clinicaltrials')

        self.api_parameters = {
            "fields": ",".join(self.reg_fields),  # actually column names, not fields
            "pageSize": 1000,
            "countTotal": "true"
        }

        self.nodes_indra_path = self.get_data_path(self.get_config('NODES_INDRA_FILE'))
        self.node_types = ["BioEntity", "ClinicalTrial"]

CONFIG = Config()
FIELDS = CONFIG.fields