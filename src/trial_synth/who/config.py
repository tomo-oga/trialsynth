from ..base.config import BaseConfig


class Config(BaseConfig):
    def __init__(self):
        self.registry = 'who'
        super().__post_init__()

        self.parsed_pickle_path = self.get_data_path(self.get_config('PROCESSED_FILE'))
        self.mappings_path = self.get_data_path(self.get_config('MAPPINGS_FILE'))
        self.sample_path = self.get_data_path(self.get_config('PROCESSED_SAMPLE'))
        self.ner_dir_path = self.get_data_path('ner')

CONFIG = Config()
FIELDS = CONFIG.fields