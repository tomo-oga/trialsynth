import sys
from dataclasses import dataclass, field
import logging
from pathlib import Path
import os

from .util import list_from_string

import shutil

if sys.version_info[0] == 3:
    from configparser import RawConfigParser
else:
    from ConfigParser import RawConfigParser

logger = logging.getLogger(__name__)


class TrialSynthConfigError(Exception):
    pass


class BaseConfig:
    """
    User-mutable properties of a registry for data processing
    """

    def __init__(self):
        self.registry: str = None

    def __post_init__(self):
        self.config_dict: dict = self._create_config_dict()

        self.name: str = self.get_config('PROCESSOR_NAME')

        self.data_dir: Path = Path(self.get_config('DATA_DIR'))
        self.sample_dir: Path = self.data_dir.joinpath('samples')
        self.raw_data_path: Path = self.get_data_path(self.get_config('RAW_DATA'))

        # get types of nodes from configuration file and create paths
        self.node_types: list[str] = list_from_string(self.get_config('NODE_TYPES'))
        self.node_types_to_paths: dict = {
            node_type: (
                self.path_from_type_template(self.data_dir, 'NODE_FILE_TEMPLATE', node_type),
                self.path_from_type_template(self.data_dir, 'NODE_PICKLE_TEMPLATE', node_type),
                self.path_from_type_template(self.sample_dir, 'NODE_SAMPLE_TEMPLATE', node_type)
            )
            for node_type in self.node_types
        }

        self.edges_path: Path = self.get_data_path(self.get_config('EDGES_FILE'))
        self.edges_sample_path: Path = Path(self.sample_dir, self.get_config('EDGES_SAMPLE_FILE'))

        self.fields: list[str] = list_from_string(self.get_config('DATA_FIELDS'))

        self.api_url = self.get_config('API_URL')
        self.api_parameters = {}

        self.has_condition_curie: str = self.get_config('CONDITION_CURIE')
        self.has_intervention_curie: str = self.get_config('INTERVENTION_CURIE')
        self.related_trial_curie: str = self.get_config('RELATED_TRIAL_CURIE')

        self.node_headers: list[str] = list_from_string(self.get_config('NODE_HEADERS'))

        self.source_key = self.registry

        root = logging.getLogger()
        root.setLevel(self.get_config('LOGGING_LEVEL'))

    def _create_data_dir(self):
        """Create the data directory if it doesn't exist
        """
        home_dir = os.path.expanduser('~')
        data_dir = os.path.join(home_dir, '.data', 'trialsynth', self.registry)

        if not os.path.isdir(data_dir):
            try:
                os.makedirs(data_dir)
            except Exception:
                logger.warning(data_dir + ' already exists')

        return data_dir

    def _create_config_dict(self):
        """Load the configuration file into the config_file dictionary
        """
        home_dir = os.path.expanduser('~')
        config_dir = os.path.join(home_dir, '.config', 'trialsynth', self.registry)
        config_path = os.path.join(config_dir, 'config.ini')
        default_config_path = os.path.join(os.path.dirname(__file__),
                                           'resources/default_config.ini')
        if not os.path.isfile(config_path):
            try:
                os.makedirs(config_dir)
            except Exception:
                logger.warning(config_dir + ' already exists')
            try:
                shutil.copyfile(default_config_path, config_path)
            except Exception:
                logger.warning('Could not copy default config file.')

        try:
            config_dict = {}
            parser = RawConfigParser()
            parser.optionxform = lambda x: x
            parser.read(config_path)
            sections = parser.sections()

            options = parser.options('trialsynth')
            for option in options:
                config_dict[option] = str(parser.get('trialsynth', option))

            if self.registry in sections:
                options = parser.options(self.registry)
                for option in options:
                    if option in config_dict:
                        logger.info("Overwriting package level configuration with registry level for option: " + option)
                    config_dict[option] = str(parser.get(self.registry, option))
            else:
                raise ValueError(f"Registry [{self.registry}] not found in configuration file.")

        except Exception as e:
            logger.warning("Could not load configuration file due to exception. "
                           "Only environment variable equivalents will be used.")
            return None

        for key in config_dict.keys():
            if config_dict == '':
                config_dict[key] = None
            elif isinstance(config_dict[key], str):
                config_dict[key] = os.path.expanduser(config_dict[key])

        config_dict['DATA_DIR'] = self._create_data_dir()
        config_dict['SOURCE_KEY'] = self.registry
        return config_dict

    def get_config(self, key: str, failure_ok: bool = True):
        """
        Get a configuration value from the environment or config file.
        Parameters
        ----------
        key : str
            The key for the configuration value
        config_dict : dict
            The configuration dictionary
        failure_ok : bool
            If False and the configuration is missing, an WhoConfigError is
                raised. If True, None is returned and no error is raised in case
                of a missing configuration. Default: False

        Returns
        -------
        value : str or None
            The configuration value or None if the configuration value doesn't
            exist and failure_ok is set to True.
        """

        err_msg = "Key %s not in environment or config file." % key
        if key in os.environ:
            return os.environ[key]
        elif key in self.config_dict:
            val = self.config_dict[key]
            # We interpret an empty value in the config file as a failure
            if val is None and not failure_ok:
                msg = 'Key %s is set to an empty value in config file.' % key
                raise TrialSynthConfigError(msg)
            else:
                return val
        elif not failure_ok:
            raise TrialSynthConfigError(err_msg)
        else:
            return None

    def get_data_path(self, filename: str) -> Path:
        return Path(self.data_dir, filename)

    def path_from_type_template(self, directory: Path, template: str, node_type: str) -> Path:
        return Path(directory, self.get_config(template).replace('[TYPE]', node_type))

CONFIG = BaseConfig()