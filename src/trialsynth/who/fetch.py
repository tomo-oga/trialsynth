import csv
import pickle

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


from ..base.fetch import BaseFetcher, logger
from .config import Config
from .trial_model import WhoTrial

from .util import PREFIXES, makelist, make_str
from ..base.models import BioEntity


class Fetcher(BaseFetcher):
    def __init__(self, config: Config):
        super().__init__()
        self.config = config

    def get_api_data(self):
        trial_path = self.config.raw_trial_path
        if trial_path.is_file():
            self.load_saved_data()
            return
        path = self.config.raw_data_path
        with open(path, 'r') as file:
            trials = [trial for trial in file]

            for trial in tqdm(
                    csv.reader(trials),
                    desc="Reading CSV WHO data",
                    total=len(trials), unit='trials'
            ):
                trial_id = trial[0].strip()
                trial_id = trial_id.replace('\ufeff', '')
                for p, prefix in PREFIXES.items():
                    if trial_id.startswith(p) or trial_id.startswith(p.lower()):
                        break
                else:
                    msg = f"could not identify {trial_id}"
                    raise ValueError(msg)

                if trial_id.startswith("EUCTR"):
                    trial_id = trial_id.removeprefix("EUCTR")
                    trial_id = "-".join(trial_id.split("-")[:3])

                    # handling inconsistencies with ChiCTR trial IDs
                if trial_id.lower().startswith("chictr-"):
                    trial_id = "ChiCTR-" + trial_id.lower().removeprefix("chictr-").upper()

                trial_id = trial_id.removeprefix("JPRN-").removeprefix("CTIS").removeprefix("PER-")

                with logging_redirect_tqdm():
                    who_trial = WhoTrial(prefix, trial_id)

                who_trial.title = make_str(trial[3])
                who_trial.type = make_str(trial[18])
                who_trial.design = makelist(trial[19], '.')
                who_trial.conditions = [BioEntity(None, None, condition) for condition in makelist(trial[29], '.')]
                who_trial.interventions = [BioEntity(None, None, intervention) for intervention in makelist(trial[30], ';')]
                who_trial.primary_outcome = make_str(trial[36])
                who_trial.secondary_outcome = make_str(trial[37])
                who_trial.secondary_ids = make_str(trial[2])
                self.raw_data.append(who_trial)
        self.save_raw_data(self.raw_data)

