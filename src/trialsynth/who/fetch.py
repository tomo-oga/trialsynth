import csv
import pickle

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from ..base.fetch import BaseFetcher, logger
from .config import Config
from .trial_model import WhoTrial

from .util import PREFIXES, makelist, make_str
from ..base.models import BioEntity, Outcome, SecondaryId, DesignInfo


class Fetcher(BaseFetcher):
    def __init__(self, config: Config):
        super().__init__()
        self.config = config

    def get_api_data(self, reload=True):
        trial_path = self.config.raw_data_path
        if trial_path.is_file() and not reload:
            self.load_saved_data()
            return
        path = self.config.get_data_path('ICTRP.csv')
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

                design_list = [design.strip() for design in makelist(trial[19], '.')]
                design_dict = {}

                try:
                    for design_attr in design_list:
                        key, value = design_attr.split(':', 1)
                        design_dict[key.strip().lower()] = value.strip()
                        who_trial.design = DesignInfo(
                            allocation=design_dict.get('allocation'),
                            assignment=design_dict.get('intervention model'),
                            masking=design_dict.get('masking'),
                            purpose=design_dict.get('primary purpose')
                        )
                except Exception:
                    logger.debug(f"Error in design attribute for curie: {who_trial.curie} using fallback")
                    pass

                if who_trial.design is None:
                    who_trial.design = DesignInfo(fallback=trial[19])
                who_trial.conditions = [BioEntity(term=condition) for condition in makelist(trial[29], ';')]
                who_trial.interventions = [BioEntity(term=intervention) for intervention in makelist(trial[30], ';')]
                who_trial.primary_outcome = Outcome(measure=make_str(trial[36]))
                who_trial.secondary_outcome = Outcome(measure=make_str(trial[37]))
                who_trial.secondary_ids = [SecondaryId(curie=curie) for curie in makelist(trial[2], ';')]
                self.raw_data.append(who_trial)
        self.save_raw_data()
