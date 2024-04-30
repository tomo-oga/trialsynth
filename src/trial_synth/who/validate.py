"""Validation for WHO clinical trials"""

from .util import PATTERNS


def is_valid(pattern: str, trial_id: str) -> bool:
    if pattern in PATTERNS and not PATTERNS[pattern].match(trial_id):
        return False
    return True
