import os

from pydantic import ValidationError
import pytest

from trial_synth.clinical_trials_dot_gov import (
    config,
    fetch
)


DOCKERIZED = os.environ.get("DOCKERIZED", False)


@pytest.mark.skipif(DOCKERIZED, reason="Test against API, not stub")
def test_stability():
    """
    If the structure of the API response changes it will break the pipeline.
    Get sample data from the live API and validate it using its data model.
    """

    configuration = config.Config()
    json_data = fetch.send_request(configuration.api_url, configuration.api_parameters)
    studies = json_data.get("studies", [])
    try:
        _ = fetch.flatten_data(studies)
    except ValidationError as exc:
        pytest.fail(f"Unexpected error while flattening API response data: {exc}")

