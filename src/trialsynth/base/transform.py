from typing import Tuple

from .models import Trial, BioEntity, Edge


def transform_secondary_ids(trial: Trial) -> str:
    """Transforms a list of secondary IDs into a string."""
    return ''.join([id.curie for id in trial.secondary_ids])


def transform_secondary_outcome(trial: Trial) -> str:
    """Transforms the secondary outcome of a trial into a string."""
    trial.secondary_outcomes = '|'.join([
        f'Measure: {outcome.measure.strip() if outcome.measure else ""}; '
        f'Time Frame: {outcome.time_frame.strip() if outcome.time_frame else ""}'
        for outcome in trial.secondary_outcomes
    ])
    return trial.secondary_outcomes


def transform_primary_outcome(trial: Trial) -> str:
    """Transforms the primary outcome of a trial into a string."""
    trial.primary_outcomes = '|'.join([
        f'Measure: {outcome.measure.strip() if outcome.measure else ""}; '
        f'Time Frame: {outcome.time_frame.strip() if outcome.time_frame else ""}'
        for outcome in trial.primary_outcomes
    ])
    return trial.primary_outcomes


def transform_interventions(trial: Trial) -> str:
    """Transforms a list of interventions into a string."""
    return ','.join([intervention.curie for intervention in set(trial.interventions) if intervention])


def transform_conditions(trial: Trial) -> str:
    """Transforms a list of conditions into a string."""
    return ','.join([condition.curie for condition in set(trial.conditions) if condition])


def transform_design(trial: Trial) -> str:
    """Transforms the design of a trial into a string."""
    if trial.design.fallback:
        return trial.design.fallback

    return (f'Purpose: {trial.design.purpose.strip() if trial.design.purpose else ""}; '
            f'Allocation: {trial.design.allocation.strip() if trial.design.allocation else ""};'
            f'Masking: {trial.design.masking.strip() if trial.design.masking else ""}; '
            f'Assignment: {trial.design.assignment.strip() if trial.design.assignment else ""}')


def transform_type(trial: Trial) -> str:
    """Transforms the type of a trial into a string."""
    return trial.type.strip() if trial.type else ''


def transform_title(trial: Trial) -> str:
    """Transforms the title of a trial into a string."""
    return trial.title.strip()


def flatten_trial_data(trial: Trial) -> Tuple:
    """Flattens trial data into a tuple of strings.

    Parameters
    ----------
    trial: Trial
        The trial to transform

    Returns
    -------
    transformed_data: Tuple
        A tuple of the transformed data. In order of title, type, design, conditions, interventions,
        primary_outcome, secondary_outcome, secondary_ids.

    """
    return (
        trial.curie,
        transform_title(trial),
        transform_type(trial),
        transform_design(trial),
        transform_conditions(trial),
        transform_interventions(trial),
        transform_primary_outcome(trial),
        transform_secondary_outcome(trial),
        transform_secondary_ids(trial),
        trial.source
    )


def flatten_bioentity(entity: BioEntity) -> Tuple[str, str, str]:
    """Flattens a BioEntity into a tuple of strings.

    Parameters
    ----------
    entity : BioEntity
        The BioEntity to flatten

    Returns
    -------
    Tuple[str, str, str]
        A tuple of the flattened BioEntity. In order of curie, term, source.
    """
    return entity.curie, entity.term, entity.type, entity.source


def flatten_edge(edge: Edge) -> Tuple[str, str, str, str, str]:
    """Flattens an Edge into a tuple of strings.

    Parameters
    ----------
    edge : Edge
        The Edge to flatten

    Returns
    -------
    Tuple[str, str, str, str, str]
        A tuple of the flattened Edge. In order of trial_curie, bio_ent_curie, rel_type, rel_type_curie, source.
    """
    return edge.trial_curie, edge.bio_ent_curie, edge.rel_type, edge.rel_type_curie, edge.source


