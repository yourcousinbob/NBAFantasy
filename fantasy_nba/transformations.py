import pandas as pd
from numpy import sign

from .configs import base_fantasy_config


def calculate_percentage_value(
    made: pd.Series = None,
    attempts: pd.Series = None,
    percent: pd.Series = None,
    team_percent: int = 0,
) -> pd.Series:
    if percent is not None:
        mean_percentage = percent.mean()
    else:
        mean_percentage = made.sum() / attempts.sum()

    if not team_percent:
        team_percent = mean_percentage

    if percent is not None:
        adjusted_percentage_value = (percent - team_percent) * attempts
    else:
        adjusted_percentage_value = (
            (made / attempts) - team_percent
        ) * attempts

    return adjusted_percentage_value


def calculate_z_scores(category: pd.Series) -> pd.Series:
    return (category - category.mean()) / category.std()


def calculate_g_scores(group: pd.DataFrame) -> pd.DataFrame:
    for column in group.columns:
        if column in base_fantasy_config.category_settings.keys():
            value = calculate_z_scores(group[column])
            group[column] = (
                sign(value)
                * abs(value)
                * base_fantasy_config.category_settings[column].week_variability
            )
    return group


def get_all_eligible_positions(pos_list: str):
    eligible_set = set()
    for pos in pos_list:
        eligible_set.update(
            base_fantasy_config.position_settings[pos].eligible_positions
        )
    return list(eligible_set)


def aggregate_player_value(positional_values):
    total_slots = 0
    for pos in positional_values["POS"]:
        total_slots += base_fantasy_config.position_settings[pos].slots

    aggregated = {}
    for col in base_fantasy_config.category_settings.keys():
        weighted_sum = (
            (positional_values[col] * positional_values["SLOTS"])
        ).sum()
        aggregated[col] = weighted_sum / total_slots

    weighted_value_sum = (
        (positional_values["VALUE"] * positional_values["SLOTS"])
    ).sum()
    aggregated["VALUE"] = weighted_value_sum / total_slots

    for col in base_fantasy_config.metadata_columns:
        if col == "POS":
            continue

        aggregated[col] = positional_values[col].iloc[0]

    return pd.Series(aggregated)
