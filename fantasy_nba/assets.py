import pandas as pd
from sklearn.preprocessing import PowerTransformer
from numpy import sign
from dagster import (
    asset,
    AssetExecutionContext,
)

from .partitions import dataset_partition
from .configs import FantasyConfig, base_fantasy_config, DagsterFantasyConfig
from .transformations import (
    calculate_percentage_value,
    calculate_g_scores,
    calculate_z_scores,
    get_all_eligible_positions,
    aggregate_player_value,
)

from .settings import settings

CATEGORIES = list(base_fantasy_config.category_settings.keys())
POSITIONS = list(base_fantasy_config.position_settings.keys())


@asset
def base_config(
    context: AssetExecutionContext, config: DagsterFantasyConfig
) -> FantasyConfig:
    modified_config = base_fantasy_config.model_copy()

    for cat in CATEGORIES:
        modified_config.category_settings[cat].weight = config.weights[cat]

    for pos in POSITIONS:
        modified_config.position_settings[pos].slots = config.slots[pos]

    modified_config.blacklist = config.blacklist
    modified_config.team_ft = config.team_ft
    modified_config.team_fg = config.team_fg

    context.add_output_metadata(
        metadata={
            **modified_config.model_dump(),
        }
    )

    return modified_config


@asset(partitions_def=dataset_partition)
def load_data(
    context: AssetExecutionContext, base_config: FantasyConfig
) -> pd.DataFrame:
    data = pd.read_csv(f"{settings.data_dir}/{context.partition_key}")
    context.log.info(data)
    return data


@asset(partitions_def=dataset_partition)
def normalised_data(
    context: AssetExecutionContext,
    base_config: FantasyConfig,
    load_data: pd.DataFrame,
) -> pd.DataFrame:

    load_data["FG%"] = calculate_percentage_value(
        attempts=load_data["FGA"],
        percent=load_data["FG%"],
        team_percent=base_config.team_fg,
    )

    load_data["FT%"] = calculate_percentage_value(
        attempts=load_data["FTA"],
        percent=load_data["FT%"],
        team_percent=base_config.team_ft,
    )

    pt = PowerTransformer(method="yeo-johnson", standardize=True)
    load_data = load_data[base_config.metadata_columns + CATEGORIES]

    normal_data = load_data.copy()

    normal_data["POS"] = (
        normal_data["POS"]
        .str.split("/")
        .apply(lambda pos_list: get_all_eligible_positions(pos_list))
    )

    exploded_positions = normal_data.explode("POS")

    for pos in POSITIONS:
        pos_data = exploded_positions.loc[
            exploded_positions["POS"] == pos
        ].copy()
        for column in CATEGORIES:
            pos_data[column] = pt.fit_transform(
                pos_data[[column]] * base_config.mean_schedule_week
            )

        exploded_positions.loc[exploded_positions["POS"] == pos, CATEGORIES] = (
            pos_data[CATEGORIES]
        )
        context.log.info(pos)
        context.log.info(pos_data)

    context.log.info(exploded_positions)
    return exploded_positions


@asset(
    partitions_def=dataset_partition,
)
def positional_value_data(
    context: AssetExecutionContext,
    normalised_data: pd.DataFrame,
) -> pd.DataFrame:
    exploded_positions = normalised_data.copy()

    positional_data = (
        exploded_positions.groupby("POS")
        .apply(calculate_g_scores)
        .reset_index(drop=True)
    )

    for position in POSITIONS:
        pos_data = positional_data[positional_data["POS"] == position]

        positions_total = pos_data[CATEGORIES].sum()

        for category in CATEGORIES:
            sub_pos = pos_data[category]

            positions_left = sub_pos.sum()

        positional_data.loc[positional_data["POS"] == position, category] = (
            sign(sub_pos)
            * (abs(sub_pos) * (positions_total[category] / positions_left))
        )

    positional_data["VALUE"] = positional_data[CATEGORIES].sum(axis=1)

    context.log.info(positional_data)
    return positional_data


@asset(
    partitions_def=dataset_partition,
)
def bl_positional_value_data(
    context: AssetExecutionContext,
    base_config: FantasyConfig,
    normalised_data: pd.DataFrame,
) -> pd.DataFrame:

    blacklist = list(base_config.blacklist.keys())

    bl_data = normalised_data.copy()[~normalised_data["PLAYER"].isin(blacklist)]

    exploded_positions = bl_data.copy()

    positional_data = (
        exploded_positions.groupby("POS")
        .apply(calculate_g_scores)
        .reset_index(drop=True)
    )

    for position in POSITIONS:
        pos_data = positional_data[positional_data["POS"] == position]

        positions_total = pos_data[CATEGORIES].sum()

        for category in CATEGORIES:
            sub_pos = pos_data[category]

            positions_left = sub_pos.sum()

        positional_data.loc[positional_data["POS"] == position, category] = (
            sign(sub_pos)
            * (abs(sub_pos) * (positions_total[category] / positions_left))
        )

    positional_data["VALUE"] = positional_data[CATEGORIES].sum(axis=1)

    context.log.info(positional_data)
    return positional_data


@asset(partitions_def=dataset_partition)
def value_data(
    context: AssetExecutionContext,
    load_data: pd.DataFrame,
    positional_value_data: pd.DataFrame,
) -> pd.DataFrame:

    values_data = positional_value_data.copy()

    values_data[CATEGORIES] = values_data[CATEGORIES].apply(
        lambda x: x * base_fantasy_config.category_settings[x.name].weight
    )

    percent_gp = values_data["GP"] / 82
    values_data["VALUE"] = values_data[CATEGORIES].sum(axis=1)
    values_data["VALUE"] = sign(values_data["VALUE"]) * (
        abs(values_data["VALUE"]) * percent_gp
    )

    slot_value_data = values_data.copy()

    slot_value_data["SLOTS"] = slot_value_data["POS"].map(
        lambda pos: base_fantasy_config.position_settings[pos].slots
    )

    agg_slot_data = (
        slot_value_data.groupby("PLAYER")
        .apply(aggregate_player_value)
        .reset_index(drop=True)
    )

    agg_slot_data = pd.merge(
        agg_slot_data, load_data[["PLAYER", "POS"]], on="PLAYER"
    )

    context.log.info(agg_slot_data)
    return agg_slot_data


@asset(partitions_def=dataset_partition)
def bl_value_data(
    context: AssetExecutionContext,
    base_config: FantasyConfig,
    load_data: pd.DataFrame,
    bl_positional_value_data: pd.DataFrame,
) -> pd.DataFrame:

    values_data = bl_positional_value_data.copy()

    values_data[CATEGORIES] = values_data[CATEGORIES].apply(
        lambda x: x * base_config.category_settings[x.name].weight
    )

    percent_gp = values_data["GP"] / 82
    values_data["VALUE"] = values_data[CATEGORIES].sum(axis=1)
    values_data["VALUE"] = sign(values_data["VALUE"]) * (
        abs(values_data["VALUE"]) * percent_gp
    )

    slot_value_data = values_data.copy()

    slot_value_data["SLOTS"] = slot_value_data["POS"].map(
        lambda pos: (
            base_config.position_settings[pos].slots
            * (
                (base_fantasy_config.team_size - base_fantasy_config.bench_size)
                / base_fantasy_config.team_size
            )
            + base_fantasy_config.position_settings[pos].slots
            * (base_fantasy_config.bench_size / base_fantasy_config.team_size)
        )
    )

    agg_slot_data = (
        slot_value_data.groupby("PLAYER")
        .apply(aggregate_player_value)
        .reset_index(drop=True)
    )

    agg_slot_data = pd.merge(
        agg_slot_data, load_data[["PLAYER", "POS"]], on="PLAYER"
    )

    context.log.info(agg_slot_data)
    return agg_slot_data


@asset(partitions_def=dataset_partition)
def salary_data(
    context: AssetExecutionContext,
    base_config: FantasyConfig,
    bl_value_data: pd.DataFrame,
    punt_value: pd.DataFrame,
) -> pd.DataFrame:
    salary_data = bl_value_data.copy()

    c_scored_cats = [
        cat for cat in CATEGORIES if base_config.category_settings[cat].weight
    ]

    def peak_value(row):
        player_cats = c_scored_cats.copy()

        while len(player_cats) > max(6, len(c_scored_cats) - 1):

            punt_name = (
                punt_value[punt_value["PLAYER"] == row["PLAYER"]][player_cats]
                .idxmax(axis=1)
                .values[0]
            )

            min_category_value = row[punt_name]

            p_value = (row["VALUE"] - min_category_value) * (
                len(player_cats) / len(c_scored_cats)
            )
            row["VALUE"] = max(row["VALUE"], p_value)
            player_cats.remove(punt_name)

        return row

    salary_data = salary_data.apply(peak_value, axis=1)

    spent = list(base_config.blacklist.values())
    top_values = salary_data.sort_values(by="VALUE", ascending=False).head(
        max(base_config.total_drafted_players, 20)
    )

    salary_data["SALARY"] = (
        top_values["VALUE"]
        * ((base_config.fantasy_teams * base_config.salary_cap) - sum(spent))
        / top_values["VALUE"].sum()
    )

    salary_data["SALARY"] = salary_data["SALARY"].where(
        salary_data["SALARY"] > 1, 1
    )

    salary_data = salary_data[
        base_config.metadata_columns
        + list(base_config.category_settings.keys())
        + ["VALUE", "SALARY"]
    ]
    salary_data.sort_values(by="VALUE", ascending=False, inplace=True)

    context.log.info(salary_data)
    return salary_data


@asset(partitions_def=dataset_partition)
def punt_data(
    context: AssetExecutionContext,
    base_config: FantasyConfig,
    value_data: pd.DataFrame,
) -> pd.DataFrame:

    punt_data = value_data.copy()

    for category in CATEGORIES:
        punt_data[category] = (
            value_data[CATEGORIES].sum(axis=1) - value_data[category]
        )

    return punt_data


@asset(partitions_def=dataset_partition)
def punt_value(
    context: AssetExecutionContext,
    base_config: FantasyConfig,
    punt_data: pd.DataFrame,
) -> pd.DataFrame:

    punt_value = punt_data.copy()

    punt_value[CATEGORIES + ["VALUE"]] = punt_data[
        CATEGORIES + ["VALUE"]
    ].apply(calculate_z_scores)

    return punt_value


all_assets = [
    base_config,
    load_data,
    normalised_data,
    positional_value_data,
    value_data,
    salary_data,
    punt_data,
    punt_value,
    bl_positional_value_data,
    bl_value_data,
]
