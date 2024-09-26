import time
import streamlit as st
import json
import pickle
import pandas as pd
import numpy as np

from fantasy_nba.configs import (
    CATEGORY_WEIGHTS,
    POSITION_SLOTS,
    POSITION_ELIGIBILITY_MAP,
    base_fantasy_config,
)

CATEGORIES = list(CATEGORY_WEIGHTS.keys())
POSITIONS = list(POSITION_ELIGIBILITY_MAP.keys())

# games cap?
# end early coz bs near playoffs
# IL spots
# team size


def filter_edited():
    if "edited" in st.session_state:
        edited = st.session_state.edited[
            st.session_state.edited["AVAILABLE"] == False  # noqa E712
        ]

        for index, row in edited.iterrows():
            st.session_state.blacklist[row["PLAYER"]] = row["PRICE"]

        bought = st.session_state.edited[st.session_state.edited["PRICE"] != 1]

        for index, row in bought.iterrows():
            st.session_state.blacklist[row["PLAYER"]] = row["PRICE"]


def refresh_data():
    filter_edited()

    copy = st.session_state.weights.copy()
    pcopy = st.session_state.slots
    team_ft = 0
    team_fg = 0

    if not st.session_state.team.empty:
        for category in CATEGORIES:
            if "%" in category:
                copy[category] = copy[category] * 0.75
                continue
            cat_total = st.session_state.team[category].sum()
            weight = 0.5 + (
                (1 - 1 / (1 + np.exp(-0.75 * (cat_total - 3)))) * 0.5
            )
            copy[category] = (
                np.sign(copy[category]) * abs(copy[category]) * weight
            )

        with open(
            "/home/bob/.dagster/storage/load_data/bob.csv", "rb"
        ) as filep:
            data = pickle.load(filep)
            team_data = data[
                data["PLAYER"].isin(st.session_state.team["PLAYER"].tolist())
            ]

            team_ft = team_data["FTM"].sum() / team_data["FTA"].sum()
            team_fg = team_data["FGM"].sum() / team_data["FGA"].sum()

    with open("custom_config.json", "w") as file:
        json.dump(
            json.dumps(
                {
                    "weights": copy,
                    "slots": pcopy,
                    "blacklist": st.session_state.blacklist,
                    "team_ft": team_ft,
                    "team_fg": team_fg,
                }
            ),
            file,
        )

    st.session_state.start_time = time.time()


def display_data(df, height=None):
    return st.data_editor(
        df.style.format(precision=2).background_gradient(
            cmap="RdYlGn", subset=CATEGORIES, vmin=-2.5, vmax=2.5
        ),
        height=height,
        use_container_width=True,
        hide_index=True,
        disabled=[
            column
            for column in df.columns
            if column not in ["PRICE", "AVAILABLE"]
        ],
        column_config={
            "PRICE": st.column_config.NumberColumn(required=True),
            "AVAILABLE": st.column_config.CheckboxColumn(required=True),
        },
    )


def save_state(selected_players, df):
    selected_rows = df[df["PLAYER"].isin(selected_players)]

    if "team" not in st.session_state:
        st.session_state.team = pd.DataFrame()

    st.session_state.team = (
        pd.concat([st.session_state.team, selected_rows])
        .drop_duplicates(subset="PLAYER")
        .reset_index(drop=True)
    )

    st.session_state.slots = optimise_slots(st.session_state.team)


def display_team():
    initial_max = 200 - (13 - len(st.session_state.team))
    init_total_money = (
        base_fantasy_config.salary_cap * base_fantasy_config.fantasy_teams
    )
    total_money = init_total_money - sum(st.session_state.blacklist.values())
    avg_ratio = total_money / init_total_money

    if "team" in st.session_state and not st.session_state.team.empty:
        team_df = st.session_state.team[["PLAYER", "TEAM", "POS"] + CATEGORIES]
        team_df["PRICE"] = team_df.apply(
            lambda x: st.session_state.blacklist.get(x["PLAYER"]), axis=1
        )

        display_data(team_df)
        max_bid = initial_max - team_df["PRICE"].sum()

        summary_row = team_df.select_dtypes(include="number").sum()
        summary_df = pd.DataFrame(summary_row).T
        summary_df["PLAYER"] = "Total                     "
        summary_df["POS"] = "-"
        summary_df["TEAM"] = "-"
        summary_df["PRICE"] = max_bid

        display_data(summary_df[list(team_df.columns)])
    else:
        st.write("No players selected yet!")
        max_bid = initial_max

    return (max_bid / initial_max) / avg_ratio


def optimise_slots(team):
    position_slots = POSITION_SLOTS.copy()
    drafted_positions = []

    with open(
        "/home/bob/.dagster/storage/positional_value_data/bob.csv", "rb"
    ) as filep:
        positional_value = pickle.load(filep)

    player_positions = positional_value[
        positional_value["PLAYER"].isin(team["PLAYER"].tolist())
    ]

    player_positions["DELTA"] = player_positions[
        "VALUE"
    ] - player_positions.groupby("PLAYER")["VALUE"].transform("mean")

    player_positions.sort_values(by="DELTA", ascending=False, inplace=True)

    bench = base_fantasy_config.bench_size
    print(player_positions)

    for index, row in player_positions.iterrows():
        player = row["PLAYER"]
        position = row["POS"]

        print(player, position)

        if position_slots[position] == 0 or player in drafted_positions:
            print("skip")
            continue

        position_slots[position] -= 1
        drafted_positions.append(player)

    for player in team["PLAYER"].tolist():
        if player not in drafted_positions:
            if bench == 0:
                eligible = positional_value[
                    positional_value["PLAYER"] == player
                ]
                for index, row in eligible.iterrows():
                    position = row["POS"]
                    print(player, position)
                    print("backup")
                    if position_slots[position] > 0:
                        position_slots[position] -= 1
                        drafted_positions.append(player)
            else:
                bench -= 1
                print(player, position)
                print("bench")

    return position_slots


def get_punt(team):
    if team.empty:
        return "NULL", 0

    with open("/home/bob/.dagster/storage/punt_value/bob.csv", "rb") as file:
        value = pickle.load(file)
        team_value = value[value["PLAYER"].isin(team["PLAYER"].tolist())]

    for cat in CATEGORIES:
        if st.session_state.weights[cat] == 0:
            team_value[cat] = 0

    punt_sum = team_value[CATEGORIES].sum()

    punt_name = punt_sum.idxmax()

    scored_cats = len(
        [cat for cat in CATEGORIES if st.session_state.weights[cat]]
    )

    punt_value = round(
        (team_value[punt_name].sum() - team_value["VALUE"].sum())
        * ((scored_cats - 1) / scored_cats),
        2,
    )

    return punt_name, punt_value


def app():
    st.set_page_config(layout="wide")
    if "start_time" not in st.session_state:
        st.session_state.start_time = None

    if "weights" not in st.session_state:
        st.session_state.weights = CATEGORY_WEIGHTS.copy()

    if "slots" not in st.session_state:
        st.session_state.slots = POSITION_SLOTS.copy()

    if "stale" not in st.session_state:
        st.session_state.stale = True

    if "team" not in st.session_state:
        st.session_state.team = pd.DataFrame({"PLAYER": []})

    if "blacklist" not in st.session_state:
        st.session_state.blacklist = {}

    with open("/home/bob/.dagster/storage/salary_data/bob.csv", "rb") as file:
        projections = pickle.load(file)
        projections = projections[
            ~projections["PLAYER"].isin(list(st.session_state.blacklist.keys()))
        ]

    with open("/home/bob/.dagster/storage/value_data/bob.csv", "rb") as file:
        value_data = pickle.load(file)

    player_search = value_data["PLAYER"].tolist()

    col1, col2 = st.columns([0.9, 0.1])
    selected_players = col1.multiselect(
        "Search and Select Players", player_search
    )

    if "PRICE" not in st.session_state:
        st.session_state.PRICE = 1

    projections["PRICE"] = st.session_state.PRICE
    projections["AVAILABLE"] = ~projections["PLAYER"].isin(
        list(st.session_state.blacklist.keys())
    )

    col2.write("loading..." if st.session_state.stale else "")
    if col2.button("Refresh"):
        save_state(selected_players, value_data)

        if "edited" in st.session_state:
            edited = st.session_state.edited[
                st.session_state.edited["PLAYER"].isin(selected_players)
            ]

            for index, row in edited.iterrows():
                st.session_state.blacklist[row["PLAYER"]] = row["PRICE"]

            filter_edited()

        if not st.session_state.stale:
            st.session_state.stale = True
            refresh_data()

        st.rerun()

    if st.session_state.stale:
        if not st.session_state.start_time:
            refresh_data()

        if time.time() - st.session_state.start_time >= 23:
            st.session_state.stale = False
            st.rerun()

    with st.sidebar:
        col1, col2 = st.columns(2)

        col1.write("Weights")
        for category in CATEGORIES:
            st.session_state.weights[category] = col1.number_input(
                category, value=st.session_state.weights[category]
            )

        col2.write("Slots")
        for pos in POSITIONS:
            st.session_state.slots[pos] = col2.number_input(
                pos, value=st.session_state.slots[pos]
            )

        punt_name, punt_value = get_punt(st.session_state.team)
        col2.metric(label="Punt", value=punt_name, delta=punt_value)

    salary_modifier = display_team()
    projections["SALARY"] = projections["SALARY"] * salary_modifier
    st.session_state.edited = display_data(projections)


if __name__ == "__main__":
    app()
