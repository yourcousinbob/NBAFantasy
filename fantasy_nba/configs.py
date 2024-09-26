from dagster import Config
from pydantic import BaseModel
from typing import List, Literal, Set


class CategoryConfig(BaseModel):
    weight: float = 1
    week_variability: float = 1


class PositionConfig(BaseModel):
    eligible_positions: List[
        Literal["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL", "BENCH"]
    ]
    slots: int


class FantasyConfig(BaseModel):
    fantasy_teams: int
    salary_cap: int
    mean_schedule_week: float
    position_settings: dict[str, PositionConfig]
    category_settings: dict[str, CategoryConfig]
    metadata_columns: List[str]
    blacklist: dict[str, int] = {}
    bench_size: int = 3
    injury_reserve: int = 1
    team_ft: float = 0
    team_fg: float = 0

    @property
    def team_size(self) -> int:
        return sum(POSITION_SLOTS.values()) + self.bench_size

    @property
    def total_drafted_players(self) -> int:
        return (
            self.team_size * self.fantasy_teams
            - len(self.blacklist)
        )


class DagsterFantasyConfig(Config):
    slots: dict[str, int]
    weights: dict[str, float]
    blacklist: dict[str, int]
    team_ft: float
    team_fg: float


POSITION_ELIGIBILITY_MAP = {
    "PG": ["PG", "G", "UTIL"],
    "SG": ["SG", "G", "UTIL"],
    "SF": ["SF", "F", "UTIL"],
    "PF": ["PF", "F", "UTIL"],
    "G": ["PG", "SG", "G", "UTIL"],
    "F": ["SF", "PF", "F", "UTIL"],
    "C": ["C", "UTIL"],
    "UTIL": ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"],
}


POSITION_SLOTS = {
    "PG": 1,
    "SG": 1,
    "SF": 1,
    "PF": 1,
    "G": 1,
    "F": 1,
    "C": 1,
    "UTIL": 3,
}

CATEGORY_WEIGHTS = {
    "PTS": 1,
    "REB": 1,
    "AST": 1,
    "STL": 1,
    "BLK": 1,
    "3PM": 1,
    "FG%": 1,
    "FT%": 1,
    # "TO": -1,
    "TO": 0,
}

CATEGORY_WEEK_VARIABILITY = {
    "PTS": 0.8,
    "REB": 0.7,
    "AST": 0.8,
    "STL": 0.5,
    "BLK": 0.7,
    "3PM": 0.65,
    "FG%": 0.5,
    "FT%": 0.45,
    "TO": 0.6,
}

METADATA_COLUMNS = ["PLAYER", "TEAM", "POS", "GP"]

base_fantasy_config = FantasyConfig(
    fantasy_teams=12,
    salary_cap=200,
    mean_schedule_week=3.28,
    metadata_columns=METADATA_COLUMNS,
    position_settings={
        position: PositionConfig(
            eligible_positions=POSITION_ELIGIBILITY_MAP[position],
            slots=POSITION_SLOTS[position],
        )
        for position in POSITION_ELIGIBILITY_MAP.keys()
    },
    category_settings={
        category: CategoryConfig(
            weight=CATEGORY_WEIGHTS[category],
            week_variability=CATEGORY_WEEK_VARIABILITY[category],
        )
        for category in CATEGORY_WEIGHTS.keys()
    },
    blacklist={},
)
