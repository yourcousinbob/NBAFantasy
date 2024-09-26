from dagster import Definitions

from .assets import all_assets
from .jobs import all_jobs
from .sensor import config_sensor

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    sensors=[config_sensor]
)
