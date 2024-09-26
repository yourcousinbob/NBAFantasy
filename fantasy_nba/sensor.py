import os
import json
import time
from dagster import sensor, RunRequest, RunConfig

from .configs import (
    DagsterFantasyConfig,
    base_fantasy_config,
    POSITION_SLOTS,
    CATEGORY_WEIGHTS,
)
from .jobs import refresh_job, all_assets_job
from .settings import settings


@sensor(job=refresh_job, minimum_interval_seconds=2)
def config_sensor():
    if os.path.isfile(settings.custom_config):
        # load config
        with open(settings.custom_config) as f:
            config = json.loads(json.load(f))
            yield RunRequest(
                run_key=str(time.time()),
                partition_key="bob.csv",
                job_name="refresh_job",
                run_config=RunConfig(
                    {"base_config": DagsterFantasyConfig(**config)}
                ),
            )
        os.remove(settings.custom_config)
