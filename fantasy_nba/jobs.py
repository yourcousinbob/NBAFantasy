from dagster import define_asset_job
from .assets import (
    positional_value_data,
    value_data,
    salary_data,
    base_config,
    punt_value,
    punt_data,
    bl_positional_value_data,
    bl_value_data,
    normalised_data,
)
from .partitions import dataset_partition


all_assets_job = define_asset_job(
    name="all_assets_job", partitions_def=dataset_partition
)
refresh_job = define_asset_job(
    name="refresh_job",
    selection=[
        positional_value_data,
        value_data,
        normalised_data,
        salary_data,
        base_config,
        punt_value,
        punt_data,
        bl_positional_value_data,
        bl_value_data,
    ],
    partitions_def=dataset_partition,
)

all_jobs = [all_assets_job, refresh_job]
