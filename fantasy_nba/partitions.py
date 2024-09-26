import os
from dagster import StaticPartitionsDefinition

from .settings import settings

dataset_partition = StaticPartitionsDefinition(
    partition_keys=[filename for filename in os.listdir(settings.data_dir)]
)
