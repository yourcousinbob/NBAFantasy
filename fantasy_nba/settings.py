from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    data_dir: str = Field("./data", env="DATA_DIR")
    output_dir: str = Field("./output", env="OUTPUT_DIR")
    custom_config: str = Field("./custom_config.json", env="CUSTOM_CONFIG")


settings = Settings()
