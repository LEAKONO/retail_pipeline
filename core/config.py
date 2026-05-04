
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    snowflake_account:    str = Field(..., description="Snowflake account ID")
    snowflake_user:       str = Field(..., description="Snowflake username")
    snowflake_password:   str = Field(..., description="Snowflake password")
    snowflake_database:   str = Field(..., description="Target database")
    snowflake_warehouse:  str = Field(..., description="Virtual warehouse")
    snowflake_role:       str = Field(..., description="Pipeline role")
    snowflake_raw_schema: str = Field(default="RAW", description="Raw schema")
    source_file_path: str = Field(..., description="Path to source CSV")
    batch_size:       int = Field(default=10000, description="Rows per batch")
    pipeline_name:    str = Field(default="orders_ingestion", description="Pipeline ID")

    log_level: str = Field(default="INFO", description="Log level")

    class Config:
        env_file          = ".env"
        env_file_encoding = "utf-8"
        case_sensitive    = False


settings = Settings()
