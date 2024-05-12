import os
from dotenv import load_dotenv

load_dotenv()


class Constants:
    """To hold constants"""

    CATALOG = os.getenv("CATALOG", "dev")
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"
    DIMENSION_SCHEMA = "dimensions"
    CONFIG_TABLE = "sensor_config"
    DATA_TABLE = "sensor_data"
    AGGREGATE_TABLE = "daily_sensor_summary"
    DATABRICKS_PROFILE = "DEFAULT"
