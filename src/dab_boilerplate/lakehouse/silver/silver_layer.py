from dab_boilerplate.core.spark_common import SparkCommon
from dab_boilerplate.core.constants import Constants
from dab_boilerplate.db.common_functions import CommonFunctions
import logging
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, FloatType, TimestampType
from pyspark.sql import DataFrame


class SilverLayer:
    """SilverLayer - Cleansed and conformed data"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spark_common = SparkCommon(app_name="boilerplate")
        self.spark = self.spark_common.spark
        self.dbutils = self.spark_common.dbutils
        self.common_db_functions = CommonFunctions()
        self.dimension_schema = (
            Constants.CATALOG + "." + Constants.DIMENSION_SCHEMA
        )
        self.bronze_schema = Constants.CATALOG + "." + Constants.BRONZE_SCHEMA
        self.silver_schema = Constants.CATALOG + "." + Constants.SILVER_SCHEMA
        self.config_table = (
            self.dimension_schema + "." + Constants.CONFIG_TABLE
        )
        self.data_table = self.bronze_schema + "." + Constants.DATA_TABLE
        self.measurements_table = (
            self.silver_schema + "." + Constants.DATA_TABLE
        )

    def validate_and_transform_sensor_data(
        self, sensor_data_df: DataFrame
    ) -> DataFrame:
        """
        Validate and transform sensor data.

        Args:
        sensor_data_df (DataFrame): Raw sensor data DataFrame.

        Returns:
        DataFrame: Validated and transformed DataFrame.
        """
        # Validation and transformation logic here
        # Example: Cast columns to appropriate data types
        return (
            sensor_data_df.withColumn(
                "timestamp", col("timestamp").cast(TimestampType())
            )
            .withColumn("sensor_id", col("sensor_id").cast(IntegerType()))
            .withColumn("value", col("value").cast(FloatType()))
            .filter(col("sensor_id").isNotNull() & col("value").isNotNull())
        )


def main():

    silver = SilverLayer()
    # load data
    sensor_data_df = silver.common_db_functions.load_data(silver.data_table)
    silver.logger.info(f"Data loaded from {silver.data_table}.")
    sensor_config_df = silver.common_db_functions.load_data(
        silver.config_table
    )
    silver.logger.info(f"Data loaded from {silver.config_table}.")
    # validate and transform sensor data
    silver.validate_and_transform_sensor_data(sensor_data_df)
    silver.logger.info("Sensor data validated.")

    # Enrich sensor data with configuration data
    silver_df = sensor_data_df.join(sensor_config_df, "sensor_id")

    # Add ingestion timestamp to track when data was transformed
    silver_df = silver_df.withColumn("ingestion_time", current_timestamp())

    silver.logger.info("Sensor data enriched and ready to ingest to silver.")

    # Define match condition and upsert data using common db function.
    match_condition = (
        "target.sensor_id = source.sensor_id AND "
        + "target.timestamp = source.timestamp"
    )
    silver.common_db_functions.upsert_to_delta_table(
        silver_df, silver.measurements_table, match_condition, "sensor_id"
    )


if __name__ == "__main__":
    main()
