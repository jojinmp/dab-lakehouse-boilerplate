from dab_boilerplate.core.spark_common import SparkCommon
from dab_boilerplate.core.constants import Constants
from dab_boilerplate.db.common_functions import CommonFunctions
import dab_boilerplate.core.logger  # noqa: F401
import logging
from pyspark.sql.functions import col, date_trunc, avg, max, min


class GoldLayer:
    """GoldLayer - Curated business-level tables"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spark_common = SparkCommon(app_name="boilerplate")
        self.spark = self.spark_common.spark
        self.dbutils = self.spark_common.dbutils
        self.common_db_functions = CommonFunctions()
        self.silver_schema = Constants.CATALOG + "." + Constants.SILVER_SCHEMA
        self.gold_schema = Constants.CATALOG + "." + Constants.GOLD_SCHEMA
        self.measurements_table = (
            self.silver_schema + "." + Constants.DATA_TABLE
        )
        self.aggregate_table = (
            self.gold_schema + "." + Constants.AGGREGATE_TABLE
        )

    def aggregate_and_upsert_data(
        self, silver_table_name: str, gold_table_name: str
    ):
        """
        Aggregate sensor data and upsert into the
        gold-layer table, optimized for reporting.
        Uses Delta Lake's merge capability for idempotent updates.

        Args:
        silver_table_name (str): Name of the
            silver layer table to read data from.
        gold_table_name (str): Name of the gold layer
            table to write or upsert data into.
        """
        # Load silver data
        df = self.spark.table(silver_table_name)

        # Aggregate data by sensor_id and day
        aggregated_df = df.groupBy(
            "sensor_id", date_trunc("day", col("timestamp")).alias("date")
        ).agg(
            avg(col("value")).alias("average_temperature"),
            max(col("value")).alias("max_temperature"),
            min(col("value")).alias("min_temperature"),
        )
        self.logger.info("Sensor data aggregated and ready to ingest to gold.")
        match_condition = (
            "gold.sensor_id = updates.sensor_id AND gold.date = updates.date"
        )
        self.common_db_functions.upsert_to_delta_table(
            aggregated_df, self.aggregate_table, match_condition, "date"
        )

        # Optimize the table by compacting files
        self.spark.sql(f"OPTIMIZE {gold_table_name} ZORDER BY (sensor_id)")


def main():
    gold = GoldLayer()
    gold.aggregate_and_upsert_data(
        gold.measurements_table, gold.aggregate_table
    )


if __name__ == "__main__":
    main()
