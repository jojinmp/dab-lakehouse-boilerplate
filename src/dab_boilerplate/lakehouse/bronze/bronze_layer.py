from dab_boilerplate.core.spark_common import SparkCommon
from dab_boilerplate.core.constants import Constants
from dab_boilerplate.db.common_functions import CommonFunctions
import logging


class BronzeLayer:
    """BronzLayer - Raw data layer."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spark_common = SparkCommon(app_name="boilerplate")
        self.spark = self.spark_common.spark
        self.dbutils = self.spark_common.dbutils
        self.common_db_functions = CommonFunctions()
        self.bronze_schema = Constants.CATALOG + "." + Constants.BRONZE_SCHEMA
        self.data_table = self.bronze_schema + "." + Constants.DATA_TABLE
        self.sensor_data_path = (
            "../data/sample_data/sample_data_2024-03-23 18:05:40.718892.json"
        )


def main():
    bronze_layer = BronzeLayer()
    # check or create sensor data table and load data
    bronze_layer.common_db_functions.create_table(
        bronze_layer.data_table, bronze_layer.sensor_data_path
    )


if __name__ == "__main__":
    main()
