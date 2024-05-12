from pyspark.sql import SparkSession
import logging
from databricks.connect import DatabricksSession
import os


def get_dbutils(spark: SparkSession):
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except ImportError as e:
        logging.error("Error while importing DBUtils: %s", e)
        return None


class SparkCommon:
    """Initialize and supply Spark"""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SparkCommon, cls).__new__(cls)
        return cls._instance

    def __init__(self, spark=None, app_name="DAB_BOILERPLATE"):
        if not hasattr(self, "initialized"):
            self.logger = logging.getLogger(__name__)
            self.spark = spark if spark else self._prepare_spark(app_name)
            self.logger.info("Spark initialized.")
            self.dbutils = get_dbutils(self.spark)
            self.initialized = True

    @staticmethod
    def _prepare_spark(app_name) -> SparkSession:
        """
        Create Spark session

        Args:
            app_name (str): Name of the application

        Returns:
            SparkSession: A SparkSession instance
        """
        if "DB_HOME" in os.environ:
            return SparkSession.builder.appName(app_name).getOrCreate()
        return DatabricksSession.builder.getOrCreate()


def main():
    spark_common = SparkCommon()
    print(spark_common.spark)


if __name__ == "__main__":
    main()
