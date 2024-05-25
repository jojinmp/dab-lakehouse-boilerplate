from dab_boilerplate.core.spark_common import SparkCommon
from dab_boilerplate.core.constants import Constants
from dab_boilerplate.db.common_functions import CommonFunctions
import dab_boilerplate.core.logger  # noqa: F401
import logging


class EnvSetup:
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
        self.gold_schema = Constants.CATALOG + "." + Constants.GOLD_SCHEMA
        self.config_table = (
            self.dimension_schema + "." + Constants.CONFIG_TABLE
        )
        self.config_file_path = "../data/sample_data/config_data_.json"

    def create_schema(self):
        """
        Create bronze, silver, gold, dimeension schemas if not already exists.
        """
        schemas = [
            self.dimension_schema,
            self.bronze_schema,
            self.silver_schema,
            self.gold_schema,
        ]
        for schema in schemas:
            try:
                self.spark.sql("CREATE SCHEMA IF NOT EXISTS " + schema)
                self.logger.info(f"{schema} schema available to use!")
            except Exception as e:
                self.logger.error(f"Error while creating {schema} schema: {e}")


def main():

    env_setup = EnvSetup()
    env_setup.create_schema()
    # check or create config table and load config data
    env_setup.common_db_functions.create_table(
        env_setup.config_table, env_setup.config_file_path
    )


if __name__ == "__main__":
    main()
