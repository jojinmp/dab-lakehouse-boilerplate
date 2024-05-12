from dab_boilerplate.core.spark_common import SparkCommon
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
import logging
import os
import pandas as pd


class CommonFunctions:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spark_common = SparkCommon(app_name="boilerplate")
        self.spark = self.spark_common.spark
        self.dbutils = self.spark_common.dbutils

    def create_table_from_file(self, file_path, table_name):
        """Create delta table from a file.

        Args:
            file_path (str): Path to the file.
            table_name (str): Delta table name.
        """
        absolute_file_path = os.path.join(os.path.dirname(__file__), file_path)
        absolute_file_path = os.path.abspath(absolute_file_path)
        # Read the JSON file into a Pandas DataFrame
        pandas_conf_df = pd.read_json(absolute_file_path)
        conf_df = self.spark.createDataFrame(pandas_conf_df)
        conf_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    def load_data(self, table_name: str) -> DataFrame:
        """
        Load data from a Delta table.

        Args:
        table_name (str): The table name to load data from.

        Returns:
        DataFrame: The loaded Spark DataFrame.
        """
        return self.spark.table(table_name)

    def create_table(self, table_name, file_path):
        """Validate existance of table and create as required.

        Args:
            table_name (str): Delta table name.
            file_path (str): Path to the data file
        """
        # Check if the table exists
        if self.spark.catalog.tableExists(table_name):
            self.logger.info(f"{table_name} table exists.")
        else:
            self.logger.info(f"{table_name} doesn't exist, creating...")
            # Path to the JSON file
            try:
                self.create_table_from_file(file_path, table_name)
                self.logger.info(f"{table_name} created.")
            except Exception as e:
                self.logger.error(
                    f"Error while creating {table_name} table: {e}"
                )

    def upsert_to_delta_table(
        self,
        source_df: DataFrame,
        target_table_name: str,
        match_condition: str,
        partition_key: str,
    ):
        """
        Perform an upsert operation to a Delta table using the given DataFrame.

        Args:
        df (DataFrame): DataFrame containing new data to be upserted.
        target_table_name (str): The Delta table name to upsert into.
        match_condition (str): The condition to match for upsert operation.

        """
        if self.spark.catalog.tableExists(target_table_name):
            # Load the target Delta table
            deltaTable = DeltaTable.forName(self.spark, target_table_name)

            # Perform the upsert operation
            (
                deltaTable.alias("target")
                .merge(
                    source=source_df.alias("source"), condition=match_condition
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            self.logger.info(f"{target_table_name} updated with new data.")

        else:
            # If the target table does not exist,
            # create it by writing the DataFrame as a new Delta table
            source_df.write.format("delta").partitionBy(partition_key).mode(
                "overwrite"
            ).saveAsTable(target_table_name)
            self.logger.info(f"Created {target_table_name} and loaded data.")
