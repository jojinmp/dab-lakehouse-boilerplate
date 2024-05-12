import unittest
from unittest.mock import patch, MagicMock


class TestGoldLayer(unittest.TestCase):
    def setUp(self):
        # Mock the logger to avoid actual logging during tests
        self.patcher_logger = patch("logging.getLogger")
        self.mock_logger = self.patcher_logger.start()
        self.addCleanup(self.patcher_logger.stop)

        # Mock SparkCommon and CommonFunctions
        self.patcher_spark_common = patch(
            "dab_boilerplate.core.spark_common.SparkCommon", autospec=True
        )
        self.mock_spark_common = self.patcher_spark_common.start()
        self.mock_spark_common.return_value.spark = (
            MagicMock()
        )  # Completely mock Spark session
        self.mock_spark_common.return_value.dbutils = MagicMock()
        self.addCleanup(self.patcher_spark_common.stop)

        self.patcher_common_functions = patch(
            "dab_boilerplate.db.common_functions.CommonFunctions",
            autospec=True,
        )
        self.mock_common_functions = self.patcher_common_functions.start()
        self.addCleanup(self.patcher_common_functions.stop)

    def test_aggregate_and_upsert_data(self):
        from dab_boilerplate.lakehouse.gold.gold_layer import GoldLayer

        gold_layer = GoldLayer()

        # Mock DataFrame operations to simulate the chain of transformations
        mock_df = MagicMock()
        gold_layer.spark.table = MagicMock(return_value=mock_df)
        mock_aggregated_df = MagicMock()
        mock_df.groupBy.return_value.agg.return_value = mock_aggregated_df

        # Perform the aggregation and upsert
        gold_layer.aggregate_and_upsert_data(
            "mock_silver_table", "mock_gold_table"
        )

        # Ensure DataFrame methods are called correctly
        gold_layer.spark.table.assert_called_once_with("mock_silver_table")
        mock_df.groupBy.assert_called_once()
        mock_df.groupBy().agg.assert_called_once()

        # Verify upsert_to_delta_table was called with the correct parameters
        self.mock_common_functions.return_value.upsert_to_delta_table.assert_called_once_with(  # noqa
            mock_aggregated_df,
            "dev.gold.daily_sensor_summary",
            "gold.sensor_id = updates.sensor_id AND gold.date = updates.date",
            "date",
        )


if __name__ == "__main__":
    unittest.main()
