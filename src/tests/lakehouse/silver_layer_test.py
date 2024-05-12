import unittest
from unittest.mock import patch, MagicMock


class TestSilverLayer(unittest.TestCase):
    def setUp(self):
        # Mock logging to avoid any actual logging during tests
        self.patcher_logger = patch("logging.getLogger")
        self.mock_logger = self.patcher_logger.start()
        self.addCleanup(self.patcher_logger.stop)
        # Mock SparkCommon to prevent actual Spark session creation
        self.patcher_spark_common = patch(
            "dab_boilerplate.core.spark_common.SparkCommon", autospec=True
        )
        self.mock_spark_common = self.patcher_spark_common.start()
        self.mock_spark_common.return_value.spark = (
            MagicMock()
        )  # Mock the Spark session
        self.mock_spark_common.return_value.dbutils = MagicMock()
        self.addCleanup(self.patcher_spark_common.stop)

        # Mock CommonFunctions to prevent actual database operations
        self.patcher_common_functions = patch(
            "dab_boilerplate.db.common_functions.CommonFunctions",
            autospec=True,
        )
        self.mock_common_functions = self.patcher_common_functions.start()
        self.addCleanup(self.patcher_common_functions.stop)

    def test_data_transformation_and_upsert(self):
        # Mock the SilverLayer class within its
        # module to test the `main` function
        with patch(
            "dab_boilerplate.lakehouse.silver.silver_layer.SilverLayer"
        ) as MockSilverLayer:
            mock_silver_instance = MockSilverLayer.return_value
            mock_silver_instance.common_db_functions.load_data = MagicMock()
            mock_silver_instance.common_db_functions.upsert_to_delta_table = (
                MagicMock()
            )

            from dab_boilerplate.lakehouse.silver.silver_layer import main

            main()

            # Ensure load_data and upsert_to_delta_table are called correctly
            mock_silver_instance.common_db_functions.load_data.assert_called()
            mock_silver_instance.common_db_functions.upsert_to_delta_table.assert_called()  # noqa: E501

    def test_main_functionality(self):
        from dab_boilerplate.lakehouse.silver.silver_layer import main

        with patch(
            "dab_boilerplate.lakehouse.silver.silver_layer.SilverLayer"
        ) as MockSilverLayer:
            mock_silver_instance = MockSilverLayer.return_value
            mock_silver_instance.measurements_table = "mock_measurements_table"
            # Mocking the DataFrame operations
            # to simulate the chain of transformations
            mock_df = MagicMock()
            mock_silver_instance.common_db_functions.load_data.return_value = (
                mock_df
            )
            mock_df.join.return_value = mock_df
            mock_df.withColumn.return_value = mock_df

            main()

            # Ensure upsert_to_delta_table is called with expected arguments
            mock_silver_instance.common_db_functions.upsert_to_delta_table.assert_called_once_with(  # noqa: E501
                mock_df,
                "mock_measurements_table",
                "target.sensor_id = source.sensor_id AND target.timestamp = source.timestamp",  # noqa: E501
                "sensor_id",
            )


if __name__ == "__main__":
    unittest.main()
