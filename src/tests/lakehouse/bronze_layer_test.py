import unittest
from unittest.mock import patch, MagicMock


class TestBronzeLayer(unittest.TestCase):
    def setUp(self):
        # Mock logging to avoid any actual logging during tests
        patcher_logger = patch("logging.getLogger")
        self.mock_logger = patcher_logger.start()
        self.addCleanup(patcher_logger.stop)

        # Mock SparkCommon to prevent actual Spark session creation
        patcher_spark_common = patch(
            "dab_boilerplate.core.spark_common.SparkCommon", autospec=True
        )
        self.mock_spark_common = patcher_spark_common.start()
        self.mock_spark_common.return_value.spark = MagicMock()
        self.mock_spark_common.return_value.dbutils = MagicMock()
        self.addCleanup(patcher_spark_common.stop)

        # Mock CommonFunctions to prevent actual database operations
        patcher_common_functions = patch(
            "dab_boilerplate.db.common_functions.CommonFunctions",
            autospec=True,
        )
        self.mock_common_functions = patcher_common_functions.start()
        self.addCleanup(patcher_common_functions.stop)

    def test_main(self):
        # This will also test the BronzeLayer instantiation within main
        with patch(
            "dab_boilerplate.lakehouse.bronze.bronze_layer.BronzeLayer"
        ) as MockBronzeLayer:
            mock_bronze_instance = MockBronzeLayer.return_value
            mock_bronze_instance.common_db_functions.create_table = MagicMock()

            # Set the specific return values for
            # data_table and sensor_data_path
            mock_bronze_instance.data_table = "dev.bronze.sensor_data"
            mock_bronze_instance.sensor_data_path = "../data/sample_data/sample_data_2024-03-23 18:05:40.718892.json"  # noqa: E501

            from dab_boilerplate.lakehouse.bronze.bronze_layer import main

            main()

            # Check if create_table was called with correct parameters
            mock_bronze_instance.common_db_functions.create_table.assert_called_once_with(  # noqa: E501
                "dev.bronze.sensor_data",
                "../data/sample_data/sample_data_2024-03-23 18:05:40.718892.json",  # noqa: E501
            )


if __name__ == "__main__":
    unittest.main()
