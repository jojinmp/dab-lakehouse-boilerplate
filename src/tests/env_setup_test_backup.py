import unittest
from unittest.mock import MagicMock, call
from dab_boilerplate.env_setup.env_setup import (
    EnvSetup,
)


class TestCreateSchema(unittest.TestCase):
    def setUp(self):
        self.instance = EnvSetup()
        self.instance.spark = MagicMock()
        self.instance.logger = MagicMock()

        self.instance.dimension_schema = "dimension"
        self.instance.bronze_schema = "bronze"
        self.instance.silver_schema = "silver"
        self.instance.gold_schema = "gold"

    def test_create_schema_success(self):
        # Mock spark.sql to simulate successful schema creation
        self.instance.spark.sql = MagicMock()
        self.instance.create_schema()

        # Assert CREATE SCHEMA IF NOT EXISTS
        # is called correctly for each schema
        expected_calls = [
            call("CREATE SCHEMA IF NOT EXISTS dimension"),
            call("CREATE SCHEMA IF NOT EXISTS bronze"),
            call("CREATE SCHEMA IF NOT EXISTS silver"),
            call("CREATE SCHEMA IF NOT EXISTS gold"),
        ]
        self.instance.spark.sql.assert_has_calls(
            expected_calls, any_order=True
        )

        # Assert logging info for each schema
        info_calls = [
            call("dimension schema available to use!"),
            call("bronze schema available to use!"),
            call("silver schema available to use!"),
            call("gold schema available to use!"),
        ]
        self.instance.logger.info.assert_has_calls(info_calls, any_order=True)

    def test_create_schema_failure(self):
        # Setup spark.sql to throw an exception for each schema
        error_messages = {
            "dimension": "Failed to create dimension schema",
            "bronze": "Failed to create bronze schema",
            "silver": "Failed to create silver schema",
            "gold": "Failed to create gold schema",
        }
        self.instance.spark.sql.side_effect = [
            Exception(error_messages[schema])
            for schema in ["dimension", "bronze", "silver", "gold"]
        ]

        # Call the method
        self.instance.create_schema()

        # Assert error logging for each schema
        expected_calls = [
            call(
                "Error while creating dimension schema: "
                + f"{error_messages['dimension']}"
            ),
            call(
                "Error while creating bronze schema: "
                f"{error_messages['bronze']}"
            ),
            call(
                "Error while creating silver schema: "
                + f"{error_messages['silver']}"
            ),
            call(
                "Error while creating gold schema: "
                + f"{error_messages['gold']}"
            ),
        ]
        self.instance.logger.error.assert_has_calls(
            expected_calls, any_order=True
        )


if __name__ == "__main__":
    unittest.main()
