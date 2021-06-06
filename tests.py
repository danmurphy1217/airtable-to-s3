import unittest
from main import retrieve_records_from, get_columns_from, create_csv_from, headers
import os
from typing import List, Dict


def retrieve_columns_secondary_strategy(
    airtable_data: List[Dict[str, int or float or str]]
):
    """
    A non-pythonic (but more readable) strategy for retrieving all column
    names (purely for testing purposes).

    :param airtable_data -> ``List[Dict[str, int or float or str]]``: data returned from airtable
    """
    csv_headers = list()

    for row in airtable_data:
        fields_in_view = set(row["fields"].keys())
        for field in fields_in_view:
            if field not in csv_headers:
                csv_headers.append(field)

    return csv_headers


class TestDownloadMethods(unittest.TestCase):
    def test_get_columns_from(self):
        """Test `get_columns_from` method."""
        tab_name = "Certificate Purchases"
        formatted_tab_name = "%20".join(tab_name.split(" "))
        view_data = retrieve_records_from(
            f"https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/{formatted_tab_name}?view=AWS%20Download",
            headers,
        )
        columns_for_csv = get_columns_from(view_data)
        number_of_columns = len(columns_for_csv)
        secondary_strategy_number_of_columns = len(
            retrieve_columns_secondary_strategy(view_data)
        )

        self.assertEqual(secondary_strategy_number_of_columns, number_of_columns)

    def test_create_csv_from(self):
        """Test `create_csv_from` method."""
        create_csv_from(
            ["column one", "column two"],
            [
                {"fields": {"column one": "row one", "column two": "row one"}},
                {"fields": {"column one": "row two", "column two": "row two"}},
            ],
            f_name="test.csv",
        )

        self.assertTrue(os.path.exists("test.csv"))


if __name__ == "__main__":
    unittest.main()
