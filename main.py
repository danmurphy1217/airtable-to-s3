import boto3
import botocore
import requests
from typing import Dict, List, Set
import os
import csv

#TODO: annotations for return types

session = boto3.session.Session(profile_name="pathstream")
aws_s3 = session.resource("s3")

headers = {
    "Authorization": os.environ.get("AIRTABLE_BEARER_KEY"),
    "Accept": "application/json",
}


def retrieve_records_from(airtable_base_url: str, headers: Dict[str, int or float or str]):
    """
    Given a valid Airtable Base URL (`airtable_base_url`), retrieve it's data.

    :param airtable_base_url -> ``str``:  a valid Airtable Base URL.
    """
    response = requests.get(airtable_base_url, headers=headers)

    response.raise_for_status()

    rows = response.json()["records"]

    offset = response.json().get("offset", None)

    while offset:
        print(offset)
        response = requests.get(airtable_base_url + f"&offset={offset}", headers=headers)
        response.raise_for_status()
        rows.extend(response.json()['records'])
        offset = response.json().get("offset", None)

    return rows


def get_columns_from(airtable_data: List[Dict[str, int or float or str]]):
    """
    Given a list of dictionary objects, build a list of headers

    :param airtable_data -> ``List[Dict[str, int or float or str]]``: data returned from airtable
    """
    csv_headers = set()

    for row in airtable_data:
        fields_in_view = set(row["fields"].keys())

        # add columns not in `csv_headers` to `csv_headers`
        columns_not_in_csv_headers = csv_headers.symmetric_difference(fields_in_view)
        csv_headers.update(columns_not_in_csv_headers)

    return csv_headers


def create_csv_from(column_names: Set[str], rows: List[Dict[str, int or float or str]], f_name: str):
    """
    Create CSV from the provided column names `column_names` and rows (`rows`).
    Save the CSV with the provided file name (`f_name`).

    :param column_names -> `Set[str]`: column names for the CSV.
    :param rows -> `List[Dict[int or float or str]]`: a list of rows for the CSV
    :param f_name -> `str`: name for the CSV.
    """
    with open(f_name, "w", newline="") as output:
        dict_writer = csv.DictWriter(output, column_names)
        dict_writer.writeheader()
        dict_writer.writerows([row["fields"] for row in rows])
    
    output.close()
    return f"Wrote {len(rows)} rows to CSV."


view_data = retrieve_records_from(
    "https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/Certificate%20Purchases?view=AWS%20Download",
    headers,
)

print(len(view_data))

columns_for_csv = get_columns_from(view_data)

print(create_csv_from(columns_for_csv, view_data, "certificate_purchases.csv"))

# (1) https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/Certificate%20Purchases?view=AWS%20Download
