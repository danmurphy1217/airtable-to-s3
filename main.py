import boto3
import botocore
import requests
from typing import Dict, List, Set
import os
import csv
import datetime
import logging

session = boto3.session.Session(profile_name="pathstream")
aws_s3_client = session.client("s3")

headers = {
    "Authorization": os.environ.get("AIRTABLE_BEARER_KEY"),
    "Accept": "application/json",
}


def retrieve_records_from(
    airtable_base_url: str, headers: Dict[str, int or float or str]
) -> List[Dict[str, int or float or str]]:
    """
    Given a valid Airtable Base URL (`airtable_base_url`), retrieve it's data.

    :param airtable_base_url -> ``str``:  a valid Airtable Base URL.
    :param headers -> ``Dict[str, int or float or str]``: the headers to use for the request.
    :returns -> List[Dict[str, int or float or str]]: a list of rows retrieved from the airtable base.
    """
    response = requests.get(airtable_base_url, headers=headers)

    response.raise_for_status()

    rows = response.json()["records"]  # first 100 rows

    offset = response.json().get("offset", None)

    while offset:  # retrieve all rows
        print(offset)
        response = requests.get(
            airtable_base_url + f"&offset={offset}", headers=headers
        )
        response.raise_for_status()
        rows.extend(response.json()["records"])
        offset = response.json().get("offset", None)

    return rows


def get_columns_from(airtable_data: List[Dict[str, int or float or str]]) -> Set[str]:
    """
    Given a list of dictionaries representing each row in the base (`airtable_data`), build a list of all column names.

    :param airtable_data -> ``List[Dict[str, int or float or str]]``: data returned from airtable
    :returns -> ``Set[str]``: a set containing all column names from the airtable base.
    """
    csv_headers = set()

    for row in airtable_data:
        fields_in_view = set(row["fields"].keys())

        # add columns not in `csv_headers` to `csv_headers`
        columns_not_in_csv_headers = csv_headers.symmetric_difference(
            fields_in_view
        )  # get names not in both `csv_headers` and `fields_in_view`
        csv_headers.update(columns_not_in_csv_headers)

    return csv_headers


def create_csv_from(
    column_names: Set[str], rows: List[Dict[str, int or float or str]], f_name: str
) -> str:
    """
    Create CSV from the provided column names (`column_names`) and rows (`rows`).
    Save the CSV with the provided file name (`f_name`).

    :param column_names -> `Set[str]`: column names for the CSV.
    :param rows -> `List[Dict[int or float or str]]`: a list of rows for the CSV
    :param f_name -> `str`: name for the CSV.
    :returns -> ``str``: the number of rows written to the CSV.
    """
    with open(f_name, "w", newline="") as output:
        dict_writer = csv.DictWriter(output, column_names)
        dict_writer.writeheader()
        dict_writer.writerows([row["fields"] for row in rows])

    output.close()
    return f"Wrote {len(rows)} rows to CSV."


def download_to_csv(tab_name: str) -> None:
    """
    download airtable data from the provided base name (`tab_name`) and save it as a CSV.

    :param tab_name -> ``str``: the name of the Airtable Base
    """
    formatted_tab_name = "%20".join(tab_name.split(" "))
    view_data = retrieve_records_from(
        f"https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/{formatted_tab_name}?view=AWS%20Download",
        headers,
    )

    columns_for_csv = get_columns_from(view_data)

    print(
        create_csv_from(columns_for_csv, view_data, "course_section_enrollments.csv"),
        flush=True,
    )


def upload_to_s3(s3_client: botocore.client, f_path: str, tab_name: str):
    """
    Upload the provided file path (`f_path`) to an S3 bucket.

    :param f_path -> `str`: the file path to upload to S3
    :param key_name -> `str`: the name to save the file as in S3
    """
    try:
        folder_name = (
            "certificate-purchases"
            if tab_name == "Certificate Purchases"
            else "enrollments"
        )
        key_name = f"{datetime.datetime.now().strftime('%d-%B-%Y')}.csv"

        s3_client.upload_file(
            Filename=f_path,
            Bucket="pathstream-airtable-data-transfer-testing",
            Key=f"{folder_name}/{key_name}",
        )
        print(f"Successfully Created: {folder_name}/{key_name}")
    except botocore.exceptions.ClientError as e:
        logging.error(e)


if __name__ == "__main__":
    download_to_csv("Certificate Purchases")  # Download Certificate Purchases Tab
    download_to_csv(
        "Course Section Enrollments"
    )  # Download Course Section Enrollments Tab

    upload_to_s3(
        aws_s3_client, "certificate_purchases.csv", "Certificate Purchases"
    )  # Upload `certificate_purchases.csv` to S3
    upload_to_s3(
        aws_s3_client, "course_section_enrollments.csv", "Course Section Enrollments"
    )  # Upload `course_section_enrollments.csv` to S3
