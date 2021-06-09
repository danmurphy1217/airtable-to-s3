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


product_skus_base = retrieve_records_from(
    f"https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/Product%20SKUs?view=Grid%20view",
    headers,
)
course_section_enrollments_base = retrieve_records_from(
    f"https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/Course%20Section%20Enrollments?view=Grid%20view",
    headers,
)
students_base = retrieve_records_from(
    f"https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/Students?view=Grid%20view",
    headers,
)
certificates_base = retrieve_records_from(
    f"https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/Certificates?view=Grid%20view",
    headers,
)
deferrals_base = retrieve_records_from(
    f"https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/Deferrals?view=Grid%20view",
    headers,
)
#
certificate_purchases_base = retrieve_records_from(
    f"https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/Certificate%20Purchases?view=Grid%20view",
    headers,
)
course_sections_base = retrieve_records_from(
    f"https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/Course%20Sections?view=Grid%20view",
    headers,
)
final_grades_base = retrieve_records_from(
    f"https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/Final%20Grades?view=Grid%20view",
    headers,
)
instructors_base = retrieve_records_from(
    f"https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/Instructors?view=Grid%20view",
    headers,
)
course_sections_testing_base = retrieve_records_from(
    f"https://api.airtable.com/v0/appBRLEUdTlfhgkUZ/%28DoNotUse%29%20Course%20Sections%20Testing?view=Grid%20view",
    headers,
)


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


def retrieve_value_for(reference_base: str, reference_column: str, row_ids: List[str]):
    """
    Given a reference base (`reference_base`) and a list of reference id(s) (`row_ids`),

    :param reference_base -> ``str``: the base that a field is referencing.
    :param reference_column -> ``str``: the column to retrieve from the referenced base.
    :param row_ids -> ``List[str]``: the row IDs to retrieve data for.

    :returns -> ``Dict[str, str]``: a dictionary containing row id - converted value (key - value) pairs.
    """
    result = list()

    for row in reference_base:
        if row["id"] in row_ids:
            result.append(str(row["fields"].get(reference_column, None)))

    return ", ".join(result)


def clean_data(
    airtable_data: List[Dict[str, int or float or str]],
    column_names: Set[str],
    tab_name: str,
) -> List[Dict[str, int or float or str]]:
    """
    Take in messy airtable data (`airtable_data`), clean it (convert lists to strings, replace reference IDs with
    actual values), and return the cleaned version.

    :param airtable_data -> ``List[Dict[str, int or float or str]]``: data returned from airtable
    :param column_names -> ``Set[str]`` a set containing all column names for the data.
    :param tab_name -> ``str``: the name of the tab

    :returns ``List[Dict[str, int or float or str]]``: cleaned airtable data.
    """
    cleaned_data = []

    columns_for_lookup = (
        ["Product SKU", "Section Enrollment List", "Email", "Cert ID", "Deferrals"]
        if tab_name == "Certificate Purchases"
        else [
            "Cert Purchase #",
            "Email",
            "Course Section Final Name",
            "Brightspace Name - Email - Link",  # FIX
            "Certificate Purchases List (from Email)",
            "Certificate ID",
            "Instructor Email",
            "Course Sections copy",
            "Deferrals",
            "Deferrals 2",

        ]
    )

    for row in airtable_data:
        print(row)
        cleaned_row = dict()
        cleaned_row["fields"] = dict()

        for column in column_names:
            if row["fields"].get(column, None):

                if isinstance(row["fields"][column], list):

                    value = row["fields"][column]

                    if row["fields"][column][0] is None:
                        value = ["None"]

                    elif isinstance(row["fields"][column][0], (int, float)):
                        value = [str(value) for value in row["fields"][column]]

                    cleaned_row["fields"][column] = ", ".join(value)

                if column in columns_for_lookup:
                    reference_base = ""
                    reference_column = ""
                    if tab_name == "Certificate Purchases":
                        if column == "Product SKU":
                            reference_base = product_skus_base
                            reference_column = column
                        elif column == "Section Enrollment List":
                            reference_base = course_section_enrollments_base
                            reference_column = "Enrollment ID"
                        elif column == "Email":
                            reference_base = students_base
                            reference_column = column
                        elif column == "Deferrals":
                            reference_base = deferrals_base
                            reference_column = "ID"
                        elif column == "Cert ID":
                            reference_base = certificates_base
                            reference_column = "Certificate ID"
                        else:
                            raise ValueError(
                                f"Column {column} not currently supported."
                            )

                        cleaned_row["fields"][column] = retrieve_value_for(
                            reference_base, reference_column, value
                        )

                    elif tab_name == "Course Section Enrollments":
                        if column == "Cert Purchase #":
                            reference_base = certificate_purchases_base
                            reference_column = column
                        elif column == "Email":
                            reference_base = students_base
                            reference_column = column
                        elif column == "Course Section Final Name":
                            reference_base = course_sections_base
                            reference_column = column
                        elif column == "Certificate ID":
                            reference_base = certificates_base
                            reference_column = "Certificate ID"
                        elif column == "Brightspace Name - Email - Link":
                            reference_base = course_section_enrollments_base
                            reference_column = "Brightspace Name - Email - Link"
                        elif column == "Certificate Purchases List (from Email)":
                            reference_base = product_skus_base
                            reference_column = "Product SKU"
                        elif column == "Instructor Email":
                            reference_base = instructors_base
                            reference_column = "Email"
                        elif column == "Course Sections copy":
                            reference_base = course_sections_testing_base
                            reference_column = "Course Section Final Name"
                        elif column == "Deferrals":
                            reference_base = deferrals_base
                            reference_column = "ID"
                        elif column == "Deferrals 2":
                            reference_base = deferrals_base
                            reference_column = "ID"
                        else:
                            raise ValueError(
                                f"Column {column} not currently supported."
                            )

                        cleaned_row["fields"][column] = retrieve_value_for(
                            reference_base, reference_column, value
                        )

        cleaned_data.append(cleaned_row)

    assert len(cleaned_data) == len(airtable_data), "Length Mismatch"
    return cleaned_data


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
    cleaned_data = clean_data(view_data, columns_for_csv, tab_name)

    print(
        create_csv_from(
            columns_for_csv, cleaned_data, f"{'_'.join(tab_name.split(' '))}.csv"
        ),
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

    # upload_to_s3(
    #     aws_s3_client, "certificate_purchases.csv", "Certificate Purchases"
    # )  # Upload `certificate_purchases.csv` to S3
    # upload_to_s3(
    #     aws_s3_client, "course_section_enrollments.csv", "Course Section Enrollments"
    # )  # Upload `course_section_enrollments.csv` to S3
