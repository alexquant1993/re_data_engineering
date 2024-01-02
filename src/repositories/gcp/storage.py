import os
import tempfile
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from prefect import task


def _save_and_upload_to_gcs(
    table: pa.table,
    bucket_name: str,
    to_path: str,
    credentials_path: str,
    batch_number: int,
):
    """
    Save a PyArrow table as a Parquet file and upload it to a GCS bucket.

    Args:
        table (pa.Table): The PyArrow table to save and upload.
        bucket_name (str): The name of the GCS bucket to upload to.
        to_path (str): The path in the GCS bucket to upload the file to.
        credentials_path (str): The path to the GCP credentials file.
        batch_number (int): The batch number to append to the file name.

    Returns:
        str: The full path of the uploaded file in the GCS bucket.
    """
    # Save the pyarrow Table as a Parquet file
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
        pq.write_table(table, temp_file.name, compression="snappy")
        temp_file_path = temp_file.name

    # Explicitly close the temporary file
    temp_file.close()

    try:
        today = datetime.today().strftime("%Y-%m-%d")
        # Upload the Parquet file to GCS
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.expanduser(
            credentials_path
        )
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        full_path = os.path.join(to_path, f"{today}_{batch_number}.parquet")
        blob = bucket.blob(full_path)
        blob.upload_from_filename(temp_file_path)
        print(f"File successfully uploaded to GCS: {full_path}.")

    finally:
        os.remove(temp_file_path)

    return full_path


@task(retries=3, log_prints=True)
def save_and_upload_to_gcs(
    table: pa.table,
    bucket_name: str,
    to_path: str,
    credentials_path: str,
    batch_number: int,
):
    """
    Task to save a PyArrow table as a Parquet file and upload it to a GCS bucket.

    This function is a Prefect task that wraps the private function _save_and_upload_to_gcs.
    It is designed to be used in a Prefect flow and will retry 3 times if it fails.

    Args:
        table (pa.Table): The PyArrow table to save and upload.
        bucket_name (str): The name of the GCS bucket to upload to.
        to_path (str): The path in the GCS bucket to upload the file to.
        credentials_path (str): The path to the GCP credentials file.
        batch_number (int): The batch number to append to the file name.

    Returns:
        str: The full path of the uploaded file in the GCS bucket.
    """
    return _save_and_upload_to_gcs(
        table, bucket_name, to_path, credentials_path, batch_number
    )
