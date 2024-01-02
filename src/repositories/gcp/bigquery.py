import os

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from prefect import task


def _load_data_from_gcs_to_bigquery(
    bucket_name, parquet_file_path, dataset_id, table_id, credentials_path
):
    """
    Load data from a GCS Parquet file into a BigQuery table.

    Args:
        bucket_name: The name of the GCS bucket containing the Parquet file.
        parquet_file_path: The path to the Parquet file within the GCS bucket.
        dataset_id: The ID of the BigQuery dataset.
        table_id: The ID of the BigQuery table.
        credentials_path: The path to the GCP credentials file.
    """

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.expanduser(credentials_path)

    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    # Check if the table exists, and create it if it doesn't
    try:
        client.get_table(table_ref)
    except NotFound:
        print(f"Creating table {table_id} in dataset {dataset_id}.")
        table = bigquery.Table(table_ref)
        client.create_table(table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    uri = f"gs://{bucket_name}/{parquet_file_path}"
    table_ref = client.dataset(dataset_id).table(table_id)

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()

    print(f"Loaded {load_job.output_rows} rows to {dataset_id}.{table_id}.")


@task(retries=3, log_prints=True)
def load_data_from_gcs_to_bigquery(
    bucket_name, parquet_file_path, dataset_id, table_id, credentials_path
):
    """
    Task to load data from a GCS Parquet file into a BigQuery table.

    This function is a Prefect task that wraps the private function _load_data_from_gcs_to_bigquery.
    It is designed to be used in a Prefect flow and will retry 3 times if it fails.

    Args:
        bucket_name (str): The name of the GCS bucket containing the Parquet file.
        parquet_file_path (str): The path to the Parquet file within the GCS bucket.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): The ID of the BigQuery table.
        credentials_path (str): The path to the GCP credentials file.
    """
    return _load_data_from_gcs_to_bigquery(
        bucket_name, parquet_file_path, dataset_id, table_id, credentials_path
    )
