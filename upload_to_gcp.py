# Built in imports
import os
from datetime import datetime
import tempfile

# Third party imports
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Prefect dependencies
from prefect import task


@task(retries=3, log_prints=True)
def prepare_parquet_file(df: pd.DataFrame) -> pa.Table:
    """Prepare a Pandas DataFrame for writing to Parquet"""
    schema_fields = [
        ("ID_LISTING", pa.string()),
        ("URL", pa.string()),
        ("TYPE_PROPERTY", pa.string()),
        # Geolocation fields
        ("ADDRESS", pa.string()),
        ("LOCATION", pa.string()),
        ("FULL_ADDRESS", pa.string()),
        ("ZIP_CODE", pa.string()),
        ("LATITUDE", pa.float32()),
        ("LONGITUDE", pa.float32()),
        ("IMPORTANCE_LOCATION", pa.float32()),
        ("LOCATION_ID", pa.int32()),
        # Price and description fields
        ("PRICE", pa.float32()),
        ("ORIGINAL_PRICE", pa.float32()),
        ("CURRENCY", pa.string()),
        ("TAGS", pa.string()),
        ("LISTING_DESCRIPTION", pa.string()),
        # Poster details
        ("POSTER_TYPE", pa.string()),
        ("POSTER_NAME", pa.string()),
        # Main property details
        ("BUILT_AREA", pa.float32()),
        ("USEFUL_AREA", pa.float32()),
        ("LOT_AREA", pa.float32()),
        ("NUM_BEDROOMS", pa.int32()),
        ("NUM_BATHROOMS", pa.int32()),
        ("CONDITION", pa.string()),
        # Other basic property features
        ("AIR_CONDITIONING", pa.bool_()),
        ("HEATING", pa.string()),
        ("BUILTIN_WARDROBE", pa.bool_()),
        ("ELEVATOR", pa.bool_()),
        ("PROPERTY_ORIENTATION", pa.string()),
        ("FLAG_PARKING", pa.bool_()),
        ("PARKING_INCLUDED", pa.bool_()),
        ("PARKING_PRICE", pa.float32()),
        ("GREEN_AREAS", pa.bool_()),
        ("POOL", pa.bool_()),
        ("TERRACE", pa.bool_()),
        ("STORAGE_ROOM", pa.bool_()),
        ("BALCONY", pa.bool_()),
        # Building features
        ("CARDINAL_ORIENTATION", pa.string()),
        ("ACCESIBILITY_FLAG", pa.bool_()),
        ("YEAR_BUILT", pa.int32()),
        ("NUM_FLOORS", pa.int32()),  # For houses
        ("FLOOR", pa.float32()),  # For apartments
        # Energy performance certificate details
        ("STATUS_EPC", pa.string()),
        ("ENERGY_CONSUMPTION_LABEL", pa.string()),
        ("ENERGY_EMISSIONS_LABEL", pa.string()),
        ("ENERGY_CONSUMPTION", pa.float32()),
        ("ENERGY_EMISSIONS", pa.float32()),
        # Time related fields
        ("LAST_UPDATE_DATE", pa.date32()),
        ("TIMESTAMP", pa.timestamp("s")),
    ]

    # Check for missing columns and create empty ones with the appropriate data type
    for field_name, field_type in schema_fields:
        if field_name not in df.columns:
            df[field_name] = pd.Series(dtype=field_type.to_pandas_dtype())

    # Create the Arrow schema and table
    schema = pa.schema(schema_fields)
    table = pa.Table.from_pandas(df, schema=schema)

    return table


@task(retries=3, log_prints=True)
def save_and_upload_to_gcs(
    table: pa.table, bucket_name: str, to_path: str, credentials_path: str,
    batch_number: int
):
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
def load_data_from_gcs_to_bigquery(
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
