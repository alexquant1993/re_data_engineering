import os
from datetime import datetime
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from prefect import task


@task(retries=3, log_prints=True)
def save_and_upload_to_gcs(
    table: pa.table,
    bucket_name: str,
    to_path: str,
    credentials_path: str,
    batch_number: int,
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
