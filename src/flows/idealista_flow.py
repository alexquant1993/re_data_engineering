import asyncio
import math
import random
import time

from prefect import flow

from src.data_processing.tasks import clean_scraped_data
from src.flows.utils import chunks
from src.repositories.gcp.bigquery import load_data_from_gcs_to_bigquery
from src.repositories.gcp.parquet import prepare_parquet_file
from src.repositories.gcp.storage import save_and_upload_to_gcs
from src.scrapers.tasks import scrape_properties_task, scrape_search_task


@flow(log_prints=True)
async def idealista_to_gcp_pipeline(
    province: str,
    type_search: str,
    time_period: str,
    bucket_name: str,
    dataset_id: str,
    credentials_path: str,
    zone: str = None,
    batch_size: int = 30,
    max_execution_time: int = 18 * 60 * 60,
    testing: bool = False,
):
    """
    Scrape idealista listings given search parameters and upload to GCS and BigQuery.

    Args:
        province: The province to search in Spain
        type_search: The type of search to perform (sale, rent or share)
        time_period: The time period to search for.
            - '24': last 24 hours
            - '48': last 48 hours
            - 'week': last week
            - 'month': last month

        bucket_name: The name of the GCS bucket to upload the data to
        dataset_id: The name of the BigQuery dataset to upload the data to
        credentials_path: The path to the GCS credentials
        zone: The zone to search in the province. These zones are defined in
        the idealista website. (default None = search in the whole province)
        batch_size: The number of properties to scrape before uploading to GCS
        (default 30)
        max_execution_time: The maximum time to run the pipeline in seconds
        (default 18 hours)
        testing: Whether to run the pipeline in testing mode (default False)
    """
    BASE_URL = "https://www.idealista.com"
    TYPE_SEARCH_URLS = {
        "sale": "venta-viviendas",
        "rent": "alquiler-viviendas",
        "share": "alquiler-habitacion",
    }
    TIME_PERIOD_URLS = {
        "24": "ultimas-24-horas",
        "48": "ultimas-48-horas",
        "week": "ultima-semana",
        "month": "ultimo-mes",
    }

    # Generate search URL
    location_url = (
        f"{zone.lower()}-{province.lower()}"
        if zone
        else f"{province.lower()}-provincia"
    )
    base_url = f"{BASE_URL}/{TYPE_SEARCH_URLS[type_search]}"
    url = f"{base_url}/{location_url}/con-publicado_{TIME_PERIOD_URLS[time_period]}/"
    print(f"Scraping {url}")

    # Produce path and table_id
    environment = "testing" if testing else "production"
    to_path = f"{environment}/{province}/{type_search}/"
    table_id = f"{type_search}-{province}-{environment}"

    # Delay if not in testing mode
    if not testing:
        sleeping_time = random.uniform(0, 30) * 60
        print(f"Random delay: {sleeping_time / 60:.2f} minutes")
        await asyncio.sleep(sleeping_time)

    start_time = time.time()

    # Scrape and process property URLs
    property_urls = await scrape_search_task(url, paginate=not testing)
    n_batches = math.ceil(len(property_urls) / batch_size)
    processed_properties = 0
    for i, property_urls_batch in enumerate(chunks(property_urls, batch_size)):
        if time.time() - start_time > max_execution_time:  # 18 hours
            print("Max execution time reached. Stopping the scraping process.")
            break

        print(f"Processing batch {i + 1} out of {n_batches}")
        property_data = await scrape_properties_task(property_urls_batch)

        if not property_data:
            continue

        try:
            cleaned_property_data = await clean_scraped_data(property_data, type_search)
            pa_cleaned_property_data = prepare_parquet_file(
                cleaned_property_data, type_search
            )
            parquet_file_path = save_and_upload_to_gcs(
                pa_cleaned_property_data, bucket_name, to_path, credentials_path, i
            )
            load_data_from_gcs_to_bigquery(
                bucket_name, parquet_file_path, dataset_id, table_id, credentials_path
            )
        except Exception as e:
            print(f"Error processing batch {i}: {e}")
            break

        processed_properties += len(property_data)
        print(f"Processed {processed_properties} properties")

    elapsed_time = int(time.time() - start_time)
    pcg_properties = (processed_properties / len(property_urls)) * 100
    print(
        f"Scraped {processed_properties}/{len(property_urls)} "
        f"({pcg_properties:.2f}% of properties) in {elapsed_time//3600} hours, "
        f"{(elapsed_time%3600)//60} minutes, and {elapsed_time%60} seconds"
    )


# Debugging one URL
# url = ['https://www.idealista.com/inmueble/94481996/']
# scraper = IdealistaScraper()
# property_data = await scraper.scrape_properties(url)
# await scraper.session.aclose()
