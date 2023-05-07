# Import custom functions
from idealista_scraper_class import IdealistaScraper
from clean_scraped_data import clean_scraped_data
from upload_to_gcp import (
    prepare_parquet_file,
    save_and_upload_to_gcs,
    load_data_from_gcs_to_bigquery,
)

# Built-in imports
import asyncio
from typing import Dict, List, Any
from dataclasses import asdict
import random
import time

# Prefect dependencies
from prefect import flow, task


@task(retries=3, log_prints=True)
async def scrape_search_task(url: str, paginate=True) -> List[str]:
    """Scrape a search page to get property URLs
    Args:
        scraper: An IdealistaScraper instance
        url: The URL of the search page
        paginate: Whether to scrape all pages of the search results (default True)
    Returns:
        A list of property URLs
    """
    async with IdealistaScraper() as scraper:
        return await scraper.scrape_search(url, paginate=paginate)


@task(retries=3, log_prints=True)
async def scrape_properties_task(property_urls: List[str]) -> List[Dict[str, Any]]:
    """Scrape a list of property pages to get property data
    Args:
        scraper: An IdealistaScraper instance
        property_urls: A list of property URLs
    Returns:
        A list of dictionaries representing each property
    """
    async with IdealistaScraper() as scraper:
        scraped_properties = await scraper.scrape_properties(property_urls)

    flattened_properties = [
        scraper.flatten_dict(asdict(item)) for item in scraped_properties
    ]

    return flattened_properties


def chunks(lst: List[str], n: int) -> List[str]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


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
        batch_size: The number of properties to scrape before uploading to GCS (default 30)
        testing: Whether to run the pipeline in testing mode (default False)
    """
    # Get base URL
    base_url = "https://www.idealista.com"
    # Get type of search parameter
    if type_search == "sale":
        type_search_url = "venta-viviendas"
    elif type_search == "rent":
        type_search_url = "alquiler-viviendas"
    elif type_search == "share":
        type_search_url = "alquiler-habitacion"
    # Get province parameter
    if zone:
        location_url = f"{zone.lower()}-{province.lower()}"
    else:
        location_url = f"{province.lower()}-provincia"
    # Generate search URL
    if time_period == "24":
        time_period_mod = "ultimas-24-horas"
    elif time_period == "48":
        time_period_mod = "ultimas-48-horas"
    elif time_period == "week":
        time_period_mod = "ultima-semana"
    elif time_period == "month":
        time_period_mod = "ultimo-mes"
    time_period_url = f"con-publicado_{time_period_mod}"
    # Generate search URL
    url = f"{base_url}/{type_search_url}/{location_url}/{time_period_url}/"
    print(f"Scraping {url}")

    # Produce path to upload in GCS, given search parameters
    if testing:
        to_path = f"testing/{province}/{type_search}/"
    else:
        to_path = f"production/{province}/{type_search}/"

    # Get table id
    if testing:
        table_id = f"{type_search}-{province}-testing"
    else:
        table_id = f"{type_search}-{province}-production"

    # Start scraping with a random wait time to avoid being blocked
    # random_wait_seconds = random.uniform(0, 30) * 60
    # await asyncio.sleep(random_wait_seconds)

    # Time the scraping process
    start_time = time.time()
    max_execution_time = 18 * 60 * 60  # 18 hours

    # Scrape idealista listings given a search URL
    property_urls = await scrape_search_task(url, paginate=not testing)

    # Process property URLs in batches
    processed_properties = 0
    for i, property_urls_batch in enumerate(chunks(property_urls, batch_size)):
        print(f"Processing batch {i}...")
        # Control the execution time
        elapsed_time = time.time() - start_time
        if elapsed_time > max_execution_time:
            print("Max execution time reached. Stopping the scraping process.")
            break

        # Scrape properties for each batch
        property_data = await scrape_properties_task(property_urls_batch)

        # If there is no property data, skip the current batch
        if not property_data:
            continue

        # Clean up scraped data
        cleaned_property_data = await clean_scraped_data(property_data)

        # Upload to GCS
        pa_cleaned_property_data = prepare_parquet_file(cleaned_property_data)
        parquet_file_path = save_and_upload_to_gcs(
            pa_cleaned_property_data, bucket_name, to_path, credentials_path, i
        )

        # Upload to BigQuery
        load_data_from_gcs_to_bigquery(
            bucket_name, parquet_file_path, dataset_id, table_id, credentials_path
        )

        # Update success counter
        processed_properties += len(property_data)
        print(f"Processed {processed_properties} properties")

    # Logging final results
    elapsed_seconds = time.time() - start_time
    elapsed_hours = int(elapsed_seconds // 3600)
    elapsed_minutes = int((elapsed_seconds % 3600) // 60)
    elapsed_seconds = int(elapsed_seconds % 60)

    pcg_properties = (processed_properties / len(property_urls)) * 100
    print(
        f"Scraped {processed_properties}/{len(property_urls)} ({pcg_properties:.2f}% of properties) in {elapsed_hours} hours, {elapsed_minutes} minutes, and {elapsed_seconds} seconds"
    )

    # Debugging one URL
    # url = ['https://www.idealista.com/inmueble/94481996/']
    # scraper = IdealistaScraper()
    # property_data = await scraper.scrape_properties(url)
    # await scraper.session.aclose()


if __name__ == "__main__":
    zone = "madrid"
    province = "madrid"
    type_search = "sale"
    time_period = "24"
    bucket_name = "idealista_data_lake_idealista-scraper-384619"
    dataset_id = "idealista_listings"
    credentials_path = "~/.gcp/terraform.json"
    asyncio.run(
        idealista_to_gcp_pipeline(
            province,
            type_search,
            time_period,
            bucket_name,
            dataset_id,
            credentials_path,
            zone,
            testing=True,
        )
    )
