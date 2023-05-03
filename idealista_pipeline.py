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


@flow(log_prints=True)
async def idealista_to_gcp_pipeline(
    province: str,
    type_search: str,
    time_period: str,
    bucket_name: str,
    dataset_id: str,
    table_id: str,
    credentials_path: str,
    zone: str = None,
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
        table_id: The name of the BigQuery table to upload the data to
        credentials_path: The path to the GCS credentials
        zone: The zone to search in the province. These zones are defined in
        the idealista website. (default None = search in the whole province)
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

    # Start scraping with a random wait time to avoid being blocked
    # random_wait_seconds = random.uniform(0, 30) * 60
    # await asyncio.sleep(random_wait_seconds)
    # Scrape idealista listings given a search URL
    property_urls = await scrape_search_task(url, paginate=not testing)
    property_data = await scrape_properties_task(property_urls)

    # Clean up scraped data
    cleaned_property_data = await clean_scraped_data(property_data)

    # Upload to GCS
    # Produce to path given search parameters
    if testing:
        to_path = f"testing/{province}/{type_search}/"
    else:
        to_path = f"production/{province}/{type_search}/"
    pa_cleaned_property_data = prepare_parquet_file(cleaned_property_data)
    parquet_file_path = save_and_upload_to_gcs(
        pa_cleaned_property_data, bucket_name, to_path, credentials_path
    )

    # Upload to BigQuery
    load_data_from_gcs_to_bigquery(
        bucket_name, parquet_file_path, dataset_id, table_id, credentials_path
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
    table_id = f"{type_search}-{province}"
    credentials_path = "~/.gcp/terraform.json"
    asyncio.run(
        idealista_to_gcp_pipeline(
            province,
            type_search,
            time_period,
            bucket_name,
            dataset_id,
            table_id,
            credentials_path,
            zone,
            testing=True,
        )
    )
