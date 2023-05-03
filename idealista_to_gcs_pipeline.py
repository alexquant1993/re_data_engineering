# Import custom functions
from idealista_scraper_class import IdealistaScraper
from clean_scraped_data import clean_scraped_data
from upload_to_gcs import prepare_parquet_file, save_and_upload_to_gcs

# Built-in imports
import asyncio
from typing import Dict, List, Any
from dataclasses import asdict

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
async def idealista_to_gcs_pipeline(
    province: str,
    type_search: str,
    time_period: int,
    bucket_name: str,
    credentials_path: str,
    testing: bool = False,
):
    """
    Scrape idealista listings given search parameters and upload to GCS
    Args:
        province: The province to search in Spain
        type_search: The type of search to perform (sale, rent or share)
        time_period: The time period to search for (in hours). Last 24 hours, 48 hours, etc.
        bucket_name: The name of the GCS bucket to upload the data to
        credentials_path: The path to the GCS credentials
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
    province_url = f"{province.lower()}-provincia"
    # Generate search URL
    time_period_url = f"con-publicado_ultimas-{time_period}-horas"
    # Generate search URL
    url = f"{base_url}/{type_search_url}/{province_url}/{time_period_url}/"

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
    save_and_upload_to_gcs(
        pa_cleaned_property_data, bucket_name, to_path, credentials_path
    )

    # Debugging one URL
    # url = ['https://www.idealista.com/inmueble/94481996/']
    # scraper = IdealistaScraper()
    # property_data = await scraper.scrape_properties(url)
    # await scraper.session.aclose()


if __name__ == "__main__":
    province = "madrid"
    type_search = "sale"
    time_period = 24
    bucket_name = "idealista_data_lake_idealista-scraper-384619"
    credentials_path = "~/.gcp/terraform.json"
    asyncio.run(
        idealista_to_gcs_pipeline(
            province,
            type_search,
            time_period,
            bucket_name,
            credentials_path,
            testing=True,
        )
    )
