import asyncio
from dataclasses import asdict

from src.data_processing.tasks import _clean_scraped_data
from src.repositories.gcp.bigquery import _load_data_from_gcs_to_bigquery
from src.repositories.gcp.parquet import _prepare_parquet_file
from src.repositories.gcp.storage import _save_and_upload_to_gcs
from src.scrapers.idealista_scraper import IdealistaScraper
from src.scrapers.utils import flatten_dict


async def debug_properties(
    urls, type_search, bucket_name, dataset_id, credentials_path
):
    to_path = f"testing/madrid/{type_search}/"
    table_id = f"{type_search}-madrid-testing"

    try:
        async with IdealistaScraper() as scraper:
            scraped_property = await scraper.scrape_properties(urls)
        property_data = [flatten_dict(asdict(item)) for item in scraped_property]
        cleaned_property_data = await _clean_scraped_data(property_data, type_search)
        pa_cleaned_property_data = _prepare_parquet_file(
            cleaned_property_data, type_search
        )
        parquet_file_path = _save_and_upload_to_gcs(
            pa_cleaned_property_data, bucket_name, to_path, credentials_path, 1
        )
        _load_data_from_gcs_to_bigquery(
            bucket_name, parquet_file_path, dataset_id, table_id, credentials_path
        )
    except Exception as e:
        print(f"Error processing url: {e}")


if __name__ == "__main__":
    urls = [
        "https://www.idealista.com/inmueble/103564458/",
        "https://www.idealista.com/inmueble/103564569/",
        "https://www.idealista.com/inmueble/103565394/",
    ]
    type_search = "sale"
    bucket_name = "idealista_data_lake_idealista-scraper-384619"
    dataset_id = "idealista_listings"
    credentials_path = "~/.gcp/prefect-agent.json"
    asyncio.run(
        debug_properties(urls, type_search, bucket_name, dataset_id, credentials_path)
    )
