from typing import Any, Dict, List
from prefect import task
from dataclasses import asdict

from scrapers.idealista_scraper import IdealistaScraper
from scrapers.utils import flatten_dict


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

    flattened_properties = [flatten_dict(asdict(item)) for item in scraped_properties]

    return flattened_properties
