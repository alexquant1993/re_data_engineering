import asyncio
import random
import httpx
from http import HTTPStatus
from typing import List
from tqdm.asyncio import tqdm_asyncio

from repositories.http.base_http_client import BaseHTTPClient
from repositories.http.random_headers import get_random_header
from parsers.idealista_parser import IdealistaParser
from models.property import Property


class IdealistaScraper:
    """
    Scraper class for fetching and parsing properties from the Idealista website.

    Attributes:
        base_url (str): The base URL for Idealista.
        batch_size (int): Number of requests after which a sleep is initiated to avoid
        rate limiting.
        num_results_page (int): Number of results per search page on Idealista.
        max_pages (int): Maximum number of pages to scrape.
    """

    def __init__(
        self,
        base_url: str = "https://www.idealista.com",
        batch_size: int = 30,
        num_results_page: int = 30,
        max_pages: int = 60,
    ):
        """
        Initialize the IdealistaScraper with the given parameters.

        Args:
            base_url (str): The base URL for Idealista.
            batch_size (int, optional): Number of requests after which a sleep is
            initiated. Defaults to 30.
            num_results_page (int, optional): Number of results per search page.
            Defaults to 30.
            max_pages (int, optional): Maximum number of pages to scrape.
            Defaults to 60.
        """
        self.base_url = base_url
        self.batch_size = batch_size
        self.num_results_page = num_results_page
        self.max_pages = max_pages

    async def __aenter__(self):
        """
        Asynchronous context manager's enter method.
        Initializes the http client with a random header and makes a warm-up request to
        the base URL.
        """
        self.http_client = BaseHTTPClient(self.base_url)
        headers = get_random_header()
        self.http_client.session = httpx.AsyncClient(
            headers=headers, follow_redirects=True, timeout=60
        )
        await self.http_client.request(self.base_url)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Asynchronous context manager's exit method.
        Closes the http client session.
        """
        if self.http_client.session:
            await self.http_client.session.aclose()
            self.http_client.session = None

    async def scrape_properties(self, urls: List[str]) -> List[Property]:
        """
        Scrape property details for a given list of URLs.

        Args:
            urls (List[str]): List of property URLs to scrape.

        Returns:
            List[Property]: List of Property objects with scraped data.
        """
        properties = []
        random.shuffle(urls)
        tasks = [self._fetch_property(url) for url in urls]
        request_counter = 0
        for future in tqdm_asyncio(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Scraping Properties",
            ncols=100,
        ):
            property_data = await future
            if property_data:
                properties.append(property_data)
            # Random sleep time between 2 and 10 seconds to avoid rate limiting
            request_counter += 1
            if request_counter % self.batch_size == 0:
                sleep_time = random.uniform(2, 10)
                print(f"Sleeping for {sleep_time: .2f} seconds to avoid rate limiting")
                await asyncio.sleep(sleep_time)
        return properties

    async def _fetch_property(self, url):
        """
        Fetch and parse a single property page.

        Args:
            url (str): URL of the property to fetch.

        Returns:
            Property: Parsed property data.
        """
        response = await self.http_client.request(url)
        if response is not None and response.status_code == HTTPStatus.OK:
            try:
                parser = IdealistaParser(response)
                return parser.parse_property()
            except Exception as e:
                print(f"Failed to parse property at {url}. Error: {str(e)}")
                return None
        else:
            return None

    async def scrape_search(self, url, paginate=True) -> List[str]:
        """
        Scrape property URLs from search result pages.

        Args:
            url (str): Search result URL to scrape.
            paginate (bool, optional): Whether to scrape all pages of search results
            or just the first one. Defaults to True.

        Returns:
            List[str]: List of scraped property URLs.
        """
        property_urls = []
        first_page = await self.http_client.request(url)
        parser = IdealistaParser(first_page, self.num_results_page)
        property_urls.extend(parser.parse_search())

        if not paginate:
            return property_urls

        total_pages = parser.get_total_pages()
        if total_pages > self.max_pages:
            print(
                f"search contains more than max page limit "
                f"({total_pages}/{self.max_pages})"
            )
            total_pages = self.max_pages

        print(f"scraping {total_pages} pages of search results concurrently")

        tasks = [
            self.http_client.request(f"{first_page.url}pagina-{page}.htm")
            for page in range(2, total_pages + 1)
        ]
        for future in tqdm_asyncio(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Scraping Search Results",
            ncols=100,
        ):
            try:
                response = await future
                parser = IdealistaParser(response)
                urls = parser.parse_search()
                property_urls.extend(urls)
            except Exception as e:
                print(f"Failed to parse search page. Error: {str(e)}")

        # Sleep after scraping pages successfully
        sleep_time = random.uniform(2, 10)
        print(f"Sleeping for {sleep_time: .2f} seconds to avoid rate limiting")
        await asyncio.sleep(sleep_time)

        return property_urls
