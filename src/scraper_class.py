# Built-in imports
import asyncio
import json
import re
import math
import random
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from collections import defaultdict
from urllib.parse import urljoin
from dataclasses import dataclass
from http import HTTPStatus

# Import third-party libraries
import httpx
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm_asyncio
from latest_user_agents import get_latest_user_agents
import httpagentparser


@dataclass
class PropertyResult:
    """A dataclass representing the result of scraping an Idealista.com property page"""

    url: str
    title: str
    location: str
    price: int
    original_price: Optional[int]
    tags: Optional[List[str]]
    currency: str
    description: str
    poster_type: str
    poster_name: str
    features: Dict[str, List[str]]
    images: Dict[str, List[str]]
    plans: List[str]
    updated: str
    time_stamp: str


class IdealistaScraper:
    """
    A class for scraping property data from Idealista.com
    """

    def __init__(self):
        # Parameters for the scraper
        self.CONCURRENT_REQUESTS_LIMIT = 1
        self.NUM_RESULTS_PAGE = 30
        self.MAX_PAGES = 60
        self.SEMAPHORE = asyncio.Semaphore(self.CONCURRENT_REQUESTS_LIMIT)

        # Parameters for the exponential backoff algorithm
        self.MAX_RETRIES = 3
        self.INITIAL_BACKOFF = 32
        self.MAX_BACKOFF = 64

        # Parameters for the random sleep interval
        self.MIN_SLEEP_INTERVAL = 25
        self.MAX_SLEEP_INTERVAL = 35

        # Token bucket algorithm parameters
        self.token_bucket = TokenBucket(tokens=1, fill_rate=1 / 27)

        # Default parameters for the search
        self.session = None
        self.base_url = "https://www.idealista.com"
        self.last_successful_url = None

    async def __aenter__(self):
        headers = self.get_random_header()
        self.session = httpx.AsyncClient(
            headers=headers, follow_redirects=True, timeout=60
        )
        # Make warm-up requests to mimic a real user
        await self.make_request(self.base_url)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session is not None:
            await self.session.aclose()
            self.session = None

    async def make_request(self, url: str):
        """
        Make an HTTP request to a URL

        Args:
            url: The URL to make a request to

        Returns:
        The response object from the request, or None if the request failed
        """
        async with self.SEMAPHORE:
            retry_count = 0
            while retry_count <= self.MAX_RETRIES:
                try:
                    # Wait for a token to be available
                    while not self.token_bucket.consume(1):
                        await asyncio.sleep(random.uniform(1, 5))

                    # Update the referer header with the last successful URL if available
                    if self.last_successful_url:
                        self.session.headers["referer"] = self.last_successful_url

                    # Make the request
                    response = await self.session.get(url)

                    if response.status_code == HTTPStatus.OK:
                        self.last_successful_url = url
                        print(f"Successful request: {url}")
                        return response

                    elif response.status_code in (
                        HTTPStatus.FORBIDDEN,
                        HTTPStatus.TOO_MANY_REQUESTS,
                        HTTPStatus.SERVICE_UNAVAILABLE,
                    ):
                        await self.handle_rate_limit(response, retry_count)
                        retry_count += 1
                        print(response.headers)

                    else:
                        # Failed request, retry
                        if retry_count < self.MAX_RETRIES:
                            sleep_duration = self.exponential_backoff_with_jitter(
                                retry_count
                            )
                            print(
                                f"HTTP {response.status_code} - Retrying in {sleep_duration} seconds: {url}"
                            )
                            await asyncio.sleep(sleep_duration)
                            retry_count += 1
                        else:
                            print(
                                f"Failed to scrape URL after {self.MAX_RETRIES} retries: {url}"
                            )
                            return None

                except (httpx.RequestError, asyncio.TimeoutError):
                    if retry_count < self.MAX_RETRIES:
                        sleep_duration = self.exponential_backoff_with_jitter(
                            retry_count
                        )
                        print(
                            f"Request error - Retrying in {sleep_duration} seconds: {url}"
                        )
                        await asyncio.sleep(sleep_duration)
                        retry_count += 1
                    else:
                        print(
                            f"Failed to scrape URL after {self.MAX_RETRIES} retries: {url}"
                        )
                        return None

    def get_random_header(self):
        """
        Get a random header from the latest user agents. The headers are chosen among the following:
        - Windows + Chrome. Weight: 0.64
        - Windows + Firefox. Weight: 0.1
        - MacOS + Chrome. Weight: 0.1
        - MacOS + Safari. Weight: 0.13
        - MacOS + Firefox. Weight: 0.03
        """
        # Get latest user agents
        latest_user_agents = get_latest_user_agents()

        # Classify the user agents by OS and browser
        agents_dict = {}
        for user_agent in latest_user_agents:
            parsed_agent = httpagentparser.detect(user_agent)
            os = parsed_agent["platform"]["name"]
            browser = parsed_agent["browser"]["name"]
            agents_dict[f"{os}-{browser}"] = user_agent

        # Create a dictionary of headers for each OS and browser
        # Windows + Chrome
        headers_windows_chrome = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "es-ES,es;q=0.9",
            "cache-control": "max-age=0",
            "referer": "https://www.google.es/",
            "upgrade-insecure-requests": "1",
            "user-agent": agents_dict["Windows-Chrome"],
        }
        # Windows + Firefox
        headers_windows_firefox = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
            "referer": "https://www.google.es/",
            "upgrade-insecure-requests": "1",
            "user-agent": agents_dict["Windows-Firefox"],
        }
        # macOS + Chrome
        headers_macos_chrome = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "es-ES,es;q=0.9",
            "cache-control": "max-age=0",
            "referer": "https://www.google.es/",
            "upgrade-insecure-requests": "1",
            "user-agent": agents_dict["Mac OS-Chrome"],
        }
        # macOS + Safari
        headers_macos_safari = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,/;q=0.8",
            "accept-language": "es-ES,es;q=0.9",
            "user-agent": agents_dict["Mac OS-Safari"],
            "referer": "https://www.google.es/",
            "accept-encoding": "gzip, deflate, br",
        }
        # macOS + Firefox
        headers_macos_firefox = {
            "user-agent": agents_dict["Mac OS-Firefox"],
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,/;q=0.8",
            "accept-language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
            "accept-encoding": "gzip, deflate, br",
            "referer": "https://www.google.es/",
            "dnt": "1",
            "upgrade-insecure-requests": "1",
        }

        # Get weights given the distribution of usage for each OS and browser
        weights = [0.64, 0.1, 0.1, 0.13, 0.03]

        return random.choices(
            [
                headers_windows_chrome,
                headers_windows_firefox,
                headers_macos_chrome,
                headers_macos_safari,
                headers_macos_firefox,
            ],
            weights=weights,
        )[0]

    async def handle_rate_limit(
        self, response: httpx.Response, retry_count: int
    ) -> None:
        """
        Handle rate-limited requests.
        On the first encounter of the rate limit, the function will sleep for 12 hours,
        and on subsequent encounters, a RateLimitException will be raised.

        Args:
            response (httpx.Response): The response object from the rate-limited request
            retry_count (int): The number of times the rate limit has been encountered

        Raises:
            RateLimitException: If the rate limit is encountered more than once
        """
        if retry_count == 0:
            sleep_duration = 12 * 60 * 60  # Sleep for 12 hours
        else:
            raise RateLimitException(
                f"HTTP {response.status_code} - Rate limit reached. URL: {response.url}"
            )

        print(
            f"HTTP {response.status_code} - Retrying in {sleep_duration} seconds: {response.url}"
        )
        await asyncio.sleep(sleep_duration)

    def parse_property(self, response: httpx.Response) -> PropertyResult:
        """
        Parse an Idealista.com property page

        Args:
            response: The HTTP response object from the property page request

        Returns:
            A PropertyResult object representing the parsed data
        """
        # Parse response
        soup = BeautifulSoup(response.text, "html.parser")

        # Get original price, before discount, if available
        original_price_element = soup.select_one(".pricedown_price span")
        original_price = None
        if original_price_element:
            original_price = int(
                original_price_element.text.strip().replace(".", "").replace(",", "")
            )

        # Get tags
        tags = None
        if soup.select_one(".detail-info-tags"):
            tags = soup.select_one(".detail-info-tags").text.split()

        # Get poster details
        # If the poster is not a particular/professional, then try with a bank class
        check_professional = soup.select_one(
            ".advertiser-name-container .about-advertiser-name"
        )
        if check_professional:
            poster_type = "Profesional"
            poster_name = check_professional.text.strip()
        else:
            poster_type = "Particular"
            poster_name = soup.select_one(".professional-name span").text.strip()

        # Get image data
        image_data = self.get_image_data(soup)

        # Create PropertyResult object
        property_result = PropertyResult(
            url=str(response.url),
            title=soup.select_one(".main-info__title-main").text.strip(),
            location=soup.select_one(".main-info__title-minor").text.strip(),
            currency=soup.select_one(".info-data-price").contents[-1].strip(),
            price=int(
                soup.select_one(".info-data-price span")
                .text.replace(".", "")
                .replace(",", "")
            ),
            original_price=original_price,
            tags=tags,
            description="\n".join(
                [p.text.strip() for p in soup.select("div.comment p")]
            ),
            poster_type=poster_type,
            poster_name=poster_name,
            features=self.get_features(soup),
            images=self.get_images(image_data),
            plans=self.get_plans(image_data),
            updated=soup.select_one(
                'p.stats-text:-soup-contains("actualizado el")'
            ).text.split(" el ")[-1],
            time_stamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

        return property_result

    async def scrape_properties(self, urls: List[str]) -> List[PropertyResult]:
        """
        Scrape Idealista.com properties

        Args:
            urls: A list of property URLs to scrape

        Returns:
            A list of PropertyResult objects representing the scraped data
        """
        random.shuffle(urls)
        properties = []
        to_scrape = [self.make_request(url) for url in urls]
        counter = 0

        for response in tqdm_asyncio(
            asyncio.as_completed(to_scrape),
            total=len(to_scrape),
            desc="Scraping Properties",
            ncols=100,
        ):
            try:
                response = await response
                if response is not None:
                    properties.append(self.parse_property(response))

                # Sleep after every 50 requests to avoid being rate limited
                counter += 1
                if counter % 30 == 0:
                    sleep_time = self.get_random_sleep_interval() * 2
                    print(
                        f"sleeping for {sleep_time: .2f} seconds after 30 requests to avoid rate limiting"
                    )
                    await asyncio.sleep(sleep_time)
            except RateLimitException as e:
                print(f"RateLimitException: {e}")
                break

        return properties

    async def scrape_search(self, url: str, paginate=True) -> List[str]:
        """
        Scrape search result pages from Idealista.com for property URLs

        Args:
            url: The search result URL to scrape
            paginate: Whether to scrape all pages of search results (up to a maximum of 60)

        Returns:
            A list of URLs for properties found in the search results
        """
        property_urls = []
        first_page = await self.make_request(url)
        property_urls.extend(self.parse_search(first_page))

        if not paginate:
            return property_urls

        total_pages = self.get_total_pages(first_page)
        if total_pages > self.MAX_PAGES:
            print(
                f"search contains more than max page limit ({total_pages}/{self.MAX_PAGES})"
            )
            total_pages = self.MAX_PAGES

        print(f"scraping {total_pages} pages of search results concurrently")

        to_scrape = [
            self.make_request(str(first_page.url) + f"pagina-{page}.htm")
            for page in range(2, total_pages + 1)
        ]

        for response in tqdm_asyncio(
            asyncio.as_completed(to_scrape),
            total=len(to_scrape),
            desc="Scraping Search Results",
            ncols=100,
        ):
            property_urls.extend(self.parse_search(await response))

        # Scraping pages succesfuly - sleep to avoid rate limiting
        sleep_time = self.get_random_sleep_interval() * 2
        print(
            f"sleeping for {sleep_time: .2f} seconds after {total_pages} requests to avoid rate limiting"
        )
        await asyncio.sleep(sleep_time)

        return property_urls

    def parse_search(self, response: httpx.Response) -> List[str]:
        """
        Parse an Idealista.com search result page for property URLs

        Args:
            response: The HTTP response object from the search result page request

        Returns:
            A list of property URLs found in the search results
        """
        soup = BeautifulSoup(response.text, "html.parser")
        urls = [
            urljoin(str(response.url), a["href"])
            for a in soup.select("article.item .item-link")
        ]
        return urls

    def get_total_pages(self, response: httpx.Response) -> int:
        """
        Get the total number of pages of search results for a given search URL

        Args:
            response: The HTTP response object from the first page of search results

        Returns:
            The total number of pages of search results
        """
        soup = BeautifulSoup(response.text, "html.parser")
        total_results = soup.select_one("h1#h1-container").text
        total_results = re.search(
            r"([0-9.,]+)\s*(?:casas|anuncios)", total_results
        ).group(1)
        return math.ceil(
            int(total_results.replace(".", "").replace(",", "")) / self.NUM_RESULTS_PAGE
        )

    def get_features(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """
        Extract property features from an Idealista.com property page

        Args:
            soup: The BeautifulSoup object representing the parsed HTML of the property page

        Returns:
            A dictionary of property features, where each key is a feature category and each
            value is a list of features in that category
        """
        feature_dict = {}
        for feature_block in soup.select('[class^="details-property-h"]'):
            feature_name = feature_block.text.strip()
            if feature_name != "Certificado energético":
                features = [
                    feat.text.strip()
                    for feat in feature_block.find_next("div").select("li")
                ]
            else:
                features = []
                for feat in feature_block.find_next("div").select("li"):
                    feat_props = feat.find_all("span")
                    type_certificate = feat_props[0].text.strip()
                    kwh_m2 = feat_props[1].text.strip()
                    energy_label = feat_props[1]["title"].upper()
                    energy_feat = f"{type_certificate} {kwh_m2} {energy_label}"
                    features.append(energy_feat.strip())

            feature_dict[feature_name] = features
        return feature_dict

    def get_image_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extract image data from an Idealista.com property page

        Args:
            soup: The BeautifulSoup object representing the parsed HTML of the property page

        Returns:
            A list of dictionaries representing each image, with keys for the image URL, caption,
            and other metadata
        """
        script = soup.find("script", string=re.compile("fullScreenGalleryPics"))
        if script is None:
            return []
        match = re.search(r"fullScreenGalleryPics\s*:\s*(\[.+?\]),", script.string)
        if match is None:
            return []
        image_data = json.loads(re.sub(r"(\w+?):([^/])", r'"\1":\2', match.group(1)))
        return image_data

    def get_images(self, image_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Extract image URLs from a list of image data dictionaries

        Args:
            image_data: A list of dictionaries representing each image, with keys for the image URL,
            caption, and other metadata

        Returns:
            A dictionary of image URLs, where each key is an image category and each value is a list
            of image URLs in that category
        """
        image_dict = defaultdict(list)
        for image in image_data:
            url = urljoin(self.base_url, image["imageUrl"])
            if image["isPlan"]:
                continue
            if image["tag"] is None:
                image_dict["main"].append(url)
            else:
                image_dict[image["tag"]].append(url)
        return dict(image_dict)

    def get_plans(self, image_data: List[Dict[str, Any]]) -> List[str]:
        """
        Extract plan image URLs from a list of image data dictionaries

        Args:
            image_data: A list of dictionaries representing each image, with keys for the image
            URL, caption, and other metadata

        Returns:
            A list of plan image URLs
        """
        plan_urls = [
            urljoin(self.base_url, image["imageUrl"])
            for image in image_data
            if image["isPlan"]
        ]
        return plan_urls

    def get_random_sleep_interval(self):
        """
        Generate a random sleep interval to add between requests
        Returns:
            A random sleep interval in seconds
        """
        return random.uniform(self.MIN_SLEEP_INTERVAL, self.MAX_SLEEP_INTERVAL)

    def exponential_backoff_with_jitter(self, retry_count):
        wait_time = min(self.INITIAL_BACKOFF * (2**retry_count), self.MAX_BACKOFF)
        jitter = random.uniform(0.5, 1.5)
        return wait_time * jitter

    def flatten_dict(self, d: dict, prefix: str = "") -> Dict[str, Any]:
        """
        Flatten a nested dictionary by concatenating keys with underscores

        Args:
            d: The dictionary to flatten
            prefix: A string to prepend to each key (default '')

        Returns:
            A flattened dictionary, where each key is a concatenation of the original keys
            separated by underscores
        """
        flat_dict = {}
        for k, v in d.items():
            if isinstance(v, dict):
                flat_dict.update(self.flatten_dict(v, f"{prefix}{k}_"))
            else:
                flat_dict[f"{prefix}{k}"] = v
        return flat_dict


class TokenBucket:
    def __init__(self, tokens, fill_rate):
        self.capacity = float(tokens)
        self.tokens = float(tokens)
        self.fill_rate = float(fill_rate)
        self.timestamp = time.monotonic()

    def get_tokens(self):
        now = time.monotonic()
        delta = now - self.timestamp
        self.timestamp = now
        self.tokens = min(self.capacity, self.tokens + self.fill_rate * delta)
        return self.tokens

    def consume(self, tokens):
        if tokens <= self.get_tokens():
            self.tokens -= tokens
            return True
        return False


class RateLimitException(Exception):
    """An exception raised when the scraper encounters rate limiting."""

    def __init__(self, message):
        super().__init__(message)
