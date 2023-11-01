from datetime import datetime
import math
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import httpx
from models.property import Property
from parsers.base_parser import BaseParser
from parsers.helpers import get_features, get_image_data, get_images, get_plans


class IdealistaParser(BaseParser):
    def __init__(
        self,
        response: httpx.Response,
        num_results_page: int = None,
        base_url: str = "https://www.idealista.com",
    ) -> None:
        """
        Args:
            response (httpx.Response): The response object from the search
            results page request.
            num_results_page (int): Number of results per search page. Optional.
        """
        self.response = response
        self.num_results_page = num_results_page
        self.base_url = base_url

    def parse_search(self) -> list:
        """
        Parse an Idealista search results page to extract property URLs.

        Returns:
            list: A list of property URLs found in the search results.
        """
        soup = BeautifulSoup(self.response.text, "html.parser")
        urls = [
            urljoin(str(self.response.url), a["href"])
            for a in soup.select("article.item .item-link")
        ]
        return urls

    def parse_property(self) -> Property:
        """
        Parse an Idealista property page to extract relevant property details.

        Returns:
            dict: A dictionary containing the parsed property details.
        """
        soup = BeautifulSoup(self.response.text, "html.parser")

        # Extracting general details
        url = str(self.response.url)
        title = soup.select_one(".main-info__title-main").text.strip()
        location = soup.select_one(".main-info__title-minor").text.strip()
        currency = soup.select_one(".info-data-price").contents[-1].strip()

        # Extracting price details
        price_element = soup.select_one(".info-data-price span")
        price = (
            int(price_element.text.replace(".", "").replace(",", ""))
            if price_element
            else None
        )
        original_price_element = soup.select_one(".pricedown_price span")
        original_price = (
            int(original_price_element.text.strip().replace(".", "").replace(",", ""))
            if original_price_element
            else None
        )

        # Tags
        tags = (
            soup.select_one(".detail-info-tags").text.split()
            if soup.select_one(".detail-info-tags")
            else None
        )

        # Extracting post description
        description = "\n".join([p.text.strip() for p in soup.select("div.comment p")])

        # Poster details
        check_professional = soup.select_one(
            ".advertiser-name-container .about-advertiser-name"
        )
        if check_professional:
            poster_type = "Profesional"
            poster_name = check_professional.text.strip()
        else:
            poster_type = "Particular"
            poster_name_element = soup.select_one(".professional-name span")
            poster_name = (
                poster_name_element.text.strip() if poster_name_element else None
            )

        # Get image data
        image_data = get_image_data(soup)

        # Dates
        updated_element = soup.select_one(
            'p.stats-text:-soup-contains("actualizado el")'
        )
        updated = updated_element.text.split(" el ")[-1] if updated_element else None
        time_stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return Property(
            url=url,
            title=title,
            location=location,
            currency=currency,
            price=price,
            original_price=original_price,
            tags=tags,
            description=description,
            poster_type=poster_type,
            poster_name=poster_name,
            features=get_features(soup),
            images=get_images(self.base_url, image_data),
            plans=get_plans(self.base_url, image_data),
            updated=updated,
            time_stamp=time_stamp,
        )

    def get_total_pages(self) -> int:
        """
        Get the total number of pages of search results for a given search URL

        Returns:
            The total number of pages of search results
        """
        soup = BeautifulSoup(self.response.text, "html.parser")
        total_results = soup.select_one("h1#h1-container").text
        total_results = re.search(
            r"([0-9.,]+)\s*(?:casas|anuncios)", total_results
        ).group(1)
        return math.ceil(
            int(total_results.replace(".", "").replace(",", "")) / self.num_results_page
        )
