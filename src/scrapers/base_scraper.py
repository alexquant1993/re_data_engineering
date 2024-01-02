from abc import ABC, abstractmethod


class BaseScraper(ABC):
    @abstractmethod
    def scrape(self, url: str):
        """
        Main scraping method to initiate the scraping process for a given URL.

        Args:
            url (str): URL to scrape.

        Returns:
            Any: The scraped data. The type of data might vary depending on the
            specific scraper implementation.
        """
        pass
