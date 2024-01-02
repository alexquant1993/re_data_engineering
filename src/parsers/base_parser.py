from abc import ABC, abstractmethod

import httpx


class BaseParser(ABC):
    @abstractmethod
    def parse_search(self, response: httpx.Response) -> list:
        """
        Parses a search results page to extract URLs or other relevant data.

        Args:
            response (httpx.Response): The response object from the search.
            results page request.

        Returns:
            list: A list of URLs or other relevant data extracted from the
            search results.
        """
        pass

    @abstractmethod
    def parse_property(self, response: httpx.Response):
        """
        Parses a property detail page to extract relevant data about the
        property.

        Args:
            response (httpx.Response): The response object from the property
            page request.

        Returns:
            Any: The parsed data about the property.
        """
        pass
