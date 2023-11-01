import asyncio
from http import HTTPStatus
import random
import httpx

from repositories.http.rate_limiter import RateLimiter


class BaseHTTPClient:
    """
    A base HTTP client for making requests to a given base URL.

    This client handles concurrent requests, retries, and rate limits. It uses a
    semaphore to limit the number of concurrent requests, and implements an exponential
    backoff with jitter for retries. It also handles rate limits by sleeping for a
    specified duration when a rate limit is encountered.

    Attributes:
        base_url (str): The base URL for the requests.
        session (httpx.AsyncClient): The HTTPX async client used for making the
        requests.
        last_successful_url (str): The URL of the last successful request.
        Used for the 'referer' header in subsequent requests.
        semaphore (asyncio.Semaphore): A semaphore to limit the number of
        concurrent requests.
        max_retries (int): The maximum number of retries for each request.
        rate_limiter (RateLimiter): A rate limiter to control the rate of requests.
        sleep_after_rate_limit (int): The duration to sleep (in seconds) when a rate
        limit is encountered.

    Methods:
        request(url: str) -> httpx.Response: Make a request to the given URL,
        handling retries and rate limits.
        _wait_before_retry(status_code: int, url: str, retry_count: int): Wait for some
        time before retrying the request.
        _get_backoff_time(retry_count: int, initial_backoff: float = 32, max_backoff:
        float = 64) -> float: Calculate backoff time with jitter.
        _handle_rate_limit(response: httpx.Response, retry_count: int) -> None:
        Handle rate-limited requests.
    """

    def __init__(
        self,
        base_url: str,
        concurrent_requests_limit: int = 1,
        max_retries: int = 3,
        sleep_after_rate_limit: int = 12 * 60 * 60,
    ):
        """
        Initializes a new instance of the BaseHTTPClient class.

        Args:
            base_url (str): The base URL for the requests.
            concurrent_requests_limit (int, optional): The maximum number of concurrent
            requests. Defaults to 1.
            max_retries (int, optional): The maximum number of retries for each request.
            Defaults to 3.
            sleep_after_rate_limit (int, optional): The duration to sleep (in seconds)
            when a rate limit is encountered. Defaults to 12 hours.
        """
        self.base_url = base_url
        self.session = None
        self.last_successful_url = None
        self.semaphore = asyncio.Semaphore(concurrent_requests_limit)
        self.max_retries = max_retries
        self.rate_limiter = RateLimiter(tokens=1, fill_rate=1 / 27)
        self.sleep_after_rate_limit = sleep_after_rate_limit

    async def request(self, url: str) -> httpx.Response:
        """
        Make a request to the given URL, handling retries and rate limits.

        This method uses a semaphore to limit the number of concurrent requests.
        It also implements retries with an exponential backoff and handles rate
        limits by sleeping for a specified duration when a rate limit is encountered.

        Args:
            url (str): The URL to make the request to.

        Returns:
            httpx.Response: The response from the request. If the request fails after
            the maximum number of retries, it returns None.
        """
        async with self.semaphore:
            for retries in range(self.max_retries + 1):
                try:
                    await self.rate_limiter.wait_for_token()
                    if self.last_successful_url:
                        self.session.headers["referer"] = self.last_successful_url

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
                        await self._handle_rate_limit(response, retries)
                    else:
                        await self._wait_before_retry(
                            response.status_code, url, retries
                        )
                except (httpx.RequestError, asyncio.TimeoutError):
                    await self._wait_before_retry(-1, url, retries)

            print(f"Failed to make request after {self.max_retries} retries.")
            return None

    async def _wait_before_retry(self, status_code: int, url: str, retry_count: int):
        """
        Wait for some time before retrying the request.

        This method calculates the backoff time with jitter and then sleeps for
        that duration.

        Args:
            status_code (int): The HTTP status code from the failed request.
            url (str): The URL of the failed request.
            retry_count (int): The number of times the request has been retried.
        """
        sleep_duration = self._get_backoff_time(retry_count)
        print(f"HTTP {status_code} - Retrying in {sleep_duration} seconds: {url}")
        await asyncio.sleep(sleep_duration)

    def _get_backoff_time(
        self, retry_count: int, initial_backoff: float = 32, max_backoff: float = 64
    ) -> float:
        """
        Calculate backoff time with jitter.

        This method calculates the backoff time based on the number of retries, with a
        jitter to avoid thundering herd problem.

        Args:
            retry_count (int): The number of times the request has been retried.
            initial_backoff (float, optional): The initial backoff time in seconds.
            Defaults to 32.
            max_backoff (float, optional): The maximum backoff time in seconds.
            Defaults to 64.

        Returns:
            float: The backoff time in seconds.
        """
        wait_time = min(initial_backoff * (2**retry_count), max_backoff)
        jitter = random.uniform(0.5, 1.5)
        return wait_time * jitter

    async def _handle_rate_limit(
        self, response: httpx.Response, retry_count: int
    ) -> None:
        """
        Handle rate-limited requests.

        This method sleeps for a specified duration when a rate limit is encountered
        for the first time. If a rate limit is encountered again, it raises a
        RateLimitException.

        Args:
            response (httpx.Response): The response from the rate-limited request.
            retry_count (int): The number of times the request has been retried.
        """
        if retry_count == 0:
            print(
                f"HTTP {response.status_code} - "
                f"Retrying in {self.sleep_after_rate_limit / 3600} hours: {response.url}"
            )
            await asyncio.sleep(self.sleep_after_rate_limit)
        else:
            raise RateLimitException(
                f"HTTP {response.status_code} - Rate limit reached. URL: {response.url}"
            )


class RateLimitException(Exception):
    """An exception raised when the scraper encounters rate limiting."""

    def __init__(self, message):
        super().__init__(message)
