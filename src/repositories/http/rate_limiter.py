import asyncio
import random


class RateLimiter:
    """
    A rate limiter using the token bucket algorithm.

    This class represents a rate limiter that uses the token bucket algorithm
    to control the rate of actions.
    It can be used to limit the rate of requests, operations, or events in
    software.

    Attributes:
        tokens (int): The current number of tokens in the bucket.
        fill_rate (float): The rate at which the bucket is filled with tokens,
        in tokens per second.

    Methods:
        consume(amount: int) -> bool: Consumes the specified amount of tokens
        from the bucket.
        wait_for_token(): Waits until a token is available and then consumes it.
    """

    def __init__(self, tokens: int, fill_rate: float):
        """
        Initializes a new instance of the RateLimiter class.

        Args:
            tokens (int): The initial number of tokens in the bucket.
            fill_rate (float): The rate at which the bucket is filled with
            tokens, in tokens per second.
        """
        self.tokens = tokens
        self.fill_rate = fill_rate
        self.last_time = asyncio.get_event_loop().time()

    async def consume(self, amount: int) -> bool:
        """
        Consumes the specified amount of tokens from the bucket.

        If there are not enough tokens in the bucket, no tokens are consumed.

        Args:
            amount (int): The amount of tokens to consume.

        Returns:
            bool: True if the tokens were consumed; False otherwise.
        """
        current_time = asyncio.get_event_loop().time()
        elapsed = current_time - self.last_time
        self.last_time = current_time
        self.tokens += elapsed * self.fill_rate

        if self.tokens >= amount:
            self.tokens -= amount
            return True
        else:
            return False

    async def wait_for_token(self):
        """
        Waits until a token is available and then consumes it.

        This method blocks until a token is available.
        """
        while not await self.consume(1):
            await asyncio.sleep(random.uniform(1, 5))
