import random
from typing import Dict

import httpagentparser
from latest_user_agents import get_latest_user_agents


def get_random_header() -> Dict[str, str]:
    """
    This function returns a random HTTP header from a set of latest user agents.

    The user agents are categorized based on the combination of operating system
    and browser. The combinations are as follows:
    - Windows + Chrome. Weight: 0.64
    - Windows + Firefox. Weight: 0.1
    - MacOS + Chrome. Weight: 0.1
    - MacOS + Safari. Weight: 0.13
    - MacOS + Firefox. Weight: 0.03

    Returns:
    dict: A dictionary representing the HTTP header.

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
