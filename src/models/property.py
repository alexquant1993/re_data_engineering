from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class Property:
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
