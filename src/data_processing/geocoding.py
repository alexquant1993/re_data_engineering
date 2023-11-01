import re
import pandas as pd
from tqdm import tqdm
from geopy.adapters import AioHTTPAdapter
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import AsyncRateLimiter


async def get_geocode_details(address1: str, address2: str, geocode) -> pd.Series:
    """
    Get geocode details given two addresses. The first address is the full address
    and the second address is the generic location. A geocode object from geopy.geocoders
    is required as an argument."""
    # If address doesn't have a number, add a 1
    if not re.search(r"\d", address1):
        address1_split = address1.split(",")
        address1_split[0] = address1_split[0] + " 1"
        address1 = ",".join(address1_split)

    # Try with the full address first
    location = await geocode(address1)
    # If that doesn't work, try with the generic location
    if location is None:
        location = await geocode(address2)

    # If that doesn't work, return None for all fields
    if location:
        # Get zip code from the full address
        zip_code = location.raw["display_name"].split(",")[-2].strip()
        if zip_code.isdigit():
            zip_code = zip_code
        else:
            zip_code = None
        return pd.Series(
            {
                "full_address": location.raw["display_name"],
                "postal_code": zip_code,
                "latitude": location.latitude,
                "longitude": location.longitude,
                "importance": location.raw.get("importance"),
                "place_id": location.raw.get("place_id"),
            }
        )
    else:
        return pd.Series(
            {
                "full_address": None,
                "postal_code": None,
                "latitude": None,
                "longitude": None,
                "importance": None,
                "place_id": None,
            }
        )


async def get_geocode_details_batch(df: pd.DataFrame) -> pd.DataFrame:
    """Get geocode details for a dataframe of addresses.
    Args:
        df (pd.DataFrame): Dataframe with two columns: ADDRESS and LOCATION
            - ADDRESS: Full address
            - LOCATION: Generic location
    Returns:
        pd.DataFrame: Dataframe with geocode details
            - FULL_ADDRESS
            - ZIP_CODE
            - LATITUDE
            - LONGITUDE
            - IMPORTANCE_LOCATION
            - LOCATION_ID
    """
    async with Nominatim(
        user_agent="idealista-scraper", adapter_factory=AioHTTPAdapter, timeout=10
    ) as geolocator:
        geocode = AsyncRateLimiter(geolocator.geocode, min_delay_seconds=1)
        geo_tasks = [
            get_geocode_details(row["ADDRESS"], row["LOCATION"], geocode)
            for _, row in df.iterrows()
        ]
        results = []
        # Geocode addresses asynchronously
        for geo_task in tqdm(
            geo_tasks, total=df.shape[0], desc="Geocoding...", ncols=100
        ):
            result = await geo_task
            results.append(result)

    df_out = pd.DataFrame()
    df_out[
        [
            "FULL_ADDRESS",
            "ZIP_CODE",
            "LATITUDE",
            "LONGITUDE",
            "IMPORTANCE_LOCATION",
            "LOCATION_ID",
        ]
    ] = pd.DataFrame(results)

    return df_out
