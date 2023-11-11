import re
from typing import Any, Dict, List

import pandas as pd
from prefect import task

from src.data_processing.feature_parser import (
    split_amenity_features,
    split_basic_features,
    split_building_features,
    split_energy_features,
)
from src.data_processing.geocoding import get_geocode_details_batch
from src.data_processing.utils import get_features_asdf, parse_date_in_column


@task(retries=3, log_prints=True)
async def clean_scraped_data(
    property_data: List[Dict[str, Any]], type_search: str
) -> pd.DataFrame:
    """Clean the data from the scraped properties."""
    print("Cleaning data...")
    df = pd.DataFrame(property_data)

    # Get ID from URL
    df_out = pd.DataFrame()
    df_out["ID_LISTING"] = df["url"].apply(lambda x: re.findall(r"\d+", x)[0])
    df_out["URL"] = df["url"]
    # Get type of property and address from title and location
    if type_search == "sale":
        pattern = r"(.*) en venta en (.*)"
    else:
        pattern = r"Alquiler de (.*) en (.*)"
    df_match = df["title"].str.extract(pattern).applymap(lambda x: x.strip())
    df_out["TYPE_PROPERTY"] = df_match[0]
    df_out["ADDRESS"] = df_match[1] + ", " + df["location"]
    # Get ZIP code based on address and location
    df_out["LOCATION"] = df["location"]
    df_geocode_details = await get_geocode_details_batch(df_out)
    df_out = pd.concat([df_out, df_geocode_details], axis=1)
    # Get price and currency
    df_out["PRICE"] = df["price"]
    df_out["ORIGINAL_PRICE"] = df["original_price"]
    df_out["CURRENCY"] = df["currency"]
    # Get post tags
    df_out["TAGS"] = df["tags"].astype(str)
    # Get listing description
    df_out["LISTING_DESCRIPTION"] = df["description"]
    # Get poster details
    df_out["POSTER_TYPE"] = df["poster_type"]
    df_out["POSTER_NAME"] = df["poster_name"]
    # Get basic listing features
    df_basic_features = get_features_asdf(
        df["features_Características básicas"], split_basic_features
    )
    # Get building listing features
    df_building_features = get_features_asdf(
        df["features_Edificio"], split_building_features
    )
    # Get amenities listing features
    df_amenities_features = get_features_asdf(
        df["features_Equipamiento"], split_amenity_features
    )
    # Get energy listing features
    df_energy_features = get_features_asdf(
        df["features_Certificado energético"], split_energy_features
    )
    # Concatenate all features
    df_out = pd.concat(
        [
            df_out,
            df_basic_features,
            df_building_features,
            df_amenities_features,
            df_energy_features,
        ],
        axis=1,
    )
    # Get last update date and timestamp
    df_out["LAST_UPDATE_DATE"] = df["updated"].apply(parse_date_in_column)
    df_out["TIMESTAMP"] = pd.to_datetime(df["time_stamp"])
    # TODO: Get columns related to photos of the listing - might be useful for future analysis
    # image_cols = [col for col in df.columns if col.startswith('image')]
    # df_out[image_cols] = df[image_cols]

    return df_out
