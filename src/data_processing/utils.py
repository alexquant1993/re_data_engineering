import locale
from datetime import datetime

import pandas as pd


def parse_date_with_locale(date_string, format_string, new_locale):
    """Parse a date string with a given format and locale."""
    # Save the current locale
    old_locale = locale.getlocale(locale.LC_TIME)

    # Set the new locale
    locale.setlocale(locale.LC_TIME, new_locale)

    # Perform the date parsing
    date_object = datetime.strptime(date_string, format_string)

    # Restore the original locale
    locale.setlocale(locale.LC_TIME, old_locale)

    return date_object


def parse_date_in_column(date_string):
    """Parse a date string given a spanish locale and adding the current year."""
    year = datetime.now().year
    date_string_with_year = f"{date_string} de {year}"
    return parse_date_with_locale(
        date_string_with_year, "%d de %B de %Y", "es_ES.UTF-8"
    )


def get_features_asdf(pds: pd.Series, split_function) -> pd.DataFrame:
    """Split a pandas series of features into a dataframe
    Args:
        pds: A pandas series of features. Each element stores a list of features.
        split_function: A function to split each feature into a dictionary
    Returns:
        A dataframe of features
    """
    # Check if pds is a pandas series
    if not isinstance(pds, pd.Series):
        raise TypeError("pds must be a pandas series")
    # Apply split function to each element of the series
    df_features = []
    for feature in pds:
        if not isinstance(feature, list):
            feature = []
        df_features.append(split_function(feature))
    df_features = pd.DataFrame(df_features)
    return df_features
