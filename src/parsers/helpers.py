from collections import defaultdict
import json
import re
from typing import Any, Dict, List
from urllib.parse import urljoin
from bs4 import BeautifulSoup


def get_features(soup: BeautifulSoup) -> Dict[str, List[str]]:
    """
    Extract property features from an Idealista.com property page

    Args:
        soup: The BeautifulSoup object representing the parsed HTML of the
        property page

    Returns:
        A dictionary of property features, where each key is a feature category
        and each value is a list of features in that category
    """
    feature_dict = {}
    for feature_block in soup.select('[class^="details-property-h"]'):
        feature_name = feature_block.text.strip()
        if feature_name != "Certificado energético":
            features = [
                feat.text.strip()
                for feat in feature_block.find_next("div").select("li")
            ]
        else:
            features = []
            for feat in feature_block.find_next("div").select("li"):
                feat_props = feat.find_all("span")
                type_certificate = feat_props[0].text.strip()
                kwh_m2 = feat_props[1].text.strip()
                energy_label = feat_props[1]["title"].upper()
                energy_feat = f"{type_certificate} {kwh_m2} {energy_label}"
                features.append(energy_feat.strip())

        feature_dict[feature_name] = features
    return feature_dict


def get_image_data(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """
    Extract image data from an Idealista.com property page

    Args:
        soup: The BeautifulSoup object representing the parsed HTML of the
        property page

    Returns:
        A list of dictionaries representing each image, with keys for the image
        URL, caption, and other metadata
    """
    script = soup.find("script", string=re.compile("fullScreenGalleryPics"))
    if script is None:
        return []
    match = re.search(r"fullScreenGalleryPics\s*:\s*(\[.+?\]),", script.string)
    if match is None:
        return []
    image_data = json.loads(re.sub(r"(\w+?):([^/])", r'"\1":\2', match.group(1)))
    return image_data


def get_images(base_url, image_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """
    Extract image URLs from a list of image data dictionaries

    Args:
        image_data: A list of dictionaries representing each image, with keys
        for the image URL, caption, and other metadata

    Returns:
        A dictionary of image URLs, where each key is an image category and
        each value is a list of image URLs in that category
    """
    image_dict = defaultdict(list)
    for image in image_data:
        url = urljoin(base_url, image["imageUrl"])
        if image["isPlan"]:
            continue
        if image["tag"] is None:
            image_dict["main"].append(url)
        else:
            image_dict[image["tag"]].append(url)
    return dict(image_dict)


def get_plans(base_url, image_data: List[Dict[str, Any]]) -> List[str]:
    """
    Extract plan image URLs from a list of image data dictionaries

    Args:
        image_data: A list of dictionaries representing each image, with keys
        for the image URL, caption, and other metadata

    Returns:
        A list of plan image URLs
    """
    plan_urls = [
        urljoin(base_url, image["imageUrl"]) for image in image_data if image["isPlan"]
    ]
    return plan_urls
