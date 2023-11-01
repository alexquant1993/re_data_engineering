import re
from typing import Dict, List


def split_basic_features(features: List[str]) -> Dict[str, str]:
    """Split basic listing features into a dictionary of key-value pairs"""
    dict_out = {}
    copy_features = features.copy()

    for feature in copy_features:
        lower_feature = feature.lower()

        if "construidos" in lower_feature or "útiles" in lower_feature:
            if "construidos" in lower_feature:
                dict_out["BUILT_AREA"] = int(
                    re.findall(r"\d+", lower_feature.split("construidos")[0])[0]
                )
            if "útiles" in lower_feature:
                dict_out["USEFUL_AREA"] = int(
                    re.search(r"\d+(?= m² útiles)", lower_feature).group()
                )
            features.remove(feature)

        elif "planta" in lower_feature:
            # Valid for single family homes
            dict_out["NUM_FLOORS"] = int(re.search(r"\d+", lower_feature).group())
            features.remove(feature)

        elif "parcela" in lower_feature:
            # Valid for single family homes
            lot_area = re.search(
                r"[\d]+[.,\d]+|[\d]*[.][\d]+|[\d]+", lower_feature
            ).group()
            dict_out["LOT_AREA"] = int(lot_area.replace(".", ""))
            features.remove(feature)

        elif "habitaci" in lower_feature:
            if "sin" in lower_feature:
                dict_out["NUM_BEDROOMS"] = 0
            else:
                dict_out["NUM_BEDROOMS"] = int(
                    re.search(r"\d+(?= habita)", feature).group()
                )
            features.remove(feature)

        elif "baño" in lower_feature:
            if "sin" in lower_feature:
                dict_out["NUM_BATHROOMS"] = 0
            else:
                dict_out["NUM_BATHROOMS"] = int(
                    re.search(r"\d+", lower_feature).group()
                )
            features.remove(feature)

        elif "garaje" in lower_feature:
            dict_out["FLAG_PARKING"] = True
            dict_out["PARKING_INCLUDED"] = "incluida" in lower_feature

            if not dict_out["PARKING_INCLUDED"]:
                parking_price = re.findall(
                    r"[\d]+[.,\d]+|[\d]*[.][\d]+|[\d]+", lower_feature
                )[0]
                dict_out["PARKING_PRICE"] = int(parking_price.replace(".", ""))

            features.remove(feature)

        elif "promoción" in lower_feature or "segunda mano" in lower_feature:
            dict_out["CONDITION"] = feature
            features.remove(feature)

        elif "armario" in lower_feature:
            dict_out["BUILTIN_WARDROBE"] = True
            features.remove(feature)

        elif "trastero" in lower_feature:
            dict_out["STORAGE_ROOM"] = True
            features.remove(feature)

        elif "orientación" in lower_feature:
            dict_out["CARDINAL_ORIENTATION"] = feature
            features.remove(feature)

        elif "calefacción" in lower_feature:
            dict_out["HEATING"] = feature
            features.remove(feature)

        elif "movilidad reducida" in lower_feature:
            dict_out["ACCESIBILITY_FLAG"] = True
            features.remove(feature)

        elif "construido en" in lower_feature:
            dict_out["YEAR_BUILT"] = int(re.search(r"\d+", lower_feature).group())
            features.remove(feature)

        elif "terraza" in lower_feature:
            dict_out["TERRACE"] = True
            features.remove(feature)

        elif "balcón" in lower_feature:
            dict_out["BALCONY"] = True
            features.remove(feature)

    # Set default values for keys that were not found in the features
    dict_out.setdefault("FLAG_PARKING", False)
    dict_out.setdefault("BUILTIN_WARDROBE", False)
    dict_out.setdefault("STORAGE_ROOM", False)
    dict_out.setdefault("ACCESIBILITY_FLAG", False)
    dict_out.setdefault("TERRACE", False)
    dict_out.setdefault("BALCONY", False)

    if features:
        print(f"WARNING: The following features were not parsed: {features}")

    return dict_out


def split_building_features(features: List[str]) -> Dict[str, str]:
    """Split building features into a dictionary of key-value pairs"""
    dict_out = {}
    copy_features = features.copy()

    for feature in copy_features:
        lower_feature = feature.lower()
        if any(
            [
                word in lower_feature
                for word in ["bajo", "planta", "interior", "exterior"]
            ]
        ):
            if "bajo" in lower_feature or "planta" in lower_feature:
                # Floor number
                if "bajo" in lower_feature:
                    dict_out["FLOOR"] = 0
                elif "entreplanta" in lower_feature:
                    dict_out["FLOOR"] = 0.5
                else:
                    dict_out["FLOOR"] = float(re.search(r"\d+", lower_feature).group())
            if "interior" in lower_feature or "exterior" in lower_feature:
                if "interior" in lower_feature:
                    dict_out["PROPERTY_ORIENTATION"] = "Interior"
                elif "exterior" in lower_feature:
                    dict_out["PROPERTY_ORIENTATION"] = "Exterior"
            features.remove(feature)

        elif "ascensor" in lower_feature:
            if "con" in lower_feature:
                dict_out["ELEVATOR"] = True
            elif "sin" in lower_feature:
                dict_out["ELEVATOR"] = False
            features.remove(feature)

    if features:
        print(f"WARNING: The following features were not parsed: {features}")

    return dict_out


def split_amenity_features(features: List[str]) -> Dict[str, str]:
    """Split amenity features into a dictionary of key-value pairs"""
    dict_out = {}
    copy_features = features.copy()

    for feature in copy_features:
        lower_feature = feature.lower()
        if "aire acondicionado" in lower_feature:
            dict_out["AIR_CONDITIONING"] = True
            features.remove(feature)
        elif "piscina" in lower_feature:
            dict_out["POOL"] = True
            features.remove(feature)
        elif "zonas verdes" in lower_feature or "jardín" in lower_feature:
            dict_out["GREEN_AREAS"] = True
            features.remove(feature)

    # Set default values for keys that were not found in the features
    dict_out.setdefault("AIR_CONDITIONING", False)
    dict_out.setdefault("POOL", False)
    dict_out.setdefault("GREEN_AREAS", False)

    if features:
        print(f"WARNING: The following features were not parsed: {features}")

    return dict_out


def split_energy_features(features: List[str]) -> Dict[str, str]:
    """Split energy features into a dictionary of key-value pairs"""
    dict_out = {}
    copy_features = features.copy()

    for feature in copy_features:
        lower_feature = feature.lower()
        if "consumo" not in lower_feature and "emisiones" not in lower_feature:
            dict_out["STATUS_EPC"] = feature
            features.remove(feature)
            break
        else:
            dict_out["STATUS_EPC"] = "Disponible"
            if "consumo" in lower_feature:
                s = lower_feature.split()
                if "kwh" in lower_feature:
                    dict_out["ENERGY_CONSUMPTION"] = float(
                        re.search(r"[\d]+[.,\d]+|[\d]*[.][\d]+|[\d]+", lower_feature)
                        .group()
                        .replace(",", ".")
                    )
                if len(s) > 1:
                    if len(s[-1]) == 1 and s[-1].isalpha():
                        dict_out["ENERGY_CONSUMPTION_LABEL"] = s[-1]
            if "emisiones" in lower_feature:
                s = lower_feature.split()
                if "kg co2" in lower_feature:
                    dict_out["ENERGY_EMISSIONS"] = float(
                        re.search(r"[\d]+[.,\d]+|[\d]*[.][\d]+|[\d]+", lower_feature)
                        .group()
                        .replace(",", ".")
                    )
                if len(s) > 1:
                    if len(s[-1]) == 1 and s[-1].isalpha():
                        dict_out["ENERGY_EMISSIONS_LABEL"] = s[-1]
            features.remove(feature)

    if features:
        print(f"WARNING: The following features were not parsed: {features}")

    return dict_out
