from typing import Any, Dict


def flatten_dict(d: dict, prefix: str = "") -> Dict[str, Any]:
    """Flatten a nested dictionary by concatenating keys with underscores.

    Args:
        d: The dictionary to flatten
        prefix: A string to prepend to each key (default '')

    Returns:
        A flattened dictionary, where each key is a concatenation of the
        original keys separated by underscores
    """
    flat_dict = {}
    for k, v in d.items():
        if isinstance(v, dict):
            flat_dict.update(flatten_dict(v, f"{prefix}{k}_"))
        else:
            flat_dict[f"{prefix}{k}"] = v
    return flat_dict
