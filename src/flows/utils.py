from typing import List


def chunks(lst: List[str], n: int) -> List[str]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
