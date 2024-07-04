from typing import Optional
def join_if_not_empty(data: list, delimiter: str = "|") -> Optional[str]:
    """Join a list of strings with a delimiter if the list is not empty

    Parameters
    ----------
    data : list
        List of strings to join
    delimiter : Optional[str]
        Delimiter to use when joining the strings. Default: "|"

    """
    if all(data):
        return delimiter.join(data)
    return None