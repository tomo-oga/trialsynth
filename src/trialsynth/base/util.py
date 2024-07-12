from typing import Optional

def list_from_string(data: str, delimiter: str = ",") -> list[str]:
    return [item.strip() for item in data.split(delimiter)]
