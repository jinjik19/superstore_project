import re


def to_snake_case(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[\s\-\.]+", "_", name)
    name = re.sub(r"[^0-9a-zA-Z_]", "", name)

    return name.lower()
