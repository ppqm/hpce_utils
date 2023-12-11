from typing import Any


def parse_value(value: Any) -> str | None:
    """Parse values and stringify the options"""

    if value is None:
        return None

    # By default, if --arg is set, the bool is true.
    if isinstance(value, bool):
        return ""

    if isinstance(value, list):
        values: list[str | None] = [parse_value(x) for x in value if x is not None]
        _values: list[str] = [x for x in values if x is not None]
        return " ".join(_values)

    if isinstance(value, str):
        if " " in value:
            value = f'"{value}"'
            return value

    return str(value)


def get_argument_string(options: dict) -> str:
    """Translate options to string commandline interface"""

    line = []
    for key, val in options.items():

        # Ignore empty values
        if val is None:
            continue

        # Ignore False values
        if val is False:
            continue

        key = key.replace("_", "-")
        _val = parse_value(val)
        cmd = f"--{key} {_val}"
        line.append(cmd)

    return " ".join(line)
