
# Arg parsers
def parse_value(value: Any) -> str:

    if value is None:
        return None

    if isinstance(value, bool):
        return ""

    if isinstance(value, list):
        values = [parse_value(x) for x in value]
        return " ".join(values)

    if isinstance(value, str):
        if " " in value:
            value = f'"{value}"'
            return value

    return str(value)


def get_argument_string(options: dict) -> str:

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


