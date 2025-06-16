import re

HTML_RE = re.compile(r"<[^>]+>")  # regex to remove HTML from descriptions


def remove_html(input: str) -> str:
    lines = HTML_RE.sub("", input).split("\n")
    return " ".join(lines)


def shorten_str(input: str, max_length: int) -> str:
    if len(input) <= max_length:
        return input.strip()
    return input[: max_length - 3].strip() + "..."
