import json
from typing import Literal, TypedDict, Any

from src.pipelines.crawlers.innnit.types import PetitionRow
from src.pipelines.typesense import TypesenseDocId


def petition_typesense_doc_id(id: str) -> TypesenseDocId:
    return TypesenseDocId("petition", id)


class PetitionDoc(TypedDict):
    id: str
    type: Literal["petition"]
    title: str
    slug: str
    description: str
    signatures: int
    petitionSuccessful: bool
    tag: str
    initiators: list[str]
    targets: list[str]


def _parse_string_list(value: Any) -> list[str]:
    """Return a list[str] whether the input is already a list or a JSON string.

    Some pipeline stages may persist array-typed columns as JSON strings, while
    others may already materialize them as arrays. This helper normalizes both
    cases to a list of strings to satisfy Typesense schema and avoid
    json.loads errors when the value is already a list.
    """
    if value is None:
        return []
    if isinstance(value, list):
        # Ensure items are strings
        return [str(x) for x in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            # If it's not valid JSON, treat the raw string as a single-item list
            return [value]
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
        # If parsed JSON isn't a list, convert to single-item list
        return [str(parsed)]
    # Fallback: coerce to string and wrap into a list
    return [str(value)]


def petition_to_doc(petition: PetitionRow) -> PetitionDoc:
    return {
        "id": str(petition_typesense_doc_id(petition["id"])),
        "type": "petition",
        "title": petition["title"],
        "slug": petition["slug"],
        "description": petition["description"],
        "signatures": petition["signatures"],
        "petitionSuccessful": petition["petitionSuccessful"],
        "tag": petition["tag"],
        "initiators": _parse_string_list(petition["initiators"]),
        "targets": _parse_string_list(petition["targets"]),
    }


def petitions_to_typesense_docs(petitions: list[PetitionRow]) -> list[PetitionDoc]:
    return list(map(petition_to_doc, petitions))
