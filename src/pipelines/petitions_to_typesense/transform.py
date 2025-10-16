import json
from typing import Literal, TypedDict

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
        "initiators": json.loads(petition["initiators"]),
        "targets": json.loads(petition["targets"]),
    }


def petitions_to_typesense_docs(petitions: list[PetitionRow]) -> list[PetitionDoc]:
    return list(map(petition_to_doc, petitions))
