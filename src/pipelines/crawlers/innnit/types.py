import hashlib
from typing import TypeAlias, TypedDict


class Petition(TypedDict):
    _id: str
    title: str
    name: str  # name = slug
    content: str
    signatureCount: int
    success: bool
    createdAt: str
    tag_title: str
    initiatorNames: list[str]
    targetNames: list[str]


JsonStr: TypeAlias = str


class _UnhashedPetitionRow(TypedDict):
    id: str
    title: str
    slug: str
    description: str
    signatures: int
    petitionSuccessful: bool
    tag: str
    initiators: JsonStr
    targets: JsonStr


class PetitionRow(_UnhashedPetitionRow):
    hash: str


def hash_row(row: _UnhashedPetitionRow) -> str:
    data = ":".join(
        [
            row["id"],
            row["title"],
            row["slug"],
            row["description"],
            str(row["signatures"]),
            str(row["petitionSuccessful"]),
            row["tag"],
            row["initiators"],
        ]
    )

    return hashlib.sha256(data.encode("utf-8")).hexdigest()
