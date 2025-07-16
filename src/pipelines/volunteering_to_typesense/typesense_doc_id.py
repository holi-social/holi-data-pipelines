from dataclasses import dataclass


@dataclass
class TypesenseDocId:
    id: str

    def __str__(self) -> str:
        return f"volunteering_{self.id}"
