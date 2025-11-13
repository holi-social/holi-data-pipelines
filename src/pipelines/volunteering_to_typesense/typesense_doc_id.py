from src.pipelines.typesense import TypesenseDocId


def typesense_doc_id(id: str) -> TypesenseDocId:
    return TypesenseDocId("volunteering", id)
