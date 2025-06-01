import decimal
from decimal import Decimal
from typing import Dict, Optional, Any

from langdetect import detect, LangDetectException

from src.common.logging.PipelineLogger import PipelineLogger

logger = PipelineLogger(__name__)


def _key_for_lang(key: str, lang: str) -> str:
    return f"{key}_en" if lang == "en" else f"{key}_de"


def _lang_for(value: Optional[str], doc_id: Optional[str]) -> str:
    """Helper to detect language and assign to appropriate field."""
    try:
        if value:
            return detect(value)
        else:
            logger.warning(
                f"No text provided for language detection in document id:'{doc_id}', returning default 'de'"
            )
    except LangDetectException:
        logger.warning(f"Language detection failed in document id:'{doc_id}', returning default 'de'")

    return "de"


def transform_row(row: Dict[str, Optional[str]]) -> Dict[str, Optional[Any]]:
    """Transform each row, detect language, and format for Typesense."""
    if row.get("id") is None:
        raise ValueError("id is mandatory")

    if row.get("title") is None:
        raise ValueError("title is mandatory")

    def decimal_as_str_or_none(value: Any) -> str | None:
        if value is None:
            return None
        try:
            return str(Decimal(value))
        except (ValueError, decimal.InvalidOperation):
            logger.warning(
                f"Could not convert value '{value}' to str via Decimal for document with id '{row.get('id')}'."
            )
            return None

    # Compute latitude and longitude as decimals or None
    latitude = decimal_as_str_or_none(row.get("latitude"))
    longitude = decimal_as_str_or_none(row.get("longitude"))

    # prefix id with 'volunteering_' to avoid collisions with other document types later on
    # initialize title_* and description_* with default values (will be filled below with real values)
    doc: Dict[str, Optional[Any]] = {
        "id": f"volunteering_{str(row.get('id'))}",
        "type": "volunteering",
        "title_de": None,
        "title_en": None,
        "description_de": None,
        "description_en": None,
        "location": row.get("location"),
        "location_lat_lng": [latitude, longitude] if latitude is not None and longitude is not None else None,
        "image_url": row.get("image"),
        "organizer_name": row.get("orga"),
        "organizer_image_url": row.get("imageOrga"),
        "external_url": row.get("link"),
        "source": row.get("source"),
        "link_locators": {"volunteering": row.get("id")},
        "topics_skills_embedding_v2": row.get("topics_skills_embedding_v2"),
        "topics_v2": row.get("topics_v2"),
        "skills_v2": row.get("skills_v2"),
        "similarity_embedding": row.get("similarity_embedding"),
        "location_type": row.get("location_type"),
        "commitment_label": row.get("commitment_label"),
    }

    title_lang = _lang_for(row.get("title"), row.get("id"))
    description_lang = _lang_for(row.get("description"), row.get("id"))

    doc[_key_for_lang("title", title_lang)] = row.get("title")
    doc[_key_for_lang("description", description_lang)] = row.get("description")

    return doc
