from typing import Dict, Optional

import prefect
from langdetect import detect, LangDetectException

# from common.logging_setup import initialize_logging

# Initialize logging
# initialize_logging()
logger = prefect.logging.get_logger()


def _key_for_lang(key: str, lang: str) -> str:
    return f"{key}_de" if lang == "de" else f"{key}_en"


def transform_row(row: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    """Transform each row, detect language, and format for Typesense."""
    def _lang_for(value: str) -> str:
        """Helper to detect language and assign to appropriate field."""
        try:
            if value:
                return detect(value)
            else:
                logger.warning(f"No text provided for language detection in document with id {row.get("id")}")
        except LangDetectException:
            logger.warning(f"Language detection failed in document with id {row.get("id")} for text: {value[:30]}...")
    return "de"

    # Initialize document with default values
    doc: Dict[str, Optional[str]] = {
        "type": "volunteering",
        "volunteering_id": row.get("id"),
        "title_de": None,
        "title_en": None,
        "description_de": None,
        "description_en": None,
        "location": row.get("location"),
        "location_lat_lng": (
            [row.get("latitude"), row.get("longitude")]
            if row.get("latitude") is not None and row.get("longitude") is not None
            else None
        ),
        "image_url": row.get("image"),
        "link_path": row.get("link"),
    }
    title_lang = _lang_for(row.get("title"))
    doc[_key_for_lang("title", title_lang)] = row.get("title")
    description_lang = _lang_for(row.get("description"))
    doc[_key_for_lang("description", description_lang)] = row.get("description")

    return doc
