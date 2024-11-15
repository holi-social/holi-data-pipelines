import logging
from langdetect import detect, LangDetectException
from typing import Dict, Optional

from common.logging_setup import initialize_logging

# Initialize logging from logging_setup
initialize_logging()
logger = logging.getLogger(__name__)


def transform_row(row: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    """Transform each row, detect language, and format for Typesense."""
    # Initialize document with default values
    doc: Dict[str, Optional[str]] = {
        "type": "volunteering",
        "volunteering_id": row.get("id", ""),
        "title_de": None,
        "title_en": None,
        "description_de": None,
        "description_en": None,
        "location": row.get("location", ""),
        "location_lat_lng": (
            [row.get("latitude"), row.get("longitude")]
            if row.get("latitude") is not None and row.get("longitude") is not None
            else None
        ),
        "image_url": row.get("image", ""),
        "link_path": row.get("link", ""),
    }

    def assign_language_field(text: str, de_field: str, en_field: str) -> None:
        """Helper to detect language and assign to appropriate field."""
        try:
            if text:
                lang = detect(text)
                if lang == "de":
                    doc[de_field] = text
                else:
                    doc[en_field] = text
            else:
                logger.debug(
                    f"No text provided for language detection in field: {de_field}/{en_field}"
                )
        except LangDetectException:
            logger.warning(f"Language detection failed for text: {text[:30]}...")

    # Detect language for title and description
    assign_language_field(row.get("title", ""), "title_de", "title_en")
    assign_language_field(
        row.get("description", ""), "description_de", "description_en"
    )

    return doc
