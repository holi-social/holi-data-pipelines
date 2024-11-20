from typing import Dict, Optional
from decimal import Decimal
import decimal

import prefect
from langdetect import detect, LangDetectException

logger = prefect.logging.get_logger()


def _key_for_lang(key: str, lang: str) -> str:
    return f"{key}_en" if lang == "en" else f"{key}_de"


def _lang_for(value: str) -> str:
    """Helper to detect language and assign to appropriate field."""
    try:
        if value:
            return detect(value)
        else:
            logger.warning(f"No text provided for language detection in document")
    except LangDetectException:
        logger.warning(f"Language detection failed")
    
    return "de"


def transform_row(row: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    """Transform each row, detect language, and format for Typesense."""
    if row.get("id") is None:
        raise ValueError("id is mandatory")

    def decimal_or_none(value: any) -> Decimal | None:
        if value is None:
            return None
        try:
            return Decimal(value)
        except (ValueError, decimal.InvalidOperation):
            return None
        
     # Compute latitude and longitude as decimals or None
    latitude = decimal_or_none(row.get("latitude"))
    longitude = decimal_or_none(row.get("longitude"))

    # initialize title_* and description_* with default values (will be filled below with real values)
    doc: Dict[str, Optional[str]] = {
        "type": "volunteering",
        "volunteering_id": row.get("id"),
        "title_de": None,
        "title_en": None,
        "description_de": None,
        "description_en": None,
        "location": row.get("location"),
        "location_lat_lng": [latitude, longitude] if latitude is not None and longitude is not None else None,
        "image_url": row.get("image"),
        "link_locators": { "volunteering": row.get("id") },
    }

    title_lang = _lang_for(row.get("title"))
    description_lang = _lang_for(row.get("description"))

    doc[_key_for_lang("title", title_lang)] = row.get("title")
    doc[_key_for_lang("description", description_lang)] = row.get("description")

    return doc
