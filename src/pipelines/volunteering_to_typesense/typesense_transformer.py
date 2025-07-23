import decimal
from decimal import Decimal
from typing import Dict, Optional, Any, List
import phonenumbers
import json
import re

from langdetect import detect, LangDetectException

from src.common.logging.PipelineLogger import PipelineLogger

logger = PipelineLogger(__name__)

TOPICS_SKILLS_EMBEDDING_V2_EXPECTED_DIMENSIONS = 53
SIMILARITY_EMBEDDING_EXPECTED_DIMENSIONS = 256


def _key_for_lang(key: str, lang: str) -> str:
    return f"{key}_en" if lang == "en" else f"{key}_de"


def _lang_for(value: Optional[str], doc_id: Optional[str]) -> str:
    """Helper to detect language and assign to appropriate field."""
    try:
        if value:
            return detect(value)
        else:
            logger.info(f"[id='{doc_id}'] No text provided for language detection in document, returning default 'de'")
    except LangDetectException:
        logger.warning(f"[id='{doc_id}'] Language detection failed, returning default 'de'")

    return "de"


def _validate_vector(row: Dict[str, Optional[Any]], key: str, expected_vector_dimension: int) -> List[float] | None:
    vector = row.get(key)
    if isinstance(vector, list):
        match len(vector):
            case n if n == expected_vector_dimension:
                if all(isinstance(element, float) for element in vector):
                    return vector
                else:
                    logger.warn(f"[id='{row.get('id')}'] Unexpected data type in vector {key}: {vector}")
            case 0:
                logger.info(f"[id='{row.get('id')}'] No {key} found")
            case _:
                logger.warn(f"[id='{row.get('id')}'] Unexpected dimensionality of {key}: {len(vector)}")
    else:
        logger.warn(
            f"[id='{row.get('id')}'] Unexpected data type for vector {key} (found: {type(vector)}, expected: list)"
        )
    return None


def _decimal_as_str_or_none(row: Dict[str, Optional[Any]], key: str) -> str | None:
    value = row.get(key)
    if value is None:
        return None
    try:
        return str(Decimal(value))
    except (ValueError, decimal.InvalidOperation):
        logger.warning(
            f"[id='{row.get('id')}'] Could not convert value '{value}' to str for '{key}' via Decimal for document."
        )
        return None


def _extract_phonenumber(number: str) -> Optional[str]:
    try:
        parsed_number = phonenumbers.parse(number, "DE")
        return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
    except phonenumbers.NumberParseException as e:
        any_number = re.search(r"\d+", number)
        message = "falling back to original string" if any_number else "omitting"
        logger.warn(f"Failed to parse phone number {number}: {e}, {message}")
        if any_number:
            return number
    return None


def _transform_contact(
    contact_str: Optional[str], orga: Optional[str], link: Optional[str]
) -> List[Dict[str, Optional[Any]]]:
    contact_info = []
    try:
        # TODO: contact is of type str during full ingest, but dict during update ingest?
        contact = json.loads(contact_str) if contact_str and isinstance(contact_str, str) else contact_str
        if contact:
            first_name = contact.get("vorname")
            last_name = contact.get("nachname")
            contact_person = f"{first_name} {last_name}" if first_name and last_name else first_name or last_name
            email = contact.get("email")
            mobile = contact.get("mobil")
            phone = contact.get("telefon")
            if contact_person:
                contact_info.append({"type": "person", "value": contact_person})
            if email:
                contact_info.append({"type": "email", "value": email})
            if mobile:
                parsed_number = _extract_phonenumber(str(mobile))
                if parsed_number:
                    contact_info.append({"type": "phone", "value": parsed_number})
            if phone:
                parsed_number = _extract_phonenumber(str(phone))
                if parsed_number:
                    contact_info.append({"type": "phone", "value": parsed_number})
        if link and orga:
            contact_info.append({"type": "website", "value": link, "label": orga})
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing contact JSON {contact_str}: {e}")
    except TypeError as e:
        logger.error(f"Error parsing contact JSON {contact_str}: {e}")
    return contact_info


def transform_row(row: Dict[str, Optional[Any]]) -> Dict[str, Optional[Any]]:
    """Transform each row, detect language, and format for Typesense."""
    if row.get("id") is None:
        raise ValueError("id is mandatory")

    if row.get("title") is None:
        raise ValueError("title is mandatory")

    # Compute latitude and longitude as decimals or None
    latitude = _decimal_as_str_or_none(row, "latitude")
    longitude = _decimal_as_str_or_none(row, "longitude")

    topics_skills_embedding_v2 = _validate_vector(
        row, "topics_skills_embedding_v2", TOPICS_SKILLS_EMBEDDING_V2_EXPECTED_DIMENSIONS
    )
    similarity_embedding = _validate_vector(row, "similarity_embedding", SIMILARITY_EMBEDDING_EXPECTED_DIMENSIONS)

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
        "organizer_url": row.get("linkOrga"),
        "external_url": row.get("link"),
        "source": row.get("source"),
        "contact": _transform_contact(row.get("contact"), row.get("orga"), row.get("link")),
        "link_locators": {"volunteering": row.get("id")},
        "topics_skills_embedding_v2": topics_skills_embedding_v2,
        "topics_v2": row.get("topics_v2"),
        "skills_v2": row.get("skills_v2"),
        "similarity_embedding": similarity_embedding,
        "location_type": row.get("location_type"),
        "commitment_label": row.get("commitment_label"),
        "commitment_duration": row.get("commitment_duration"),
        "commitment_duration_ai_derived": True,
        "commitment_duration_ai_reasoning": row.get("commitment_duration_ai_reasoning"),
    }

    title_lang = _lang_for(row.get("title"), row.get("id"))
    description_lang = _lang_for(row.get("description"), row.get("id"))

    doc[_key_for_lang("title", title_lang)] = row.get("title")
    doc[_key_for_lang("description", description_lang)] = row.get("description")

    return doc
