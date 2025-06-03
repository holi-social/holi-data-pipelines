from typing import Optional


def map_task_regularity_to_commitment_label(regularity: Optional[str]) -> Optional[str]:
    if not regularity:
        return None
    match regularity:
        case "ONE_TIME":
            return "one-time"
        case "ONCE_WEEK":
            return "recurring"
        case "ONCE_MONTH":
            return "recurring"
        case "REGULARLY":
            return "recurring"
        case "PROJECT_BASED":
            return "negotiable"
        case "NEGOTIABLE":
            return "negotiable"
    return None


def map_task_location_type_to_typesense_location_type(location_type: Optional[str]) -> Optional[str]:
    if not location_type:
        return None
    match location_type:
        case "HYBRID":
            return "hybrid"
        case "REMOTE":
            return "remote"
        case "ONSITE":
            return "onsite"
    return None
