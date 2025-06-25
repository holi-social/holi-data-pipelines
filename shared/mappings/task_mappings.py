from typing import Optional, Literal, get_args

OkunaTaskLocationType = Literal["HYBRID", "REMOTE", "ONSITE"]

TypesenseTaskLocationType = Literal["remote", "onsite"]


def map_task_location_type_to_typesense_location_type(
    location_type: Optional[OkunaTaskLocationType],
) -> Optional[TypesenseTaskLocationType]:
    if not location_type:
        return None
    match location_type:
        case "HYBRID":
            return "remote"
        case "REMOTE":
            return "remote"
        case "ONSITE":
            return "onsite"
    raise ValueError(
        f"Unexpected location_type str '{location_type}'. Expected one of {get_args(OkunaTaskLocationType)}"
    )


OkunaTaskRegularity = Literal[
    "ONE_TIME", "ONCE_WEEK", "ONCE_MONTH", "REGULARLY", "PROJECT_BASED", "SEVERAL_TIMES_WEEK", "NEGOTIABLE"
]

TypesenseTaskRegularity = Literal["one-time", "recurring", "negotiable"]


def map_task_regularity_to_commitment_label(
    regularity: Optional[OkunaTaskRegularity],
) -> Optional[TypesenseTaskRegularity]:
    if not regularity:
        return None
    match regularity:
        case "ONE_TIME":
            return "one-time"
        case "ONCE_WEEK":
            return "recurring"
        case "ONCE_MONTH":
            return "recurring"
        case "SEVERAL_TIMES_WEEK":
            return "recurring"
        case "REGULARLY":
            return "recurring"
        case "PROJECT_BASED":
            return "negotiable"
        case "NEGOTIABLE":
            return "negotiable"
    raise ValueError(f"Unexpected regularity str '{regularity}'. Expected one of {get_args(OkunaTaskRegularity)}")
