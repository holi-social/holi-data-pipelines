from typing import Any, cast, Dict

from prefect import flow, task
from prefect.states import Failed, State, Completed

from src.common.extractors.bq_extractor import extract_data_from_bigquery
from src.common.loaders.typesense_loader import upsert_typesense_documents
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.typesense_client import TypesenseClient
from src.common.variables import variable_value, secret_value

logger = PipelineLogger(__name__)

UserRow = dict[str, str | None]
UserDoc = dict[str, Any | None]

@task
def load_users() -> list[UserRow]:
    return extract_data_from_bigquery("""
        SELECT
          user.username as user_id,
          profile.name as first_name,
          profile.last_name,
          profile.location,
          profile.avatar,
          profile.avatar_blurhash,
          profile.about_me
        FROM
          `public.openbook_auth_user` user
        LEFT OUTER JOIN
          `public.openbook_auth_userprofile` profile
        ON
          user.id=profile.user_id
        WHERE
          user.is_deleted=false
    """)

def user_to_doc(user: UserRow) -> UserDoc:
    full_name = f"{user.get("first_name")} {user.get('last_name')}"
    return {
        "id": f"profile_{str(user.get("user_id"))}",
        "type": "profile",
        "title_de": full_name,
        "title_en": full_name,
        "description_de": user.get("about_me"),
        "description_en": user.get("about_me"),
        "location": user.get("location"),
        "location_lat_lng": None,
        "image_url": user.get("avatar"),
        "link_locators": {
            "profile": user.get("user_id")
        },
    }

@task
def users_to_typesense_docs(users: list[UserRow]) -> list[UserDoc]:
    return [user_to_doc(user) for user in users]

@flow
def profiles_to_typesense_ingest_from_scratch() -> State | None:
    config = cast(Dict[str, str], {
        "api_key": secret_value("typesense-admin-api-key"),
        "host": variable_value("typesense_host"),
        "port": variable_value("typesense_port"),
        "protocol": variable_value("typesense_protocol"),
        "collection": variable_value("typesense_collection"),
    })
    typesense_client = TypesenseClient(config)

    users = load_users()
    logger.debug("Found %s users", len(users))

    user_documents = users_to_typesense_docs(users)
    logger.debug("Transformed %s documents", len(user_documents))
    
    upsert_succeeded = upsert_typesense_documents(typesense_client, config["collection"], user_documents)
    if not upsert_succeeded:
        return Failed(message="Some documents failed ingestion. Please see error log.")
    else:
        return Completed(message=f"All {len(user_documents)} documents successfully ingested.")
