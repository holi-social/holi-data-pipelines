from typing import Any, cast

from prefect import task
from prefect.cache_policies import NONE
from prefect.states import Failed, State, Completed

from src.common.extractors.bq_extractor import extract_data_from_bigquery
from src.common.loaders.typesense_loader import upsert_typesense_documents
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.alerting import alerting_flow
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.common.variables import variable_value

logger = PipelineLogger(__name__)

UserRow = dict[str, str | None]
UserDoc = dict[str, Any | None]


@task(cache_policy=NONE)  # type: ignore[arg-type]
def load_users(source_dataset: str) -> list[UserRow]:
    return extract_data_from_bigquery(f"""
        SELECT
          user.username as user_id,
          profile.name as first_name,
          profile.last_name,
          profile.location,
          profile.avatar,
          profile.avatar_blurhash,
        FROM
          `{source_dataset}.openbook_auth_user` user
        LEFT OUTER JOIN
          `{source_dataset}.openbook_auth_userprofile` profile
        ON
          user.id=profile.user_id
        WHERE
          user.is_deleted=false
    """)  # noqa: S608


def user_to_doc(user: UserRow) -> UserDoc:
    user_full_name = f"{user.get('first_name')} {user.get('last_name')}"
    return {
        "id": f"profile_{str(user.get('user_id'))}",
        "type": "profile",
        # deleted description fields as they'd creep into embeddings and mess up search results
        "location": user.get("location"),
        # omitting optional fields set to None to reduce document size
        "image_url": user.get("avatar"),
        "link_locators": {"profile": user.get("user_id")},
        "user_full_name": user_full_name,
    }


@task(cache_policy=NONE)  # type: ignore[arg-type]
def users_to_typesense_docs(users: list[UserRow]) -> list[UserDoc]:
    return [user_to_doc(user) for user in users]


@alerting_flow
def profiles_to_typesense_ingest_from_scratch_flow() -> State:
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())
    typesense_collection = cast(str, variable_value("typesense_collection"))
    source_dataset = cast(str, variable_value("bigquery_okuna_dataset"))
    return profiles_to_typesense_ingest_from_scratch(typesense_client, source_dataset, typesense_collection)


@task(cache_policy=NONE)  # type: ignore[arg-type]
def profiles_to_typesense_ingest_from_scratch(
    typesense_client: TypesenseClient, source_dataset: str, collection: str
) -> State:
    logger.info(f"Starting profiles ingestion from scratch into typesense collection '{collection}'")

    users = load_users(source_dataset)
    logger.info("Found %s users", len(users))

    user_documents = users_to_typesense_docs(users)
    logger.info("Transformed %s documents", len(user_documents))

    upsert_succeeded = upsert_typesense_documents(typesense_client, logger, collection, user_documents)
    if not upsert_succeeded:
        return Failed(message="Some documents failed ingestion. Please see error log.")
    else:
        return Completed(message=f"All {len(user_documents)} documents successfully ingested.")
