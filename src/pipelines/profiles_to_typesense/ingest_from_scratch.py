from typing import Any, cast

from prefect import task
from prefect.cache_policies import NONE
from prefect.states import Failed, State, Completed

from src.common.extractors.bq_extractor import extract_data_from_bigquery
from src.common.loaders.typesense_loader import upsert_typesense_documents
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from src.common.monitoring.metrics import report_number_of_processed_entities
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.common.variables import variable_value
from src.pipelines.bigquery import bigquery_okuna_dataset

logger = PipelineLogger(__name__)

UserRow = dict[str, str | None]
UserDoc = dict[str, Any | None]


@task(cache_policy=NONE)  # type: ignore[arg-type]
def load_users() -> list[UserRow]:
    return extract_data_from_bigquery(f"""
        SELECT
          user.username as user_id,
          profile.name as first_name,
          profile.last_name,
          profile.location,
          profile.avatar,
          profile.avatar_blurhash,
        FROM
          `{bigquery_okuna_dataset()}.openbook_auth_user` user
        LEFT OUTER JOIN
          `{bigquery_okuna_dataset()}.openbook_auth_userprofile` profile
        ON
          user.id=profile.user_id
        WHERE
          user.is_deleted=false
          AND profile IS NOT NULL
          AND user.identity IS NOT NULL
    """)  # noqa: S608


def user_to_doc(user: UserRow) -> UserDoc | None:
    def empty(string: str | None) -> bool:
        return string is None or string.strip() == ""

    if empty(user.get("first_name")) and empty(user.get("last_name")):
        return None

    user_names = [user.get("first_name"), user.get("last_name")]
    user_full_name = " ".join(filter(lambda s: s != "", map(str.strip, filter(None, user_names))))
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


def users_to_typesense_docs(users: list[UserRow]) -> list[UserDoc]:
    return [user_doc for user_doc in map(user_to_doc, users) if user_doc is not None]


@monitored_flow
def profiles_to_typesense_ingest_from_scratch_flow() -> State:
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())
    typesense_collection = cast(str, variable_value("typesense_collection"))
    task_result = profiles_to_typesense_ingest_from_scratch(typesense_client, typesense_collection)
    if isinstance(task_result, State):
        return task_result
    elif isinstance(task_result, int):
        report_number_of_processed_entities(task_result)
        return Completed()
    else:
        raise ValueError(f"Unexpected task result: {task_result}")


@task(cache_policy=NONE)  # type: ignore[arg-type]
def profiles_to_typesense_ingest_from_scratch(typesense_client: TypesenseClient, collection: str) -> State | int:
    logger.info(f"Starting profiles ingestion from scratch into typesense collection '{collection}'")

    users = load_users()
    logger.info("Found %s users", len(users))

    user_documents = users_to_typesense_docs(users)
    logger.info("Transformed %s documents", len(user_documents))

    upsert_succeeded = upsert_typesense_documents(typesense_client, logger, collection, user_documents)
    if not upsert_succeeded:
        return Failed(message="Some documents failed ingestion. Please see error log.")
    else:
        return len(user_documents)


if __name__ == "__main__":
    profiles_to_typesense_ingest_from_scratch_flow()
