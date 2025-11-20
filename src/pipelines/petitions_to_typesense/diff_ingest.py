from dataclasses import dataclass
from datetime import datetime, date

from prefect import task
from prefect.cache_policies import NONE

from src.common.external_data_pubsub_client import ExternalDataPubSubClient
from src.common.extractors.bq_extractor import (
    extract_deleted_records_in_partitions,
    extract_upserted_records_in_partitions,
)
from src.common.loaders.typesense_loader import upsert_typesense_documents
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.typesense_client import TypesenseClient
from src.pipelines.crawlers.innnit.types import PetitionRow
from src.pipelines.petitions_to_typesense.transform import petition_typesense_doc_id, petitions_to_typesense_docs
from src.pipelines.typesense import delete_typesense_documents


logger = PipelineLogger(__name__)


@dataclass
class ChangedRecords:
    new_or_updated: list[PetitionRow]
    deleted: list[PetitionRow]


@task(cache_policy=NONE)
def diff_ingest(
    table_name: str,
    last_complete_ingestion_timestamp: datetime | None,
    latest_partition_timestamp: date,
    typesense: TypesenseClient,
    pubsub: ExternalDataPubSubClient,
    typesense_collection: str,
) -> tuple[bool, bool, int]:
    changed_records = extract_changed_records(
        table_name,
        last_complete_ingestion_timestamp.date() if last_complete_ingestion_timestamp is not None else None,
        latest_partition_timestamp,
    )
    processed_items = 0

    new_or_updated_docs = petitions_to_typesense_docs(changed_records.new_or_updated)

    deletion_succeeded = True
    if changed_records.deleted:
        deleted_record_ids = [record["id"] for record in changed_records.deleted]
        document_ids_to_delete = [petition_typesense_doc_id(deleted_id) for deleted_id in deleted_record_ids]

        deletion_succeeded = delete_typesense_documents(typesense, typesense_collection, document_ids_to_delete)
        if deletion_succeeded:
            processed_items += len(document_ids_to_delete)
            # TODO: publish deletion of petition

    upsert_succeeded = upsert_typesense_documents(
        typesense_client=typesense,
        logger=logger,
        typesense_collection_name=typesense_collection,
        new_or_updated_documents=new_or_updated_docs,
        return_state=False,  # why mypy needs this to recognise the unwrapped return is beyond me
    )
    if upsert_succeeded:
        processed_items += len(new_or_updated_docs)

    return deletion_succeeded, upsert_succeeded, processed_items


def extract_changed_records(
    table_name: str,
    last_complete_ingestion_date: date | None,
    latest_partition_date: date,
) -> ChangedRecords:
    new_or_updated = extract_upserted_records_in_partitions(
        table_name,
        new_partition_date=latest_partition_date,
        old_partition_date=last_complete_ingestion_date,
        partition_time_column_name="_PARTITIONTIME",
    )

    deleted = extract_deleted_records_in_partitions(
        table_name,
        new_partition_date=latest_partition_date,
        old_partition_date=last_complete_ingestion_date,
        partition_time_column_name="_PARTITIONTIME",
    )

    return ChangedRecords(
        new_or_updated=new_or_updated,  # type: ignore[arg-type]
        deleted=deleted,  # type: ignore[arg-type]
    )
