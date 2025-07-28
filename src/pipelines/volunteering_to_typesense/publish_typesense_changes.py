from src.common.events.VolunteeringDeletedEvent import VolunteeringDeletedEvent
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.pubsub_client import PubSubClient

logger = PipelineLogger(__name__)


def publish_volunteering_deletion(pubsub_client: PubSubClient, deleted_record_ids: list[str]) -> bool:
    try:
        publish_futures = []

        for record_id in deleted_record_ids:
            event = VolunteeringDeletedEvent(str(record_id))
            future = pubsub_client.publish(event)
            publish_futures.append(future)

        for future in publish_futures:
            future.result()

        logger.info(f"Published {len(deleted_record_ids)} VolunteeringDeletedEvent(s)")
        return True
    except Exception as e:
        logger.error(
            f"Error publishing VolunteeringDeletedEvent(s): {str(e)}\n"
            f"Affected document ids: {','.join([str(doc_id) for doc_id in deleted_record_ids])}"
        )
        return False
