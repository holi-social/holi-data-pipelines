from dataclasses import dataclass, asdict
from typing import cast, List, Dict, TypeAlias, Any

from google.cloud.bigquery import Client
from prefect.states import State, Completed
from prefect_gcp import GcpCredentials
import re
import yaml

import vertexai
from vertexai.preview.generative_models import GenerativeModel, GenerationConfig

from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from ratelimit import limits, sleep_and_retry

from src.common.variables import variable_value
from src.pipelines.topics_v2 import topic_v2_titles, topic_v2_slugs
from src.pipelines.skills_v2 import skill_v2_titles, skill_v2_slugs

from tenacity import retry, stop_after_attempt, RetryError

Item: TypeAlias = Any

STOP_AFTER_BATCH = 0
PROJECT_ID = "holi-shared"  # TODO via variables
ENVIRONMENT = cast(str, variable_value("prefect_environment"))
BQ_DATALAKE = f"{PROJECT_ID}.datalake_{'development' if ENVIRONMENT == 'staging' else ENVIRONMENT}"
BQ_DATAPOOL = f"{PROJECT_ID}.datapool_{'development' if ENVIRONMENT == 'staging' else ENVIRONMENT}"
TABLE_NAME_CLASSIFICATIONS = f"{BQ_DATALAKE}.volunteering_voltastics_classification_v2"
TABLE_NAME_UPDATES = f"{BQ_DATAPOOL}.volunteering_voltastics_classification_updates_v2"
MODEL_NAME = "gemini-2.0-flash-001"
GCLOUD_LOCATION = "europe-north1"  # TODO via variables ?
GEMINI_RATE_LIMIT_PER_MINUTE = 2000
BATCH_SIZE = 1000
RETRY_ATTEMPTS = 3
ENGAGEMENT_TYPE = "volunteering_voltastics"

# Regex to remove HTML from descriptions
HTML_RE = re.compile(r"<[^>]+>")
MAX_TITLE_LEN = 500
MAX_DESC_LEN = 10000
MAX_CATEGORIES_LEN = 500
OUTPUT_FORMAT = "YAML"
MIN_VALUE = -1
MAX_VALUE = 1
THRESHOLD_TOPICS = 0.9
THRESHOLD_SKILLS = 0.9

# Regexes to parse Gemini output
YAML_RE = re.compile(r"```yaml([\S\s]*)```")
# Gemini output contains ellipsis lines quite often
ELLIPSIS_RE = re.compile(r"\.\.\.\n")

logger = PipelineLogger(__name__)


@dataclass
class TermClassificationResult:
    embedding: List[float]
    slugs: List[str]


@dataclass
class ClassificationResult:
    id: str
    hash: str
    embedding_array: List[float]


def compute_topics_skills_classifications_v2() -> None:
    logger.info(f"Starting topics & skills V2 classification for volunteering in {ENVIRONMENT}")

    vertexai.init(project=PROJECT_ID, location=GCLOUD_LOCATION)
    generation_config = GenerationConfig()
    model = GenerativeModel(MODEL_NAME, generation_config=generation_config)

    # noqa: S608
    # noqa: S608
    pool = ThreadPoolExecutor()

    batch_nr = 0

    bqclient = GcpCredentials(project=PROJECT_ID).get_bigquery_client(project=PROJECT_ID, location=GCLOUD_LOCATION)
    bqclient_writing = GcpCredentials(project=PROJECT_ID).get_bigquery_client(
        project=PROJECT_ID, location=GCLOUD_LOCATION
    )

    while not STOP_AFTER_BATCH or batch_nr < STOP_AFTER_BATCH:  # TODO remove STOP_AFTER_BATCH after testing
        logger.info(f"Batch {batch_nr} - starting batch (of size {BATCH_SIZE})")

        total_items_count_result = bqclient.query(f"SELECT count(*) FROM `{TABLE_NAME_UPDATES}`").result()  # noqa: S608
        total_items_count = next(total_items_count_result)[0]
        logger.info(f"Batch {batch_nr} - found {total_items_count} items to classify")

        # exit early if there are no items to classify
        if total_items_count == 0:
            logger.info(f"Batch {batch_nr} - all items classified")
            break  # exit while loop

        batch_items = bqclient.query(f"SELECT * FROM `{TABLE_NAME_UPDATES}` LIMIT {BATCH_SIZE}").result()  # noqa: S608
        logger.info(f"Batch {batch_nr} - starting classification for {batch_items.total_rows} items")

        classify_and_store_item = lambda item, batch_nr: _classify_and_store_item(  # noqa: E731
            model,
            TABLE_NAME_CLASSIFICATIONS,
            batch_nr,
            item,
            bqclient_writing,
        )
        futures = [pool.submit(classify_and_store_item, item, batch_nr) for item in batch_items]
        wait(futures, return_when=ALL_COMPLETED)

        logger.info(f"Batch {batch_nr} - finished classification of {batch_items.total_rows} items")
        batch_nr += 1

    logger.info("Finished topics & skills V2 classification of volunteering_voltastics")


def _classify_and_store_item(
    model: GenerativeModel, table_name_classifications: str, batch_nr: int, item: Item, bqclient_writing: Client
) -> None:
    logger.debug(f"Batch {batch_nr} - item id={item.id} - classifying item")
    try:
        classification = _classify_item_v2(model, batch_nr, item)
        if classification is None:
            logger.debug(f"Batch {batch_nr} - item id={item.id} - failed to classify item, skipping")
            return
        errors = bqclient_writing.insert_rows_json(table_name_classifications, [asdict(classification)])
        logger.debug(f"Batch {batch_nr} - item id={item.id} - written to BigQuery")
        if len(errors) > 0:
            raise Exception(f"Failed to insert classification {classification}: {errors}")
    except Exception as e:
        logger.error(
            f"Error while classifying item id={item.id}, skipping, Error: %s",
            str(e),
            exc_info=e,
        )


def _classify_item_v2(model: GenerativeModel, batch_nr: int, item: Item) -> ClassificationResult | None:
    try:
        topics_result = _get_topic_classification(model, item)
        skills_result = _get_skill_classification(model, item)
        embedding_array = topics_result.embedding + skills_result.embedding
        return ClassificationResult(item.id, item.hash, embedding_array)
    except RetryError as e:
        last_attempt = e.last_attempt
        last_response = last_attempt.result() if last_attempt.result() else "No response"
        logger.error(
            f"Batch {batch_nr} - item {item.id} - failed to classify after {RETRY_ATTEMPTS} retries. Last Gemini response: {last_response}.",
            exc_info=e,
        )
        return None
    except Exception as e:
        logger.error(f"Batch {batch_nr} - item {item.id} - failed to classify", exc_info=e)
        return None


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS))
def _get_skill_classification(model: GenerativeModel, item: Item) -> TermClassificationResult:
    prompt = _create_skills_prompt_volunteering(item)
    response = _invoke_model(model, prompt)
    logger.debug(f"raw skill classification response: {response}")
    return _validate_and_transform_response(
        skill_v2_titles,
        skill_v2_slugs,
        THRESHOLD_SKILLS,
        response,
    )


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS))
def _get_topic_classification(model: GenerativeModel, item: Item) -> TermClassificationResult:
    prompt = _create_topics_prompt_volunteering(item)
    response = _invoke_model(model, prompt)
    logger.debug(f"raw topic classification response: {response}")
    return _validate_and_transform_response(
        topic_v2_titles,
        topic_v2_slugs,
        THRESHOLD_TOPICS,
        response,
    )


def _parse_yaml_response(response: str) -> Dict[str, Any]:
    match = YAML_RE.search(response)
    if not match:
        return yaml.safe_load(response)
    yaml_str = match.group(1)
    return yaml.safe_load(yaml_str)


def _strip_ellipsis(response: str) -> str:
    return ELLIPSIS_RE.sub("", response)


def _parse_response(response: str) -> Dict[str, Any]:
    cleaned_response = _strip_ellipsis(response)
    yaml_response = _parse_yaml_response(cleaned_response)
    if not isinstance(yaml_response, Dict):
        raise Exception(f"Expected dict, found {type(yaml_response)}")
    return yaml_response


def _validate_weights(weights: Dict[str, Any], titles: List[str]) -> None:
    for weight in weights.values():
        if not (MIN_VALUE <= weight <= MAX_VALUE):
            raise Exception("invalid weight", weight)
    if len(weights) != len(titles):
        raise Exception("invalid number of weights", len(weights), len(titles))
    for title in titles:
        if title not in weights:
            raise Exception("missing title", title)


def _transform_weights(
    weights: Dict[str, Any], titles: List[str], slugs: List[str], threshold: float
) -> TermClassificationResult:
    embedding = [weights[titles[i]] for i in range(len(titles))]
    slugs = list(map(lambda x: x[0], filter(lambda x: x[1] >= threshold, zip(slugs, embedding))))
    return TermClassificationResult(embedding, slugs)


def _validate_and_transform_response(
    titles: List[str], slugs: List[str], threshold: float, response: str
) -> TermClassificationResult:
    try:
        weights = _parse_response(response)
        _validate_weights(weights, titles)
        return _transform_weights(weights, titles, slugs, threshold)
    except Exception as e:
        raise Exception(f"Invalid response:\n{str(response)}") from e


def _create_skills_prompt_volunteering(item: Item) -> str:
    title = _shorten_title(item.title)
    desc = _shorten_description(item.description)
    # Description can be empty
    desc_part = f' und der Beschreibung "{desc}"' if desc else ""
    categories = _join_categories(item.uses)
    # Categories might be empty
    categories_part = f" Das Freiwilligenprojekt ist den Kategorien {categories} zugeordnet." if categories else ""
    titles_list = "\n".join(map(lambda skill_title: f"- {skill_title}", skill_v2_titles))
    return f"""
    Dies ist ein Freiwilligenprojekt mit dem Titel "{title}"{desc_part}.{categories_part}
    Deine Aufgabe ist es, für die folgenden Fähigkeiten eine Gewichtung zu diesem Freiwilligenprojekt zwischen {MIN_VALUE} und {MAX_VALUE} ({MIN_VALUE} für nicht relevant, {MAX_VALUE} für relevant) zu bestimmen:
    {titles_list}
    Gib die Fähigkeiten inklusive Gewichtung im {OUTPUT_FORMAT}-Format ohne Erklärung zurück.
    """


def _create_topics_prompt_volunteering(item: Item) -> str:
    title = _shorten_title(item.title)
    titles_list = "\n".join(map(lambda topic_title: f"- {topic_title}", topic_v2_titles))
    desc = _shorten_description(item.description)
    # Description can be empty
    desc_part = f' und der Beschreibung "{desc}"' if desc else ""
    categories = _join_categories(item.uses)
    # Categories might be empty
    categories_part = f" Das Freiwilligenprojekt ist den Kategorien {categories} zugeordnet." if categories else ""
    return f"""
    Dies ist ein Freiwilligenprojekt mit dem Titel "{title}"{desc_part}.{categories_part}
    Deine Aufgabe ist es, für die folgenden Themen eine Gewichtung zu diesem Freiwilligenprojekt zwischen {MIN_VALUE} und {MAX_VALUE} ({MIN_VALUE} für unpassend, {MAX_VALUE} für passend) zu bestimmen:
    {titles_list}
    Gib die Themen inklusive Gewichtung im {OUTPUT_FORMAT}-Format ohne Erklärung zurück.
    """


def _shorten_title(title: str) -> str:
    if len(title) <= MAX_TITLE_LEN:
        return title.strip()
    return title[: MAX_TITLE_LEN - 3].strip() + "..."


def _shorten_description(description: str) -> str:
    lines = HTML_RE.sub("", description).split("\n")
    full_desc = " ".join(lines)
    if len(full_desc) <= MAX_DESC_LEN:
        return full_desc.strip()
    first_line = lines[0]
    if len(first_line) <= MAX_DESC_LEN:
        return first_line.strip()
    return first_line[: MAX_DESC_LEN - 3].strip() + "..."


def _join_categories(categories: List[str]) -> str:
    joined_categories = ", ".join(categories)
    if len(joined_categories) <= MAX_CATEGORIES_LEN:
        return joined_categories
    return joined_categories[: MAX_CATEGORIES_LEN - 3] + "..."


@sleep_and_retry  # type: ignore
@limits(calls=GEMINI_RATE_LIMIT_PER_MINUTE, period=60)  # type: ignore
def _invoke_model(model: GenerativeModel, prompt: str) -> str:
    """This blocks_invoke_model the thread until the next request can be performed"""
    response = model.generate_content(prompt)
    return response.text


@monitored_flow
def volunteering_classification() -> State:
    compute_topics_skills_classifications_v2()
    return Completed()


if __name__ == "__main__":
    volunteering_classification()
