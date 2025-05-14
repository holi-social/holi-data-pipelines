from dataclasses import dataclass
from logging import Logger
from typing import List, Dict, Any

import re
import yaml

from vertexai.preview.generative_models import GenerativeModel
from vertexai.generative_models import GenerationConfig

from shared.classifiers.topics_skills_classifier_v2 import TopicsSkillsClassifierV2, TopicsSkillsClassificationV2
from ratelimit import limits, sleep_and_retry

from src.pipelines.topics_v2 import topic_v2_titles, topic_v2_slugs
from src.pipelines.skills_v2 import skill_v2_titles, skill_v2_slugs

from tenacity import retry, stop_after_attempt

from src.pipelines.volunteering_classification.volunteering_classification_input import VolunteeringClassificationInput

MODEL_NAME = "gemini-2.0-flash-001"
MODEL_RATE_LIMIT_PER_MINUTE = 2000
MODEL_INVOCATION_RETRY_ATTEMPTS = 3

HTML_RE = re.compile(r"<[^>]+>")  # regex to remove HTML from descriptions
MAX_TITLE_LEN = 500
MAX_DESC_LEN = 10000
MAX_CATEGORIES_LEN = 500
MODEL_RESPONSE_OUTPUT_FORMAT = "YAML"
CONFIDENCE_MIN_VALUE = -1
CONFIDENCE_MAX_VALUE = 1

CONFIDENCE_THRESHOLD_TOPICS_V2 = 0.8  # if model confidence >= threshold, a topic slug is added to topics_v2
CONFIDENCE_THRESHOLD_SKILLS_V2 = 0.8  # if model confidence >= threshold, a skill slug is added to skills_v2

# Regexes to parse Gemini output
YAML_RE = re.compile(r"```yaml([\S\s]*)```")
# Gemini output contains ellipsis lines quite often
ELLIPSIS_RE = re.compile(r"\.\.\.\n")


@dataclass
class TermClassificationResult:
    embedding: List[float]
    slugs: List[str]


class VolunteeringTopicsSkillsClassifierV2(TopicsSkillsClassifierV2[VolunteeringClassificationInput]):
    model: GenerativeModel

    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)
        self.model = GenerativeModel(MODEL_NAME, generation_config=(GenerationConfig()))

    def get_classification(self, item: VolunteeringClassificationInput) -> TopicsSkillsClassificationV2:
        topics_result = self._get_topic_v2_classification(item)
        skills_result = self._get_skill_v2_classification(item)
        topics_skills_embedding_v2 = topics_result.embedding + skills_result.embedding
        return TopicsSkillsClassificationV2(
            topics_skills_embedding_v2=topics_skills_embedding_v2,
            topics_v2=topics_result.slugs,
            skills_v2=skills_result.slugs,
        )

    @retry(stop=stop_after_attempt(MODEL_INVOCATION_RETRY_ATTEMPTS))
    def _get_skill_v2_classification(self, item: VolunteeringClassificationInput) -> TermClassificationResult:
        prompt = self._create_skills_prompt_volunteering(item)
        response = self._invoke_model(prompt)
        self.logger.debug(f"raw skill classification response: {response}")
        return self._validate_and_transform_response(
            skill_v2_titles,
            skill_v2_slugs,
            CONFIDENCE_THRESHOLD_SKILLS_V2,
            response,
        )

    @retry(stop=stop_after_attempt(MODEL_INVOCATION_RETRY_ATTEMPTS))
    def _get_topic_v2_classification(self, item: VolunteeringClassificationInput) -> TermClassificationResult:
        prompt = self._create_topics_prompt_volunteering(item)
        response = self._invoke_model(prompt)
        self.logger.debug(f"raw topic classification response: {response}")
        return self._validate_and_transform_response(
            topic_v2_titles,
            topic_v2_slugs,
            CONFIDENCE_THRESHOLD_TOPICS_V2,
            response,
        )

    @staticmethod
    def _parse_yaml_response(response: str) -> Dict[str, Any]:
        match = YAML_RE.search(response)
        if not match:
            return yaml.safe_load(response)
        yaml_str = match.group(1)
        return yaml.safe_load(yaml_str)

    @staticmethod
    def _strip_ellipsis(response: str) -> str:
        return ELLIPSIS_RE.sub("", response)

    @staticmethod
    def _parse_response(response: str) -> Dict[str, Any]:
        cleaned_response = VolunteeringTopicsSkillsClassifierV2._strip_ellipsis(response)
        yaml_response = VolunteeringTopicsSkillsClassifierV2._parse_yaml_response(cleaned_response)
        if not isinstance(yaml_response, Dict):
            raise Exception(f"Expected dict, found {type(yaml_response)}")
        return yaml_response

    @staticmethod
    def _validate_weights(weights: Dict[str, Any], titles: List[str]) -> None:
        for weight in weights.values():
            if not (CONFIDENCE_MIN_VALUE <= weight <= CONFIDENCE_MAX_VALUE):
                raise Exception("invalid weight", weight)
        if len(weights) != len(titles):
            raise Exception("invalid number of weights", len(weights), len(titles))
        for title in titles:
            if title not in weights:
                raise Exception("missing title", title)

    @staticmethod
    def _transform_weights(
        weights: Dict[str, Any], titles: List[str], slugs: List[str], confidence_threshold: float
    ) -> TermClassificationResult:
        embedding = [weights[titles[i]] for i in range(len(titles))]
        slugs = list(
            map(lambda x: x[0], filter(lambda x: x[1] >= confidence_threshold, zip(slugs, embedding, strict=False)))
        )
        return TermClassificationResult(embedding, slugs)

    @staticmethod
    def _validate_and_transform_response(
        titles: List[str], slugs: List[str], confidence_threshold: float, response: str
    ) -> TermClassificationResult:
        try:
            weights = VolunteeringTopicsSkillsClassifierV2._parse_response(response)
            VolunteeringTopicsSkillsClassifierV2._validate_weights(weights, titles)
            return VolunteeringTopicsSkillsClassifierV2._transform_weights(
                weights, titles, slugs, confidence_threshold
            )
        except Exception as e:
            raise Exception(f"Invalid response:\n{str(response)}") from e

    @staticmethod
    def _create_skills_prompt_volunteering(item: VolunteeringClassificationInput) -> str:
        title = VolunteeringTopicsSkillsClassifierV2._shorten_title(item.title)
        desc = VolunteeringTopicsSkillsClassifierV2._shorten_description(item.description)
        # Description can be empty
        desc_part = f' und der Beschreibung "{desc}"' if desc else ""
        categories = VolunteeringTopicsSkillsClassifierV2._join_categories(item.uses or [])
        # Categories might be empty
        categories_part = f" Das Freiwilligenprojekt ist den Kategorien {categories} zugeordnet." if categories else ""
        titles_list = "\n".join(map(lambda skill_title: f"- {skill_title}", skill_v2_titles))
        return f"""
        Dies ist ein Freiwilligenprojekt mit dem Titel "{title}"{desc_part}.{categories_part}
        Deine Aufgabe ist es, für die folgenden Fähigkeiten eine Gewichtung zu diesem Freiwilligenprojekt zwischen {CONFIDENCE_MIN_VALUE} und {CONFIDENCE_MAX_VALUE} ({CONFIDENCE_MIN_VALUE} für nicht relevant, {CONFIDENCE_MAX_VALUE} für relevant) zu bestimmen:
        {titles_list}
        Gib die Fähigkeiten inklusive Gewichtung im {MODEL_RESPONSE_OUTPUT_FORMAT}-Format ohne Erklärung zurück.
        """

    @staticmethod
    def _create_topics_prompt_volunteering(item: VolunteeringClassificationInput) -> str:
        title = VolunteeringTopicsSkillsClassifierV2._shorten_title(item.title)
        titles_list = "\n".join(map(lambda topic_title: f"- {topic_title}", topic_v2_titles))
        desc = VolunteeringTopicsSkillsClassifierV2._shorten_description(item.description)
        # Description can be empty
        desc_part = f' und der Beschreibung "{desc}"' if desc else ""
        categories = VolunteeringTopicsSkillsClassifierV2._join_categories(item.uses or [])
        # Categories might be empty
        categories_part = f" Das Freiwilligenprojekt ist den Kategorien {categories} zugeordnet." if categories else ""
        return f"""
        Dies ist ein Freiwilligenprojekt mit dem Titel "{title}"{desc_part}.{categories_part}
        Deine Aufgabe ist es, für die folgenden Themen eine Gewichtung zu diesem Freiwilligenprojekt zwischen {CONFIDENCE_MIN_VALUE} und {CONFIDENCE_MAX_VALUE} ({CONFIDENCE_MIN_VALUE} für unpassend, {CONFIDENCE_MAX_VALUE} für passend) zu bestimmen:
        {titles_list}
        Gib die Themen inklusive Gewichtung im {MODEL_RESPONSE_OUTPUT_FORMAT}-Format ohne Erklärung zurück.
        """

    @staticmethod
    def _shorten_title(title: str) -> str:
        if len(title) <= MAX_TITLE_LEN:
            return title.strip()
        return title[: MAX_TITLE_LEN - 3].strip() + "..."

    @staticmethod
    def _shorten_description(description: str) -> str:
        lines = HTML_RE.sub("", description).split("\n")
        full_desc = " ".join(lines)
        if len(full_desc) <= MAX_DESC_LEN:
            return full_desc.strip()
        first_line = lines[0]
        if len(first_line) <= MAX_DESC_LEN:
            return first_line.strip()
        return first_line[: MAX_DESC_LEN - 3].strip() + "..."

    @staticmethod
    def _join_categories(categories: List[str]) -> str:
        joined_categories = ", ".join(categories)
        if len(joined_categories) <= MAX_CATEGORIES_LEN:
            return joined_categories
        return joined_categories[: MAX_CATEGORIES_LEN - 3] + "..."

    @sleep_and_retry  # type: ignore
    @limits(calls=MODEL_RATE_LIMIT_PER_MINUTE, period=60)  # type: ignore
    def _invoke_model(self, prompt: str) -> str:
        """This blocks_invoke_model the thread until the next request can be performed"""
        response = self.model.generate_content(prompt)
        return response.text
