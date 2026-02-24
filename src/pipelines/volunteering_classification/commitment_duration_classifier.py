import json
import re
from logging import Logger
from typing import Any, cast, Dict

from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt
from vertexai.generative_models import GenerationConfig, GenerativeModel

from shared.classifiers.commitment_duration_classifier import (
    CommitmentDurationClassifier,
    CommitmentDurationClassification,
    parse_commitment_duration,
)
from src.common.str_utils import shorten_str, remove_html
from src.pipelines.volunteering_classification.voltastics_classification_input import VoltasticsClassificationInput

MODEL_NAME = "gemini-2.5-flash"
MODEL_RATE_LIMIT_PER_MINUTE = 2000
MODEL_INVOCATION_RETRY_ATTEMPTS = 3

DURATIONS_CONSTANTS_TO_HUMAN_READABLE = {
    "MINUTES_10to15": "Less than 15 minutes",
    "MINUTES_15to30": "15 to 30 minutes",
    "MINUTES_30to60": "30 minutes – 1 hour",
    "HOURS_1to2": "1–2 hours",
    "HOURS_2to4": "2–4 hours",
    "HOURS_4to6": "4–6 hours",
    "DAYS_1": "1 day",
    "DAYS_2": "2 days",
    "DAYS_3_OR_MORE": "3 days or more",
    "INDETERMINATE": "Unclear / Not enough info",
}

DURATIONS_HUMAN_READABLE_TO_CONSTANTS = {v: k for k, v in DURATIONS_CONSTANTS_TO_HUMAN_READABLE.items()}

HTML_RE = re.compile(r"<[^>]+>")

# maximum title and description length were constrained once upon a time when the first prototype for classification VOs
# was written. the main intention was to ensure we don't burn through too many credits. data inspection back then showed
# that the few data items that had very long descriptions typically didn't contain valuable information for
# classification anyway. also, the assumption is that users wouldn't read such long descriptions in the first place.
MAX_TITLE_LEN = 500
MAX_DESC_LEN = 10000


class VolunteeringCommitmentDurationClassifier(CommitmentDurationClassifier[VoltasticsClassificationInput]):
    model: GenerativeModel

    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)
        self.model = GenerativeModel(MODEL_NAME, generation_config=GenerationConfig(temperature=0.5))

    def get_classification(self, item: VoltasticsClassificationInput) -> CommitmentDurationClassification:
        prompt = self._create_classification_prompt(item)
        return self._invoke_and_parse_with_retry(prompt, item.id)

    @retry(stop=stop_after_attempt(MODEL_INVOCATION_RETRY_ATTEMPTS))
    def _invoke_and_parse_with_retry(self, prompt: str, item_id: str) -> CommitmentDurationClassification:
        response = self._invoke_model(prompt)
        self.logger.debug(f"[id={item_id}] raw time requirement response: {response}")
        return self._parse_and_validate_response(response)

    @sleep_and_retry  # type: ignore[misc, untyped-decorator]
    @limits(calls=MODEL_RATE_LIMIT_PER_MINUTE, period=60)  # type: ignore[misc, untyped-decorator]
    def _invoke_model(self, prompt: str) -> str:
        response = self.model.generate_content(prompt)
        return response.text

    def _create_classification_prompt(self, item: VoltasticsClassificationInput) -> str:
        """Create the classification prompt for time requirement classification."""
        title = shorten_str(item.title, MAX_TITLE_LEN)
        description = shorten_str(remove_html(item.description), MAX_DESC_LEN)

        # noqa: S608 - This is an AI prompt template, not SQL code
        prompt = f"""Role: AI Text Analyzer skilled in interpreting and estimating volunteer time commitments.

        Goal: Analyze a volunteering opportunity based on its Title and Description. Estimate the most likely `Time required` for a typical session, even if not explicitly stated. Prioritize estimation over selecting 'Unclear / Not enough info'.

        Input: Opportunity data (id, title, description).

        Instructions:

        1.  **Analyze:** Read the Title and Description carefully. Ignore HTML, focus on textual content describing the activity.
        2.  **Identify Explicit Time Clues:** First, search for specific durations, time ranges, shift lengths (e.g., "2 hours", "10am-12pm", "weekend", "monthly meeting").
        3.  **Classify Time (Estimate if Needed):**
            *   **If explicit time clues exist:** Use them to select the best-fitting `Time required` category. If a range is given (e.g., "2–4 hours"), choose the category covering the lower or most typical part of the range based on context.
            *   **If NO explicit time clues exist:**
                *   Examine the *nature of the task described* in the Title and Description (e.g., "attend a meeting", "write letters", "staff an event table", "clean up a park").
                *   Based *only* on the described task, make a **reasonable estimate** of a typical single session's duration. Consider common sense durations for such activities.
                *   Select the `Time required` category that best fits your estimate. Lean towards shorter, common durations unless the description implies significant effort (e.g., "deep clean", "full event support").
            *   **Use `Unclear / Not enough info` ONLY as a last resort** if the description is too vague to even make a reasonable estimate about the task's duration (e.g., "flexible help needed", "various tasks").
        4.  **Format Output:** Create a JSON object containing the `id`, `reasoning` field explaining the time requirement selection, and the chosen category.

        Categories - `Time required`:
        {"\n".join(f"*   `{cat}`" for cat in DURATIONS_CONSTANTS_TO_HUMAN_READABLE.values())}

        Output JSON Format:
        ```json
        {{
        "id": "id",
        "reasoning": "Explain the Time requirement selection. State if based on explicit text OR estimation from the described task. E.g., Explicit: 'Text states '10am-12pm', fitting '1–2 hours'.' Estimated: 'No duration given. Estimated '2–4 hours' based on typical time for 'staffing an event table' described.'",
        "Time required": "[Selected Time Category]"
        }}

        Strict Constraints:

        * Base analysis only on provided Title and Description.
        * Use category names exactly as listed.
        * Reasoning must focus on Time requirement decision and directly reference text clues.
        * Output must match the specified JSON structure precisely.

        Now, classify the following opportunity:

        Opportunity:
        id: {item.id}
        Title: {title}
        Description: {description}

        Output:"""  # noqa: S608

        self.logger.debug(f'[id={item.id}] created prompt for title="{title}" and description="{description}"')

        return prompt

    @staticmethod
    def _parse_and_validate_response(response: str) -> CommitmentDurationClassification:
        clean_json = VolunteeringCommitmentDurationClassifier._clean_json_response(response)
        parsed_data = json.loads(clean_json)

        VolunteeringCommitmentDurationClassifier._validate_response_structure(parsed_data)

        commitment_duration_parsed = DURATIONS_HUMAN_READABLE_TO_CONSTANTS[cast(str, parsed_data["Time required"])]
        commitment_duration = parse_commitment_duration(commitment_duration_parsed)

        reasoning = cast(str, parsed_data["reasoning"])

        return CommitmentDurationClassification(
            commitment_duration=commitment_duration,
            commitment_duration_ai_reasoning=reasoning,
        )

    @staticmethod
    def _clean_json_response(response: str) -> str:
        lines = response.strip().splitlines()

        if lines and lines[0].strip().startswith("```json"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]

        return "\n".join(lines)

    @staticmethod
    def _validate_str_field_exists(parsed_data: Dict[str, Any], field_name: str) -> None:
        if not parsed_data.get(field_name):
            raise ValueError(f'Missing field "{field_name}" in response')
        if not isinstance(parsed_data.get(field_name), str):
            raise ValueError(
                f'Expected field "{field_name}" to be a `str`, found `{type(parsed_data.get(field_name))}`'
            )

    @staticmethod
    def _validate_response_structure(parsed_data: Any) -> None:
        if not isinstance(parsed_data, dict):
            raise ValueError("Response is not a JSON object")
        VolunteeringCommitmentDurationClassifier._validate_str_field_exists(parsed_data, "Time required")
        VolunteeringCommitmentDurationClassifier._validate_str_field_exists(parsed_data, "reasoning")
