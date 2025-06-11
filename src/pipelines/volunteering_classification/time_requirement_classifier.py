import json
import re
from logging import Logger
from typing import Any

from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt
from vertexai.generative_models import GenerationConfig, GenerativeModel

from shared.classifiers.time_requirement_classifier import TimeRequirementClassifier, TimeRequirementClassification
from src.pipelines.volunteering_classification.volunteering_classification_input import VolunteeringClassificationInput

MODEL_NAME = "gemini-2.5-flash-preview-05-20"
MODEL_RATE_LIMIT_PER_MINUTE = 2000
MODEL_INVOCATION_RETRY_ATTEMPTS = 3

TIME_REQUIREMENT_CATEGORIES = [
    "Less than 30 minutes",
    "30 minutes – 1 hour",
    "1–2 hours",
    "2–4 hours",
    "4–6 hours",
    "1 day",
    "More than 1 day",
    "Unclear / Not enough info",
]

HTML_RE = re.compile(r"<[^>]+>")
MAX_TITLE_LEN = 500
MAX_DESC_LEN = 10000


class VolunteeringTimeRequirementClassifier(TimeRequirementClassifier[VolunteeringClassificationInput]):
    model: GenerativeModel

    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)
        self.model = GenerativeModel(MODEL_NAME, generation_config=GenerationConfig(temperature=0.5))

    def get_classification(self, item: VolunteeringClassificationInput) -> TimeRequirementClassification:
        prompt = self._create_classification_prompt(item)
        return self._invoke_and_parse_with_retry(prompt, item.id)

    @retry(stop=stop_after_attempt(MODEL_INVOCATION_RETRY_ATTEMPTS))
    def _invoke_and_parse_with_retry(self, prompt: str, item_id: str) -> TimeRequirementClassification:
        response = self._invoke_model(prompt)
        self.logger.debug(f"[id={item_id}] raw time requirement response: {response}")
        return self._parse_and_validate_response(response, item_id)

    @sleep_and_retry  # type: ignore[misc]
    @limits(calls=MODEL_RATE_LIMIT_PER_MINUTE, period=60)  # type: ignore[misc]
    def _invoke_model(self, prompt: str) -> str:
        response = self.model.generate_content(prompt)
        return response.text

    @staticmethod
    def _create_classification_prompt(item: VolunteeringClassificationInput) -> str:
        """Create the classification prompt for time requirement classification."""
        title = VolunteeringTimeRequirementClassifier._shorten_title(item.title)
        description = VolunteeringTimeRequirementClassifier._shorten_description(item.description)

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
        {"\n".join(f"*   `{cat}`" for cat in TIME_REQUIREMENT_CATEGORIES)}

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

        return prompt

    @staticmethod
    def _parse_and_validate_response(response: str, item_id: str) -> TimeRequirementClassification:
        clean_json = VolunteeringTimeRequirementClassifier._clean_json_response(response)
        parsed_data = json.loads(clean_json)

        VolunteeringTimeRequirementClassifier._validate_response_structure(parsed_data)

        time_required = parsed_data["Time required"]
        reasoning = parsed_data["reasoning"]

        VolunteeringTimeRequirementClassifier._validate_time_category(time_required)

        return TimeRequirementClassification(time_required=time_required, time_reasoning=reasoning)

    @staticmethod
    def _clean_json_response(response: str) -> str:
        lines = response.strip().splitlines()

        if lines and lines[0].strip().startswith("```json"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]

        return "\n".join(lines)

    @staticmethod
    def _validate_response_structure(parsed_data: Any) -> None:
        if not isinstance(parsed_data, dict):
            raise ValueError("Response is not a JSON object")

        if not parsed_data.get("Time required"):
            raise ValueError("Missing 'Time required' field in response")
        if not parsed_data.get("reasoning"):
            raise ValueError("Missing 'reasoning' field in response")

    @staticmethod
    def _validate_time_category(time_required: str) -> None:
        if time_required not in TIME_REQUIREMENT_CATEGORIES:
            raise ValueError(f"Invalid time requirement category: {time_required}")

    @staticmethod
    def _shorten_title(title: str) -> str:
        """Shorten title if too long."""
        if len(title) <= MAX_TITLE_LEN:
            return title.strip()
        return title[: MAX_TITLE_LEN - 3].strip() + "..."

    @staticmethod
    def _shorten_description(description: str) -> str:
        """Shorten description if too long, removing HTML."""
        lines = HTML_RE.sub("", description).split("\n")
        full_desc = " ".join(lines)
        if len(full_desc) <= MAX_DESC_LEN:
            return full_desc.strip()
        first_line = lines[0]
        if len(first_line) <= MAX_DESC_LEN:
            return first_line.strip()
        return first_line[: MAX_DESC_LEN - 3].strip() + "..."
