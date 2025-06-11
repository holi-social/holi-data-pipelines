import unittest
from typing import Any
from unittest.mock import Mock

from shared.classifiers.time_requirement_classifier import (
    TimeRequirementClassification,
    TimeRequirementClassifier,
)


class TestTimeRequirementClassification(unittest.TestCase):
    """Test cases for the TimeRequirementClassification dataclass."""

    def test_time_requirement_classification_creation(self) -> None:
        """Test that TimeRequirementClassification can be created with required fields."""
        classification = TimeRequirementClassification(
            time_required="1-2 hours", time_reasoning="Based on the description of a 2-hour event."
        )

        self.assertEqual(classification.time_required, "1-2 hours")
        self.assertEqual(classification.time_reasoning, "Based on the description of a 2-hour event.")

    def test_time_requirement_classification_with_unable_to_determine(self) -> None:
        """Test classification with 'Unable to determine' category."""
        classification = TimeRequirementClassification(
            time_required="Unable to determine", time_reasoning="Insufficient information in the description."
        )

        self.assertEqual(classification.time_required, "Unable to determine")
        self.assertEqual(classification.time_reasoning, "Insufficient information in the description.")

    def test_time_requirement_classification_with_various_categories(self) -> None:
        """Test classification with different time categories."""
        test_cases = [
            ("<30 mins", "Quick task mentioned in description"),
            ("30 mins - 1 hour", "Estimated based on activity type"),
            ("1-2 hours", "Explicit time range given"),
            ("2-4 hours", "Full morning session described"),
            ("4-6 hours", "Half-day commitment mentioned"),
            ("1 day", "Full day event stated"),
            ("> 1 day", "Multi-day workshop described"),
        ]

        for time_req, reasoning in test_cases:
            with self.subTest(time_required=time_req):
                classification = TimeRequirementClassification(time_required=time_req, time_reasoning=reasoning)
                self.assertEqual(classification.time_required, time_req)
                self.assertEqual(classification.time_reasoning, reasoning)


class TestTimeRequirementClassifierBase(unittest.TestCase):
    """Test cases for the TimeRequirementClassifier base class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_logger = Mock()

    def test_time_requirement_classifier_is_abstract(self) -> None:
        """Test that TimeRequirementClassifier cannot be instantiated directly."""
        with self.assertRaises(TypeError):
            TimeRequirementClassifier[Any](self.mock_logger)  # type: ignore[abstract]

    def test_time_requirement_classifier_logger_assignment(self) -> None:
        """Test that the logger is properly assigned in the base class."""

        class ConcreteTimeRequirementClassifier(TimeRequirementClassifier[Any]):
            def get_classification(self, item: Any) -> TimeRequirementClassification:
                return TimeRequirementClassification(time_required="1-2 hours", time_reasoning="Test reasoning")

        classifier = ConcreteTimeRequirementClassifier(self.mock_logger)
        self.assertEqual(classifier.logger, self.mock_logger)

    def test_concrete_classifier_implements_get_classification(self) -> None:
        """Test that concrete classifiers must implement get_classification."""

        class ConcreteTimeRequirementClassifier(TimeRequirementClassifier[Any]):
            def get_classification(self, item: Any) -> TimeRequirementClassification:
                return TimeRequirementClassification(
                    time_required="2-4 hours", time_reasoning="Concrete implementation test"
                )

        classifier = ConcreteTimeRequirementClassifier(self.mock_logger)

        mock_item = Mock()
        mock_item.id = "test_id"

        result = classifier.get_classification(mock_item)

        self.assertIsInstance(result, TimeRequirementClassification)
        self.assertEqual(result.time_required, "2-4 hours")
        self.assertEqual(result.time_reasoning, "Concrete implementation test")


if __name__ == "__main__":
    unittest.main()
