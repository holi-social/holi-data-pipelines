import unittest
from unittest.mock import patch
from typing import cast, List, Any

from src.pipelines.tasks_to_typesense.ingest_from_scratch import (
    to_topics_skills_embedding_v2,
    task_to_doc,
    SKILLS_V2_VECTOR_INDICES,
    TOPICS_V2_VECTOR_INDICES,
)


class TestTopicsSkillsEmbedding(unittest.TestCase):
    def test_empty_skills_list(self) -> None:
        """Test that an empty skills list returns a zero vector of the correct size"""
        result = to_topics_skills_embedding_v2([])
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 0.0)  # All zeros

    def test_none_skills_list(self) -> None:
        """Test that None skills list returns a zero vector of the correct size"""
        result = to_topics_skills_embedding_v2(None)
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 0.0)  # All zeros

    def test_single_skill(self) -> None:
        """Test that a single skill is correctly encoded"""
        skill = "administration"
        result = to_topics_skills_embedding_v2([skill])
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 1.0)  # One skill set to 1.0
        self.assertEqual(result[len(TOPICS_V2_VECTOR_INDICES) + SKILLS_V2_VECTOR_INDICES[skill]], 1.0)

    def test_multiple_skills(self) -> None:
        """Test that multiple skills are correctly encoded"""
        skills = ["administration", "driving", "team-leadership"]
        result = to_topics_skills_embedding_v2(skills)
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 3.0)  # Three skills set to 1.0
        for skill in skills:
            self.assertEqual(result[len(TOPICS_V2_VECTOR_INDICES) + SKILLS_V2_VECTOR_INDICES[skill]], 1.0)

    def test_unknown_skill(self) -> None:
        """Test that unknown skills are ignored"""
        skills = ["administration", "unknown_skill", "team-leadership"]
        result = to_topics_skills_embedding_v2(skills)
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 2.0)  # Only two valid skills set to 1.0
        self.assertEqual(result[len(TOPICS_V2_VECTOR_INDICES) + SKILLS_V2_VECTOR_INDICES["administration"]], 1.0)
        self.assertEqual(result[len(TOPICS_V2_VECTOR_INDICES) + SKILLS_V2_VECTOR_INDICES["team-leadership"]], 1.0)


class TestTaskToDoc(unittest.TestCase):
    def setUp(self) -> None:
        """Set up common test data"""
        self.task_data = {
            "id": 123,
            "name": "Test Task",
            "description": "This is a test task",
            "location": "Berlin, Germany",
            "thumbnail": "https://example.com/image.jpg",
            "community_id": 456,
            "skills_v2": ["administration", "driving"],
            "geolocation_geojson": {
                "type": "Point",
                "coordinates": [13.404954, 52.520008],  # Longitude, Latitude
            },
        }

    def test_basic_task_conversion(self) -> None:
        """Test basic task conversion to document"""
        result = task_to_doc(self.task_data)

        # Check basic fields
        self.assertEqual(result["id"], "space_task_123")
        self.assertEqual(result["type"], "space_task")
        self.assertEqual(result["title_de"], "Test Task")
        self.assertEqual(result["title_en"], "Test Task")
        self.assertEqual(result["description_de"], "This is a test task")
        self.assertEqual(result["description_en"], "This is a test task")
        self.assertEqual(result["location"], "Berlin, Germany")
        self.assertEqual(result["image_url"], "https://example.com/image.jpg")

        # Check link locators
        assert result["link_locators"] is not None
        self.assertEqual(result["link_locators"]["space"], 456)
        self.assertEqual(result["link_locators"]["task"], 123)

        # Check location_lat_lng (should be reversed from geojson)
        self.assertEqual(result["location_lat_lng"], [52.520008, 13.404954])

        # Check embeddings
        self.assertIsNotNone(result["topics_skills_embedding_v2"])
        self.assertIsNone(result["similarity_embedding"])

    def test_task_without_geolocation(self) -> None:
        """Test task conversion without geolocation data"""
        task_data = self.task_data.copy()
        task_data["geolocation_geojson"] = None

        result = task_to_doc(task_data)
        self.assertIsNone(result["location_lat_lng"])

    def test_task_without_skills(self) -> None:
        """Test task conversion without skills data"""
        task_data = self.task_data.copy()
        task_data["skills_v2"] = None

        result = task_to_doc(task_data)
        # Should have zero vector for skills embedding
        self.assertEqual(sum(cast(List[float], result["topics_skills_embedding_v2"])), 0.0)

    @patch("src.pipelines.tasks_to_typesense.ingest_from_scratch.to_topics_skills_embedding_v2")
    def test_embedding_function_called(self, mock_embedding: Any) -> None:
        """Test that the embedding function is called with the right parameters"""
        mock_embedding.return_value = [0.0] * (len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES))

        task_to_doc(self.task_data)
        mock_embedding.assert_called_once_with(self.task_data.get("skills_v2"))


if __name__ == "__main__":
    unittest.main()
