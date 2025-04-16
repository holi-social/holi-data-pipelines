import unittest
from typing import cast, List

from src.pipelines.tasks_to_typesense.ingest_from_scratch import task_to_doc


class TestTaskToDoc(unittest.TestCase):
    def setUp(self) -> None:
        """Set up common test data"""
        self.task_data = {
            "id": 123,
            "name": "Test Task",
            "community_slug": "my-community",
            "description": "This is a test task",
            "location": "Berlin, Germany",
            "thumbnail": "https://example.com/image.jpg",
            "community_id": 456,
            "geolocation_geojson": {
                "type": "Point",
                "coordinates": [13.404954, 52.520008],  # Longitude, Latitude
            },
            "topics_skills_embedding_v2": [1, 0, -1],
            "topics_v2": ["animal-welfare", "arts-culture"],
            "skills_v2": ["administration", "driving"],
            "similarity_embedding": [-1, 0, 1],
        }

    def test_basic_task_conversion(self) -> None:
        """Test basic task conversion to document"""
        result = task_to_doc(self.task_data)

        # Check basic fields
        self.assertEqual(result["id"], "task_123")
        self.assertEqual(result["space_id"], "456")
        self.assertEqual(result["type"], "task")
        self.assertEqual(result["title_de"], "Test Task")
        self.assertEqual(result["title_en"], "Test Task")
        self.assertEqual(result["description_de"], "This is a test task")
        self.assertEqual(result["description_en"], "This is a test task")
        self.assertEqual(result["location"], "Berlin, Germany")
        self.assertEqual(result["image_url"], "https://example.com/image.jpg")

        # Check link locators
        assert result["link_locators"] is not None
        self.assertEqual(result["link_locators"]["space"], "my-community")
        self.assertEqual(result["link_locators"]["task"], 123)

        # Check location_lat_lng (should be reversed from geojson)
        self.assertEqual(result["location_lat_lng"], [52.520008, 13.404954])

        # Check embeddings
        self.assertEqual(result["topics_skills_embedding_v2"], [1, 0, -1])
        self.assertEqual(result["topics_v2"], ["animal-welfare", "arts-culture"])
        self.assertEqual(result["skills_v2"], ["administration", "driving"])
        self.assertEqual(result["similarity_embedding"], [-1, 0, 1])

        # Check locator
        self.assertEqual(result["link_locators"], {"space": "my-community", "task": 123})

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


if __name__ == "__main__":
    unittest.main()
