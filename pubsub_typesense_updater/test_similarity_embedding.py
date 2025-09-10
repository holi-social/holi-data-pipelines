import unittest
from unittest.mock import patch, MagicMock

from similarity_embedding import create_similarity_embedding
from test_data import test_similarity_embedding


class TestSimilarityEmbedding(unittest.TestCase):
    @patch("similarity_embedding.VertexAIEmbeddings")
    @patch(
        "os.environ",
        {
            "VERTEX_AI_PROJECT": "project",
            "VERTEX_AI_LOCATION": "location",
            "VERTEX_AI_RETRIES": 3,
        },
    )
    def test_should_correctly_create_similarity_embedding(self, vertex_ai_mock):
        # Setup
        model_mock = MagicMock()
        model_mock.embed.return_value = [test_similarity_embedding]
        vertex_ai_mock.return_value = model_mock

        # Given
        input = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed eiusmod tempor incidunt ut labore et dolore magna aliqua."

        # When
        output = create_similarity_embedding.fn(input)

        # Then
        vertex_ai_mock.assert_called_with(
            model_name="text-multilingual-embedding-002",
            project="project",
            location="location",
            max_retries=3,
        )
        model_mock.embed.assert_called_with([input], dimensions=256, embeddings_task_type="RETRIEVAL_DOCUMENT")
        self.assertEqual(output, test_similarity_embedding)
