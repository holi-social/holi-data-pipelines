import unittest
from unittest.mock import patch, MagicMock
from vertexai.language_models import TextEmbeddingInput

from similarity_embedding import create_similarity_embedding
from test_data import test_similarity_embedding


class TestSimilarityEmbedding(unittest.TestCase):
    @patch("similarity_embedding.TextEmbeddingModel")
    def test_should_correctly_create_similarity_embedding(self, text_embedding_model_mock):
        # Setup
        embedding_mock = MagicMock()
        embedding_mock.values = test_similarity_embedding
        model_mock = MagicMock()
        model_mock.get_embeddings.return_value = [embedding_mock]
        text_embedding_model_mock.from_pretrained.return_value = model_mock

        # Given
        input = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed eiusmod tempor incidunt ut labore et dolore magna aliqua."

        # When
        output = create_similarity_embedding.fn(input)

        # Then
        text_embedding_model_mock.from_pretrained.assert_called_with("text-multilingual-embedding-002")
        model_mock.get_embeddings.assert_called_with(
            [TextEmbeddingInput(text=input, task_type="RETRIEVAL_DOCUMENT")], output_dimensionality=256
        )
        self.assertEqual(output, test_similarity_embedding)
