import unittest
from unittest.mock import patch, MagicMock
from langchain_core.prompt_values import StringPromptValue

from topics_skills_embedding import (
    TopicEmbeddingJson,
    invoke_llm_with_structured_output_cached,
)


class TestTopicSkillsEmbedding(unittest.TestCase):
    @patch("topics_skills_embedding.ChatVertexAI")
    @patch("topics_skills_embedding.CallbackHandler")
    @patch(
        "os.environ",
        {
            "VERTEX_AI_PROJECT": "project",
            "VERTEX_AI_LOCATION": "location",
            "VERTEX_AI_RETRIES": 3,
        },
    )
    def test_should_invoke_llm_correctly(self, callback_handler_mock, vertex_ai_mock):
        # Setup
        expected_output = {"foo": "bar"}
        output_schema = TopicEmbeddingJson.model_json_schema()
        langfuse_handler = MagicMock()
        callback_handler_mock.return_value = langfuse_handler
        model_mock = MagicMock()
        model_mock.invoke.return_value = expected_output
        llm_mock = MagicMock()
        llm_mock.with_structured_output.return_value = model_mock
        vertex_ai_mock.return_value = llm_mock

        # Given
        prompt = "Here is a donation with title '{title}', calculate some topics!"
        input = {"title": "Help students"}

        # When
        output = invoke_llm_with_structured_output_cached.fn(
            langchain_prompt=prompt,
            input=input,
            output_json_schema=output_schema,
            model_name="some-model",
        )

        # Then
        vertex_ai_mock.assert_called_with(
            model_name="some-model",
            project="project",
            location="location",
            max_retries=3,
            callbacks=[langfuse_handler],
        )
        llm_mock.with_structured_output.assert_called_with(output_schema, method="json_mode")
        model_mock.invoke.assert_called_with(
            StringPromptValue(text="Here is a donation with title 'Help students', calculate some topics!")
        )
        self.assertEqual(output, expected_output)
