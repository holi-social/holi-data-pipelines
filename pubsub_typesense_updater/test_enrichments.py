import unittest

from pubsub_typesense_updater.enrichments import Enrichments, normalize_text
from pubsub_typesense_updater.test_data import (
    valid_skills_embedding,
    valid_topics_embedding,
    test_topic_skills_embedding,
)


class TestEnrichments(unittest.TestCase):
    def test_should_extract_topic_slugs(self):
        expected_slugs = ["literacy-education", "mental-physical-health", "youth-development"]

        enrichment = Enrichments(
            similarity_embedding=None, topics_v2_embedding=valid_topics_embedding, skills_v2_embedding=None
        )

        actual_slugs = enrichment.topics_v2()

        self.assertEqual(actual_slugs, expected_slugs)

    def test_should_not_extract_topic_slugs_if_embedding_is_missing(self):
        enrichment = Enrichments(similarity_embedding=None, topics_v2_embedding=None, skills_v2_embedding=None)

        actual_slugs = enrichment.topics_v2()

        self.assertIsNone(actual_slugs)

    def test_should_extract_skill_slugs(self):
        expected_slugs = [
            "childcare",
            "fundraising",
            "intercultural-competence",
            "moderation-facilitation",
            "team-leadership",
        ]

        enrichment = Enrichments(
            similarity_embedding=None, topics_v2_embedding=None, skills_v2_embedding=valid_skills_embedding
        )

        actual_slugs = enrichment.skills_v2()

        self.assertEqual(actual_slugs, expected_slugs)

    def test_should_not_extract_skill_slugs_if_embedding_is_missing(self):
        enrichment = Enrichments(similarity_embedding=None, topics_v2_embedding=None, skills_v2_embedding=None)

        actual_slugs = enrichment.skills_v2()

        self.assertIsNone(actual_slugs)

    def test_should_create_topics_skills_embedding(self):
        enrichment = Enrichments(
            similarity_embedding=None,
            topics_v2_embedding=valid_topics_embedding,
            skills_v2_embedding=valid_skills_embedding,
        )

        actual_embedding = enrichment.topics_skills_embedding_v2()

        self.assertEqual(actual_embedding, test_topic_skills_embedding)

    def test_should_handle_missing_topics_in_topics_skills_embedding(self):
        expected_embedding = [0] * 24 + test_topic_skills_embedding[24:]

        enrichment = Enrichments(
            similarity_embedding=None, topics_v2_embedding=None, skills_v2_embedding=valid_skills_embedding
        )

        actual_embedding = enrichment.topics_skills_embedding_v2()

        self.assertEqual(actual_embedding, expected_embedding)

    def test_should_handle_missing_skills_in_topics_skills_embedding(self):
        expected_embedding = test_topic_skills_embedding[:24] + [0] * 29

        enrichment = Enrichments(
            similarity_embedding=None, topics_v2_embedding=valid_topics_embedding, skills_v2_embedding=None
        )

        actual_embedding = enrichment.topics_skills_embedding_v2()

        self.assertEqual(actual_embedding, expected_embedding)

    def test_should_not_return_topics_skills_embedding_if_topics_and_skills_are_missing(self):
        enrichment = Enrichments(similarity_embedding=None, topics_v2_embedding=None, skills_v2_embedding=None)

        actual_embedding = enrichment.topics_skills_embedding_v2()

        self.assertIsNone(actual_embedding)

    def test_should_normalize_text(self):
        text = "<div>  Some text with <b>html</b> and whitespace  </div> <br/> <br/>"
        expected_text = "Some text with html and whitespace"

        self.assertEqual(normalize_text(text), expected_text)
