import unittest

from src.pipelines.task_classification.task_topics_skills_classifier_v2 import (
    to_topics_skills_embedding_v2,
    TOPICS_V2_VECTOR_INDICES,
    SKILLS_V2_VECTOR_INDICES,
)


class TestTopicsSkillsEmbedding(unittest.TestCase):
    def test_empty_skills_list(self) -> None:
        """Test that an empty skills list returns a zero vector of the correct size"""
        result = to_topics_skills_embedding_v2([], [])
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 0.0)  # All zeros

    def test_none_lists(self) -> None:
        """Test that None skills list returns a zero vector of the correct size"""
        result = to_topics_skills_embedding_v2(None, None)
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 0.0)  # All zeros

    def test_single_skill(self) -> None:
        """Test that a single skill is correctly encoded"""
        skill = "administration"
        result = to_topics_skills_embedding_v2(None, [skill])
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 1.0)  # One skill set to 1.0
        self.assertEqual(result[len(TOPICS_V2_VECTOR_INDICES) + SKILLS_V2_VECTOR_INDICES[skill]], 1.0)

    def test_multiple_skills(self) -> None:
        """Test that multiple skills are correctly encoded"""
        skills = ["administration", "driving", "team-leadership"]
        result = to_topics_skills_embedding_v2(None, skills)
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 3.0)  # Three skills set to 1.0
        for skill in skills:
            self.assertEqual(result[len(TOPICS_V2_VECTOR_INDICES) + SKILLS_V2_VECTOR_INDICES[skill]], 1.0)

    def test_unknown_skill(self) -> None:
        """Test that unknown skills are ignored"""
        skills = ["administration", "unknown_skill", "team-leadership"]
        result = to_topics_skills_embedding_v2(None, skills)
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 2.0)  # Only two valid skills set to 1.0
        self.assertEqual(result[len(TOPICS_V2_VECTOR_INDICES) + SKILLS_V2_VECTOR_INDICES["administration"]], 1.0)
        self.assertEqual(result[len(TOPICS_V2_VECTOR_INDICES) + SKILLS_V2_VECTOR_INDICES["team-leadership"]], 1.0)

    def test_empty_topics_list(self) -> None:
        """Test that an empty topics list returns a zero vector of the correct size"""
        result = to_topics_skills_embedding_v2([], [])
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 0.0)  # All zeros

    def test_single_topic(self) -> None:
        topic = "animal-welfare"
        result = to_topics_skills_embedding_v2([topic], [])
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 1.0)  # One topic set to 1.0
        self.assertEqual(result[TOPICS_V2_VECTOR_INDICES[topic]], 1.0)

    def test_multiple_topics(self) -> None:
        topics = ["animal-welfare", "climate-environment"]
        skills = ["community-organizing"]
        result = to_topics_skills_embedding_v2(topics, skills)
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 3.0)  # Two topics set to 1.0
        for topic in topics:
            self.assertEqual(result[TOPICS_V2_VECTOR_INDICES[topic]], 1.0)
        for skill in skills:
            self.assertEqual(result[len(TOPICS_V2_VECTOR_INDICES) + SKILLS_V2_VECTOR_INDICES[skill]], 1.0)

    def test_unknown_topic(self) -> None:
        topics = ["animal-welfare", "unknown_topic"]
        skills = ["community-organizing"]
        result = to_topics_skills_embedding_v2(topics, skills)
        expected_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
        self.assertEqual(len(result), expected_size)
        self.assertEqual(sum(result), 2.0)  # Only one valid topic set to 1.0
        self.assertEqual(result[TOPICS_V2_VECTOR_INDICES["animal-welfare"]], 1.0)
        self.assertEqual(result[len(TOPICS_V2_VECTOR_INDICES) + SKILLS_V2_VECTOR_INDICES["community-organizing"]], 1.0)
