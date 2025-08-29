import unittest
from unittest.mock import patch, MagicMock, ANY
from langchain_core.prompts import PromptTemplate
from topics_skills_embedding import (
    TopicEmbeddingJson,
    SkillEmbeddingJson,
)

from pubsub_typesense_updater.enrich_donations import (
    EnrichedDonation,
    create_similarity_embedding_for_donation,
    create_skills_v2_embedding_for_donation,
    create_text_input_for_similarity_embedding_from_donation,
    create_topics_v2_embedding_for_donation,
    enrich_donation,
)
from shared.events import Donation

test_donation = Donation(
    id=1,
    title="Integrative Sommerfreizeit für Jugendliche nach Dänemark",
    description="<div>Die Sehnsucht bei Jugendlichen nach unbeschwerten Tagen ist sehr groß. Die Sommerfreizeit im Juli 2025 soll solche Tage ermöglichen. Jugendliche aus unterschiedlichen sozialen Zusammenhängen und mit ihren je eigenen Vorstellungen vom Leben verbringen 14 Tage an der Ostseeküste von Dänemark eine erlebnisreiche Zeit miteinander.</div>",
    summary="Freizeiten sind für junge Menschen eine gute Möglichkeit, mit Gleichaltrigen gemeinsam eine erlebnisreiche und zugleich entspannte Zeit zu erleben. Solche Erfahrungen stärken die Jugendliche in herausfordernden Zeiten und schwierigen Lebensphasen.",
    city="Sdr. Stenderup",
    carrier={"name": "Evangelische Kirchengemeinde Steinheim/Main"},
    categories=[
        {"name": "Bildung"},
        {"name": "Integration"},
        {"name": "Kinder und Jugend"},
    ],
)

test_similarity_embedding = [0.1, 0.2, 0.3]

valid_topics_embedding_json = {
    "animal_welfare": 0,
    "arts_culture": 0,
    "climate_environment": 0,
    "democracy_politics": 0,
    "disability_inclusion": 0,
    "disaster_relief": 0,
    "elderly_care": 0,
    "family_kids": 0,
    "food_hunger_relief": 0,
    "gender_equality": 0,
    "housing": 0,
    "human_rights": 0,
    "lgbtqia_plus": 0,
    "lifestyle_consumption": 0,
    "literacy_education": 0,
    "mental_physical_health": 0,
    "migration_refugees": 1,
    "mobility_transport": 0,
    "other": 0,
    "peace_security": 0,
    "poverty_homelessness": 0,
    "sports_recreation": 0.5,
    "technology_innovation": 0,
    "youth_development": 1,
}

valid_topics_embedding = TopicEmbeddingJson(
    animal_welfare=0,
    arts_culture=0,
    climate_environment=0,
    democracy_politics=0,
    disability_inclusion=0,
    disaster_relief=0,
    elderly_care=0,
    family_kids=0,
    food_hunger_relief=0,
    gender_equality=0,
    housing=0,
    human_rights=0,
    lgbtqia_plus=0,
    lifestyle_consumption=0,
    literacy_education=0,
    mental_physical_health=0,
    migration_refugees=1,
    mobility_transport=0,
    other=0,
    peace_security=0,
    poverty_homelessness=0,
    sports_recreation=0.5,
    technology_innovation=0,
    youth_development=1,
)

valid_skills_embedding_json = {
    "administration": 0,
    "animal_care": 0,
    "arts_crafts": 0,
    "building_handicraft": 0,
    "childcare": 0,
    "community_organizing": 0.5,
    "creativity_design": 0,
    "driving": 0,
    "emergency_care": 0,
    "event_planning": 0,
    "finances_taxes": 0,
    "food_service": 1,
    "fundraising": 0,
    "gardening_conservation": 0.5,
    "intercultural_competence": 0,
    "it_technology": 0,
    "legal_assistance": 0,
    "management_strategy": 0,
    "marketing_communications": 0,
    "moderation_facilitation": 0,
    "music_performance": 0,
    "nursing_caregiving": 0,
    "other": 0,
    "religious_spiritual_care": 0,
    "research_analysis": 0,
    "sales_distribution": 0,
    "team_leadership": 0,
    "translation": 0,
    "tutoring_coaching": 0,
}

valid_skills_embedding = SkillEmbeddingJson(
    administration=0,
    animal_care=0,
    arts_crafts=0,
    building_handicraft=0,
    childcare=0,
    community_organizing=0.5,
    creativity_design=0,
    driving=0,
    emergency_care=0,
    event_planning=0,
    finances_taxes=0,
    food_service=1,
    fundraising=0,
    gardening_conservation=0.5,
    intercultural_competence=0,
    it_technology=0,
    legal_assistance=0,
    management_strategy=0,
    marketing_communications=0,
    moderation_facilitation=0,
    music_performance=0,
    nursing_caregiving=0,
    other=0,
    religious_spiritual_care=0,
    research_analysis=0,
    sales_distribution=0,
    team_leadership=0,
    translation=0,
    tutoring_coaching=0,
)


class TestEnrichDonations(unittest.TestCase):
    @patch("pubsub_typesense_updater.enrich_donations.create_similarity_embedding")
    def test_should_create_similarity_embedding(self, create_similarity_embedding_mock: MagicMock) -> None:
        expected_text_input = "Integrative Sommerfreizeit für Jugendliche nach Dänemark Die Sehnsucht bei Jugendlichen nach unbeschwerten Tagen ist sehr groß. Die Sommerfreizeit im Juli 2025 soll solche Tage ermöglichen. Jugendliche aus unterschiedlichen sozialen Zusammenhängen und mit ihren je eigenen Vorstellungen vom Leben verbringen 14 Tage an der Ostseeküste von Dänemark eine erlebnisreiche Zeit miteinander. Freizeiten sind für junge Menschen eine gute Möglichkeit, mit Gleichaltrigen gemeinsam eine erlebnisreiche und zugleich entspannte Zeit zu erleben. Solche Erfahrungen stärken die Jugendliche in herausfordernden Zeiten und schwierigen Lebensphasen. Sdr. Stenderup Evangelische Kirchengemeinde Steinheim/Main Bildung Integration Kinder und Jugend"
        create_similarity_embedding_mock.return_value = test_similarity_embedding

        result = create_similarity_embedding_for_donation(test_donation)

        create_similarity_embedding_mock.assert_called_with(
            expected_text_input, cache_logging_context={"logger": ANY, "log_prefix": "[id=1 / similarity_embedding]"}
        )
        self.assertEqual(result, test_similarity_embedding)

    def test_should_handle_optional_values_for_similarity_embedding_text_input(self) -> None:
        minimal_donation = Donation(id=1, title="Title", description="Description", city="City", categories=[{}])
        expected_text_input = "Title Description  City  "

        result = create_text_input_for_similarity_embedding_from_donation(minimal_donation)

        self.assertEqual(result, expected_text_input)

    def test_should_remove_whitespace_and_html_for_similarity_embedding_text_input(self) -> None:
        minimal_donation = Donation(
            id=1, title="  Title ", description="<div><b>Description</b></div>", city="City ", categories=[{}]
        )
        expected_text_input = "Title Description  City  "

        result = create_text_input_for_similarity_embedding_from_donation(minimal_donation)

        self.assertEqual(result, expected_text_input)

    @patch("pubsub_typesense_updater.enrich_donations.get_client")
    @patch(
        "pubsub_typesense_updater.enrich_donations.invoke_llm_with_structured_output_cached",
        return_value=valid_topics_embedding_json,
    )
    def test_should_create_topics_v2_embedding(self, llm_mock: MagicMock, langfuse_get_client_mock: MagicMock) -> None:
        expected_prompt_template = "Here is a donation, estimate the topic similarities!"
        langfuse_mock = MagicMock()
        prompt_mock = MagicMock()
        langfuse_get_client_mock.return_value = langfuse_mock
        langfuse_mock.get_prompt.return_value = prompt_mock
        prompt_mock.get_langchain_prompt.return_value = expected_prompt_template

        result = create_topics_v2_embedding_for_donation(test_donation)

        langfuse_mock.get_prompt.assert_called_with(
            "donations_topics_v2_embedding_prompt",
            fallback="""Here is a donation project with title '{{title}}' and decription '{{description}}'.
    Estimate the similarity of the project to the given topics between -1 and 1 (-1 for not similar, 1 for similar)""",
        )
        llm_mock.assert_called_with(
            prompt_template=PromptTemplate.from_template(expected_prompt_template),
            input=test_donation.model_dump(),
            output_json_schema=TopicEmbeddingJson.model_json_schema(),
            cache_logging_context={
                "logger": ANY,
                "log_prefix": "[id=1 / topics_v2_embedding]",
            },
        )
        self.assertEqual(
            result,
            valid_topics_embedding,
        )

    @patch("pubsub_typesense_updater.enrich_donations.get_client")
    @patch(
        "pubsub_typesense_updater.enrich_donations.invoke_llm_with_structured_output_cached",
        return_value={"animal_welfare": 0},
    )
    def test_should_raise_exception_for_invalid_topics_embedding(
        self, llm_mock: MagicMock, langfuse_get_client_mock: MagicMock
    ) -> None:
        expected_prompt_template = "Here is a donation, estimate the topic similarities!"
        langfuse_mock = MagicMock()
        prompt_mock = MagicMock()
        langfuse_get_client_mock.return_value = langfuse_mock
        langfuse_mock.get_prompt.return_value = prompt_mock
        prompt_mock.get_langchain_prompt.return_value = expected_prompt_template

        with self.assertRaises(Exception) as ctx:
            create_topics_v2_embedding_for_donation(test_donation)

        self.assertIn(
            "23 validation errors for TopicEmbeddingJson",
            str(ctx.exception),
        )

    @patch("pubsub_typesense_updater.enrich_donations.get_client")
    @patch(
        "pubsub_typesense_updater.enrich_donations.invoke_llm_with_structured_output_cached",
        return_value=valid_skills_embedding_json,
    )
    def test_should_create_skills_v2_embedding(self, llm_mock: MagicMock, langfuse_get_client_mock: MagicMock) -> None:
        expected_prompt_template = "Here is a donation, estimate the skill similarities!"
        langfuse_mock = MagicMock()
        prompt_mock = MagicMock()
        langfuse_get_client_mock.return_value = langfuse_mock
        langfuse_mock.get_prompt.return_value = prompt_mock
        prompt_mock.get_langchain_prompt.return_value = expected_prompt_template

        result = create_skills_v2_embedding_for_donation(test_donation)

        langfuse_mock.get_prompt.assert_called_with(
            "donations_skills_v2_embedding_prompt",
            fallback="""Here is a donation project with title '{{title}}' and decription '{{description}}'.
    Estimate the similarity of the project to the given skills between -1 and 1 (-1 for not similar, 1 for similar)""",
        )
        llm_mock.assert_called_with(
            prompt_template=PromptTemplate.from_template(expected_prompt_template),
            input=test_donation.model_dump(),
            output_json_schema=SkillEmbeddingJson.model_json_schema(),
            cache_logging_context={
                "logger": ANY,
                "log_prefix": "[id=1 / skills_v2_embedding]",
            },
        )
        self.assertEqual(
            result,
            valid_skills_embedding,
        )

    @patch("pubsub_typesense_updater.enrich_donations.get_client")
    @patch(
        "pubsub_typesense_updater.enrich_donations.invoke_llm_with_structured_output_cached",
        return_value={"animal_care": 0},
    )
    def test_should_raise_exception_for_invalid_skills_embedding(
        self, llm_mock: MagicMock, langfuse_get_client_mock: MagicMock
    ) -> None:
        expected_prompt_template = "Here is a donation, estimate the skill similarities!"
        langfuse_mock = MagicMock()
        prompt_mock = MagicMock()
        langfuse_get_client_mock.return_value = langfuse_mock
        langfuse_mock.get_prompt.return_value = prompt_mock
        prompt_mock.get_langchain_prompt.return_value = expected_prompt_template

        with self.assertRaises(Exception) as ctx:
            create_skills_v2_embedding_for_donation(test_donation)

        self.assertIn(
            "28 validation errors for SkillEmbeddingJson",
            str(ctx.exception),
        )

    @patch(
        "pubsub_typesense_updater.enrich_donations.create_skills_v2_embedding_for_donation",
        return_value=valid_skills_embedding,
    )
    @patch(
        "pubsub_typesense_updater.enrich_donations.create_topics_v2_embedding_for_donation",
        return_value=valid_topics_embedding,
    )
    @patch(
        "pubsub_typesense_updater.enrich_donations.create_similarity_embedding_for_donation",
        return_value=test_similarity_embedding,
    )
    def test_should_enrich_donation(
        self, similarity_embedding_mock, topics_v2_embedding_mock, skills_v2_embedding_mock
    ):
        result = enrich_donation(test_donation)

        self.assertEqual(
            result,
            EnrichedDonation(
                donation=test_donation,
                similarity_embedding=test_similarity_embedding,
                topics_v2_embedding=valid_topics_embedding,
                skills_v2_embedding=valid_skills_embedding,
            ),
        )
