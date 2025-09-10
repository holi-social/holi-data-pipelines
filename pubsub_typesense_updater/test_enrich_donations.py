import unittest
from unittest.mock import patch, MagicMock, ANY
from topics_skills_embedding import (
    TopicEmbeddingJson,
    SkillEmbeddingJson,
)

from enrich_donations import (
    EnrichedDonation,
    create_similarity_embedding_for_donation,
    create_skills_v2_embedding_for_donation,
    create_text_input_for_similarity_embedding_from_donation,
    create_topics_v2_embedding_for_donation,
    enrich_donation,
    enriched_donation_to_typesense_doc,
)
from shared.events import Donation
from test_data import (
    valid_skills_embedding,
    valid_skills_embedding_json,
    valid_topics_embedding,
    valid_topics_embedding_json,
    test_donation,
    test_donation_typesense_document,
    test_similarity_embedding,
    test_minimal_donation,
    test_minimal_typesense_document,
)

normalized_description = "Die Sehnsucht bei Jugendlichen nach unbeschwerten Tagen ist sehr groß. Die Sommerfreizeit im Juli 2025 soll solche Tage ermöglichen. Jugendliche aus unterschiedlichen sozialen Zusammenhängen und mit ihren je eigenen Vorstellungen vom Leben verbringen 14 Tage an der Ostseeküste von Dänemark eine erlebnisreiche Zeit miteinander. Sie kochen füreinander, organisieren gemeinsam den Abwasch und die Reinigung der gemeinsam genutzten Räume, wie auch die gemeinsamen Freizeitaktivitäten. Sie werden von einem 6 bis 8 köpfigen Team aus ehrenamtlichen jungen Erwachsenen im Alter zwischen 20 bis 30 Jahren und dem Sozialpädagogen der Kirchengemeinde begleitet. Die Jugendlichen kommen damit zugleich mit unterschiedlichen Lebensentwürfen und Perspektiven in Kontakt, die Anregung und Vorbild für ihr eigenes Leben sein können.Soziale Kompetenzen werden in diesem „Aufeinander-angewiesen-sein“ intensiv erworben, auf die Probe gestellt und erweitert. Was sich hier in 14 Tagen entwickelt, dauert auf andere Weise oft Jahre. Es werden auch nicht zuletzt Konfliktlösungen eingeübt, gemeinsame Strategien entwickelt und Kompromissfähigkeit erlernt. Integration bedeutet hier ein Lernen auf Augenhöhe für alle. Unsere Sommerfreizeit ist ein Ort von Bildung, Begegnung, Partizipation und sozialem Lernen, bietet Erholung und fördert so die psychische Widerstandskraft und vermittelt Methoden zur Stressregulierung.Damit diese unbeschwerten Tage für alle Jugendlichen möglich werden, gleich welcher Herkunft oder in welch schwierigen Lagen sich Jugendliche und ihre Familien befinden, braucht es finanzielle Unterstützung. Mit einer Spende können Sie jungen Menschen diese nachhaltig prägenden Erfahrungen und Erlebnissen einer Jugendfreizeit ermöglichen."


class TestEnrichDonations(unittest.TestCase):
    @patch("enrich_donations.create_similarity_embedding")
    def test_should_create_similarity_embedding(self, create_similarity_embedding_mock: MagicMock) -> None:
        expected_text_input = f"Integrative Sommerfreizeit für Jugendliche nach Dänemark {normalized_description} Freizeiten sind für junge Menschen eine gute Möglichkeit, mit Gleichaltrigen gemeinsam eine erlebnisreiche und zugleich entspannte Zeit zu erleben. Solche Erfahrungen stärken die Jugendliche in herausfordernden Zeiten und schwierigen Lebensphasen. Sdr. Stenderup Evangelische Kirchengemeinde Steinheim/Main Bildung Integration Kinder und Jugend"
        create_similarity_embedding_mock.return_value = test_similarity_embedding

        result = create_similarity_embedding_for_donation(test_donation)

        create_similarity_embedding_mock.assert_called_with(
            expected_text_input,
            cache_logging_context={"logger": ANY, "log_prefix": "[id=144757 / similarity_embedding]"},
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

    @patch("enrich_donations.get_client")
    @patch(
        "enrich_donations.invoke_llm_with_structured_output_cached",
        return_value=valid_topics_embedding_json,
    )
    def test_should_create_topics_v2_embedding(self, llm_mock: MagicMock, langfuse_get_client_mock: MagicMock) -> None:
        expected_prompt_template = (
            "Here is a donation with title '{title}' and description '{description}', estimate the topic similarities!"
        )
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
            langchain_prompt=expected_prompt_template,
            input={"title": test_donation.title, "description": normalized_description},
            output_json_schema=TopicEmbeddingJson.model_json_schema(),
            cache_logging_context={
                "logger": ANY,
                "log_prefix": "[id=144757 / topics_v2_embedding]",
            },
        )
        self.assertEqual(
            result.model_dump_json(),
            valid_topics_embedding.model_dump_json(),
        )

    @patch("enrich_donations.get_client")
    @patch(
        "enrich_donations.invoke_llm_with_structured_output_cached",
        return_value={"animal_welfare": 0},
    )
    def test_should_raise_exception_for_invalid_topics_embedding(
        self, llm_mock: MagicMock, langfuse_get_client_mock: MagicMock
    ) -> None:
        expected_prompt_template = "Here is a donation with title '{title}', estimate the topic similarities!"
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

    @patch("enrich_donations.get_client")
    @patch(
        "enrich_donations.invoke_llm_with_structured_output_cached",
        return_value=valid_skills_embedding_json,
    )
    def test_should_create_skills_v2_embedding(self, llm_mock: MagicMock, langfuse_get_client_mock: MagicMock) -> None:
        expected_prompt_template = (
            "Here is a donation with title '{title}' and description '{description}', estimate the skill similarities!"
        )
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
            langchain_prompt=expected_prompt_template,
            input={"title": test_donation.title, "description": normalized_description},
            output_json_schema=SkillEmbeddingJson.model_json_schema(),
            cache_logging_context={
                "logger": ANY,
                "log_prefix": "[id=144757 / skills_v2_embedding]",
            },
        )
        self.assertEqual(
            result.model_dump_json(),
            valid_skills_embedding.model_dump_json(),
        )

    @patch("enrich_donations.get_client")
    @patch(
        "enrich_donations.invoke_llm_with_structured_output_cached",
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
        "enrich_donations.create_skills_v2_embedding_for_donation",
        return_value=valid_skills_embedding,
    )
    @patch(
        "enrich_donations.create_topics_v2_embedding_for_donation",
        return_value=valid_topics_embedding,
    )
    @patch(
        "enrich_donations.create_similarity_embedding_for_donation",
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

    def test_should_transform_enriched_donation_to_typesense_document(self):
        enriched_donation = EnrichedDonation(
            similarity_embedding=test_similarity_embedding,
            topics_v2_embedding=valid_topics_embedding,
            skills_v2_embedding=valid_skills_embedding,
            donation=test_donation,
        )

        actual_document = enriched_donation_to_typesense_doc(enriched_donation)

        self.assertEqual(actual_document.model_dump_json(), test_donation_typesense_document.model_dump_json())

    def test_should_transform_minimal_donation_to_typesense_document(self):
        enriched_donation = EnrichedDonation(
            similarity_embedding=None,
            topics_v2_embedding=None,
            skills_v2_embedding=None,
            donation=test_minimal_donation,
        )

        actual_document = enriched_donation_to_typesense_doc(enriched_donation)

        self.assertEqual(actual_document.model_dump_json(), test_minimal_typesense_document.model_dump_json())
