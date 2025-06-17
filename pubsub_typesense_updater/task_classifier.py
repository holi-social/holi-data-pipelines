from logging import Logger
from google.cloud.bigquery import Client as BigQueryClient
from shared.classifiers.classifier import Classifier
from shared.classifiers.task_classification_input import TaskClassificationInput
from shared.classifiers.task_commitment_duration_classifier import TaskCommitmentDurationClassifier
from shared.classifiers.task_topics_skills_classifier_v2 import TaskTopicsSkillsClassifierV2
from shared.embedders.task_similarity_embedder import TaskSimilarityEmbedder


class TaskClassifier(Classifier[TaskClassificationInput]):
    def __init__(
        self,
        table_name_classifications: str,
        logger: Logger,
        bqclient: BigQueryClient,
    ):
        super().__init__(
            topics_skills_classifier_v2=TaskTopicsSkillsClassifierV2(logger),
            similarity_embedder=TaskSimilarityEmbedder(logger),
            commitment_label_extractor=lambda item: item.regularity,
            location_type_extractor=lambda item: item.location_type,
            commitment_duration_classifier=TaskCommitmentDurationClassifier(logger),
            item_type="task",
            table_name_classifications=table_name_classifications,
            logger=logger,
            bqclient=bqclient,
        )
