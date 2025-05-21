from typing import Any, List, Optional, Union, cast

from prefect import Flow, deploy
from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments.runner import RunnerDeployment
from prefect.deployments.schedules import FlexibleScheduleList
from prefect.events import DeploymentTriggerTypes, TriggerTypes

from src.common.variables import variable_value_as
from src.pipelines.aktion_mensch_to_typesense.ingest_from_scratch import (
    aktion_mensch_to_typesense_ingest_from_scratch_flow,
)
from src.pipelines.aktion_mensch_to_typesense.ingest_updates import aktion_mensch_to_typesense_ingest_updates_flow
from src.pipelines.all_to_typesense.ingest_from_scratch import all_to_typesense_ingest_from_scratch_flow
from src.pipelines.crawlers.aktion_mensch.pipeline import crawl_aktion_mensch
from src.pipelines.crawlers.betterplace.pipeline import crawl_betterplace
from src.pipelines.crawlers.voltastics.pipeline import crawl_voltastics
from src.pipelines.example.pipeline import hello_universe
from src.pipelines.profiles_to_typesense.ingest_from_scratch import profiles_to_typesense_ingest_from_scratch_flow
from src.pipelines.spaces_to_typesense.ingest_from_scratch import spaces_to_typesense_ingest_from_scratch_flow
from src.pipelines.task_classification.task_classification import task_classification_flow
from src.pipelines.tasks_to_typesense.ingest_from_scratch import tasks_to_typesense_ingest_from_scratch_flow
from src.pipelines.typesense_qc.typesense_qc import typesense_qc
from src.pipelines.volunteering_classification.aktion_mensch_classification import aktion_mensch_classification
from src.pipelines.volunteering_classification.volunteering_classification import volunteering_classification
from src.pipelines.volunteering_geo_enrichment.volunteering_geo_enrichment import volunteering_geo_enrichment
from src.pipelines.volunteering_to_typesense.ingest_from_scratch import (
    volunteering_to_typesense_ingest_from_scratch_flow,
)
from src.pipelines.volunteering_to_typesense.ingest_updates import volunteering_to_typesense_ingest_updates_flow
from src.pipelines.volunteering_updates.pipeline import volunteering_updates


def environment() -> str:
    # FIXME remove cast once https://github.com/PrefectHQ/prefect/issues/16825 has a resolution
    return cast(str, variable_value_as(str, "prefect_environment"))


def default_docker_image_tag() -> str:
    return f"europe-north1-docker.pkg.dev/holi-shared/docker/data-pipeline-job-{environment()}:latest"


def cron_schedule(expression: str) -> CronSchedule:
    return CronSchedule(
        cron=expression,
        timezone="Etc/UTC",
        day_or=True,
    )


def default_job_variables(name: str) -> dict[str, Any]:
    return {
        "memory": "512Mi",
        "timeout": 86400,
        "image": default_docker_image_tag(),
        "name": name,
    }


def deployment_for_flow(
    flow: Flow[Any, Any],
    schedules: Optional[FlexibleScheduleList] = None,
    paused: bool = False,
    description: Optional[str] = None,
    job_variables_overrides: Optional[dict[str, Any]] = None,
    triggers: Optional[list[Union[DeploymentTriggerTypes, TriggerTypes]]] = None,
) -> RunnerDeployment:
    # merge default job variables with overrides
    job_variables = default_job_variables(name=flow.name)
    if job_variables_overrides:
        job_variables.update(job_variables_overrides)
    # FIXME remove MyPy ignore comment once https://github.com/PrefectHQ/prefect/issues/16825 has a resolution
    return flow.to_deployment(  # type: ignore[return-value]
        name=flow.name,
        schedules=schedules,
        paused=paused,
        # avoid having multiple runs of the same flow running concurrently.
        # Sometimes, flow runs get stuck. Then they occupy the concurrency limit and no new runs can start.
        # You can cancel these stuck runs with `prefect flow-run ls --state Running` and `prefect flow-run cancel <id>`.
        # If the run then gets stuck in 'Cancelling' state, you might need to delete it `prefect flow-run delete <id>`
        concurrency_limit=ConcurrencyLimitConfig(limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW),
        description=description,
        enforce_parameter_schema=True,
        work_pool_name="cloud-run-v2",
        work_queue_name="default",
        job_variables=job_variables,
        triggers=triggers,
    )


def get_deployments() -> List[RunnerDeployment]:
    return [
        deployment_for_flow(hello_universe, description="A simple example pipeline, also used as a smoketest from CI"),
        deployment_for_flow(
            all_to_typesense_ingest_from_scratch_flow,
            description="Orchestrates the ETL pipeline to ingest all data to Typesense",
            job_variables_overrides={"memory": "4096Mi"},
        ),
        deployment_for_flow(
            volunteering_to_typesense_ingest_from_scratch_flow,
            description="Ingests volunteering opportunities from BigQuery into Typesense",
            job_variables_overrides={"memory": "4096Mi"},
        ),
        deployment_for_flow(
            profiles_to_typesense_ingest_from_scratch_flow,
            description="Ingests user profiles from Datastream-replicated Okuna into Typesense",
            job_variables_overrides={"memory": "4096Mi"},
        ),
        deployment_for_flow(
            spaces_to_typesense_ingest_from_scratch_flow,
            description="Ingests spaces from Datastream-replicated Okuna into Typesense",
            job_variables_overrides={"memory": "4096Mi"},
        ),
        deployment_for_flow(
            task_classification_flow,
            description="Classifies tasks from Datastream-replicated Okuna",
            job_variables_overrides={"memory": "4096Mi"},
        ),
        deployment_for_flow(
            tasks_to_typesense_ingest_from_scratch_flow,
            description="Ingests tasks from Datastream-replicated Okuna into Typesense",
            job_variables_overrides={"memory": "4096Mi"},
        ),
        deployment_for_flow(
            crawl_aktion_mensch,
            description="Crawls Aktion Mensch for volunteering opportunities and ingests them into BigQuery",
            job_variables_overrides={"memory": "1024Mi"},
        ),
        deployment_for_flow(
            crawl_voltastics,
            description="Crawls voltastics.com for volunteering opportunities and ingests them into BigQuery",
            job_variables_overrides={"memory": "1024Mi"},
        ),
        deployment_for_flow(
            volunteering_updates,
            description="Orchestrates crawling, classification and enrichment and updating Typesense indexes for volunteering",
            schedules=[cron_schedule("40 1 * * *") if environment() == "production" else cron_schedule("40 2 * * *")],
            job_variables_overrides={"memory": "1024Mi"},
        ),
        deployment_for_flow(
            crawl_betterplace,
            description="Crawls betterplace.org for charity projects and ingests them into BigQuery",
            job_variables_overrides={"memory": "1024Mi"},
            paused=True,
        ),
        deployment_for_flow(
            typesense_qc,
            description="Quality check for the state of replication of entities into Typesense",
            schedules=[cron_schedule("00 7 * * *")],
            job_variables_overrides={"memory": "1024Mi"},
        ),
        deployment_for_flow(
            aktion_mensch_classification,
            description="Runs classifications on unclassified Aktion Mensch volunteering opportunities",
            job_variables_overrides={"memory": "4096Mi"},
        ),
        deployment_for_flow(
            volunteering_classification,
            description="Runs classifications on unclassified volunteering opportunities",
            job_variables_overrides={"memory": "4096Mi"},
        ),
        deployment_for_flow(
            volunteering_geo_enrichment,
            description="Resolves lat/lng for volunteering opportunities lacking those",
            job_variables_overrides={"memory": "1024Mi"},
        ),
        deployment_for_flow(
            volunteering_to_typesense_ingest_updates_flow,
            description="Orchestrates the ETL pipeline to move volunteering updates from BigQuery to Typesense",
            job_variables_overrides={"memory": "4096Mi"},
        ),
        deployment_for_flow(
            aktion_mensch_to_typesense_ingest_from_scratch_flow,
            description="Ingests Aktion Mensch volunteering opportunities from BigQuery into Typesense from scratch",
            job_variables_overrides={"memory": "4096Mi"},
        ),
        deployment_for_flow(
            aktion_mensch_to_typesense_ingest_updates_flow,
            description="Orchestrates the ETL pipeline to move Aktion Mensch volunteering updates from BigQuery to Typesense",
            job_variables_overrides={"memory": "4096Mi"},
        ),
    ]


if __name__ == "__main__":
    # FIXME remove MyPy ignore comment once https://github.com/PrefectHQ/prefect/issues/16825 has a resolution
    deploy(  # type: ignore[unused-coroutine]
        *get_deployments(),
        work_pool_name="cloud-run-v2",
        image=default_docker_image_tag(),
        build=False,
        push=False,
        print_next_steps_message=False,
    )
