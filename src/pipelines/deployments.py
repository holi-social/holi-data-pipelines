from typing import List, Optional, Any, cast

from prefect import deploy, Flow
from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments.runner import RunnerDeployment
from prefect.deployments.schedules import FlexibleScheduleList

from src.common.variables import variable_value_as
from src.pipelines.all_to_typesense.ingest_from_scratch import all_to_typesense_ingest_from_scratch_flow
from src.pipelines.crawlers.betterplace.pipeline import crawl_betterplace
from src.pipelines.crawlers.voltastics.pipeline import crawl_voltastics
from src.pipelines.example.pipeline import hello_universe
from src.pipelines.profiles_to_typesense.ingest_from_scratch import profiles_to_typesense_ingest_from_scratch_flow
from src.pipelines.spaces_to_typesense.ingest_from_scratch import spaces_to_typesense_ingest_from_scratch_flow
from src.pipelines.tasks_to_typesense.ingest_from_scratch import tasks_to_typesense_ingest_from_scratch_flow
from src.pipelines.typesense_qc.typesense_qc import typesense_qc
from src.pipelines.volunteering_to_typesense.ingest_updates import volunteering_to_typesense_ingest_updates_flow


def environment() -> str:
    # FIXME remove cast once https://github.com/PrefectHQ/prefect/issues/16825 has a resolution
    return cast(str, variable_value_as(str, "prefect_environment"))


def default_docker_image_tag() -> str:
    return f"europe-north1-docker.pkg.dev/holi-shared/docker/data-pipeline-job-{environment()}:latest"


def cron_schedule(expression: str) -> CronSchedule:
    return CronSchedule(
        cron=expression,
        timezone="Europe/Berlin",
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
            volunteering_to_typesense_ingest_updates_flow,
            description="Orchestrates the ETL pipeline to move volunteering updates from BigQuery to Typesense",
            schedules=[cron_schedule("30 6 * * *")],
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
            tasks_to_typesense_ingest_from_scratch_flow,
            description="Ingests tasks from Datastream-replicated Okuna into Typesense",
            job_variables_overrides={"memory": "4096Mi"},
        ),
        deployment_for_flow(
            crawl_voltastics,
            description="Crawls voltastics.com for volunteering opportunities and ingests them into BigQuery",
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
