import platform
from typing import Any, Optional

import pendulum
from opentelemetry import metrics
from opentelemetry.exporter.cloud_monitoring import (
    CloudMonitoringMetricsExporter,
)
from opentelemetry.metrics import Meter
from opentelemetry.resourcedetector.gcp_resource_detector import GoogleCloudResourceDetector, NoGoogleResourcesFound
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes
from prefect import runtime
from prefect.client.schemas.objects import FlowRun, State
from prefect.flows import Flow
from trycast import isassignable, checkcast  # type: ignore

from src.common.logging.PipelineLogger import PipelineLogger
from src.common.variables import variable_value

_logger = PipelineLogger(__name__)


def _get_resource() -> Resource:
    try:
        return GoogleCloudResourceDetector(raise_on_error=True).detect()  # type: ignore[no-untyped-call]
    except NoGoogleResourcesFound:
        return Resource.create(
            {
                ResourceAttributes.HOST_NAME: platform.node(),
                ResourceAttributes.SERVICE_NAMESPACE: "prefect_local",
            }
        )


metrics.set_meter_provider(
    MeterProvider(
        metric_readers=[
            PeriodicExportingMetricReader(
                # https://cloud.google.com/monitoring/api/metrics_other
                CloudMonitoringMetricsExporter(prefix="external.googleapis.com/user"),
                export_interval_millis=5000,
            )
        ],
        resource=_get_resource(),
    ),
)
_meter = metrics.get_meter("opentelemetry.instrumentation.pipelines")

_common_counters = {
    "pipeline_run.count": _meter.create_counter(
        name="pipeline_run.count",
        description="number of pipeline runs",
        unit="1",
    ),
}
_common_gauges = {
    "pipeline_run.duration": _meter.create_gauge(
        name="pipeline_run.duration", description="duration of pipeline runs", unit="ms"
    ),
    "pipeline_run.processed_entities": _meter.create_gauge(
        name="pipeline_run.processed_entities", description="number of processed entities in a pipeline run", unit="1"
    ),
}


def get_meter() -> Meter:
    """
    Get the meter in order to create custom metrics. Before creating custom metrics, try to design metrics common to all
    pipelines and keep them contained in this module, accessible via a report_* function.
    See report_number_of_processed_entities
    :return: the meter instance for creating custom metrics
    """
    return _meter


def report_number_of_processed_entities(no_processed: int) -> None:
    _common_gauges["pipeline_run.processed_entities"].set(no_processed, get_labels())


_STATIC_LABELS: Optional[dict[str, str]] = None


def _get_static_labels() -> dict[str, str]:
    """
    Get the static labels that are common to all metrics. These labels are set once and are not changed during the
    lifetime of the process in order to avoid querying prefect variables every time a metric is reported.

    :return: the labels that are static over the lifetime of the process.
    """
    global _STATIC_LABELS
    if _STATIC_LABELS is None:
        environment = variable_value("prefect_environment")
        if not isassignable(environment, str):
            environment = "undefined"
        _STATIC_LABELS = {
            "service": "prefect",
            "component": "pipeline",
            "prefect_environment": checkcast(str, environment),
        }
    return _STATIC_LABELS


def _get_prefect_runtime_labels() -> dict[str, str]:
    """
    Get the prefect runtime specific labels that are common to all metrics. These labels are determined at call time,
    from the prefect.runtime objects. They include (if available) the deployment name, task name, flow run name and flow
    name.

    :return: the labels that are prefect runtime specific.
    """
    labels: dict[str, str] = {}
    deployment_name = runtime.deployment.get_name()
    if deployment_name:
        labels.update({"prefect_deployment_name": deployment_name})
    task_name = runtime.task_run.get_task_name()
    if task_name:
        labels.update({"prefect_task_name": task_name})
    flow_run_name = runtime.flow_run.get_name()
    if flow_run_name:
        labels.update({"prefect_flow_run_name": flow_run_name})
    flow_name = runtime.flow_run.get_flow_name()
    if flow_name:
        labels.update({"prefect_flow_name": flow_name})
    return labels


def get_labels() -> dict[str, str]:
    """
    Get the labels that are common to all metrics. Combine these with any custom labels when you define custom metrics.
    Some labels included are the prefect environment, deployment name, task name, flow run name and flow name.

    :return: the labels that are common to all metrics.
    """
    return _get_static_labels() | _get_prefect_runtime_labels()


def monitor(
    _flow: Flow[Any, Any],
    _run: FlowRun,
    _state: State,
) -> None:
    """
    This function is called whenever a flow is run, completed, cancelled, crashed or failed.
    It increments the pipeline_runs counter.
    """
    _common_counters["pipeline_run.count"].add(1, get_labels() | {"prefect_flow_state": _state.type.lower()})

    start_time = _run.start_time
    if start_time:
        full_timedelta = pendulum.now("utc") - start_time
        _common_gauges["pipeline_run.duration"].set(
            full_timedelta.microseconds / 1000.0,
            get_labels() | {"prefect_flow_state": _state.type.lower()},
        )
    else:
        _logger.warning(f"Flow run has no start time, cannot calculate duration. State {_state.type}")
