import platform
import os
from typing import Any, Optional, Mapping, Type

from whenever import Instant
from opentelemetry import metrics
from opentelemetry.exporter.cloud_monitoring import (
    CloudMonitoringMetricsExporter,
)
from opentelemetry.metrics import Meter
from opentelemetry.resourcedetector.gcp_resource_detector import (
    _metadata,  # noqa
    _get_google_metadata_and_common_attributes,  # noqa
)
from opentelemetry.resourcedetector.gcp_resource_detector._detector import (  # noqa
    _make_resource,  # noqa
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from prefect import runtime
from prefect.client.schemas.objects import FlowRun, State
from prefect.flows import Flow

from src.common.logging.PipelineLogger import PipelineLogger
from src.common.variables import variable_value_as

_logger = PipelineLogger(__name__)


def _get_prefect_runtime_labels() -> dict[str, str]:
    """
    Get the prefect runtime specific labels that are common to all metrics. These labels are determined at call time,
    from the prefect.runtime objects. They include (if available) the deployment name, task name, flow run name and flow
    name.

    :return: the labels that are prefect runtime specific.
    """
    labels: dict[str, str] = {}
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


def _get_resource() -> Resource:
    flow_run_name = runtime.flow_run.get_name() or "unknown"
    flow_name = runtime.flow_run.get_flow_name() or "unknown"
    environment_value = variable_value_as(str, "prefect_environment", "undefined")
    # we want to record metrics using generic_task as resource type. This does not work right now
    # debugging needed. the auto resource detector is a good source for debugging.
    # https://cloud.google.com/monitoring/api/resources#tag_generic_task
    if not _metadata.is_available():  # we're not running in GCP, probably a developer's environment
        return Resource.create(
            {
                "host.id": f"local:{platform.node()}",
                "gcp.resource_type": "generic_task",
                "project_id": "holi-shared",  # hardcoded, can't be determined on a resource outside GCP
                "location": "global",
                "namespace": environment_value,
                "job": flow_name,
                "task_id": flow_run_name,
            }
        )
    else:
        (
            common_attributes,
            all_metadata,
        ) = _get_google_metadata_and_common_attributes()  # type: ignore
        common_attributes.update(
            {
                "host.id": all_metadata["instance"]["id"],
                "gcp.resource_type": "generic_task",
                "project_id": all_metadata["project"]["projectId"],
                "location": _get_location_from_metadata(all_metadata),
                "namespace": environment_value,  # this could be the flow or environment
                "job": flow_name,  # this should be the flow name
                "task_id": flow_run_name,  # this should be the flow run id
            }
        )
        _logger.warning(f"GCP resource attributes: ${common_attributes.keys()}")
        return _make_resource(common_attributes)


def _get_location_from_metadata(metadata: Mapping[str, Any]) -> str:
    return metadata["instance"].get("region", metadata["instance"].get("zone", "/unknown")).split("/")[-1]


def record_after_flow_metrics(
    _flow: Flow[Any, Any],
    _run: FlowRun,
    _state: State,
) -> None:
    """
    This function is called whenever a flow is completed, cancelled, crashed or failed.
    It increments the pipeline_runs counter.
    """
    metrics_instance = Metrics()
    counter = metrics_instance._common_counters["pipeline_run.count"].add(  # noqa
        1, metrics_instance.get_labels() | {"prefect_flow_state": _state.type.lower()}
    )

    start_time = _run.start_time
    if start_time:
        full_timedelta = Instant.now() - Instant.from_py_datetime(start_time)
        metrics_instance._common_gauges["pipeline_run.duration"].set(  # noqa
            full_timedelta.in_seconds(),
            metrics_instance.get_labels() | {"prefect_flow_state": _state.type.lower()},
        )
    else:
        _logger.warning(f"Flow run has no start time, cannot calculate duration. State {_state.type}")


def report_number_of_processed_entities(no_processed: int) -> None:
    metrics_instance = Metrics()
    metrics_instance._common_gauges["pipeline_run.processed_entities"].set(no_processed, metrics_instance.get_labels())  # noqa


class Metrics:
    """
    This class is a singleton that provides access to the meter for creating custom metrics. It must not be
    instantiated outside a flow, because it uses prefect.runtime objects to determine the labels for the metrics.
    """

    _instance: Optional["Metrics"] = None

    def __new__(cls: Type["Metrics"]) -> "Metrics":
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls)
            cls._instance._initialize_once()
        return cls._instance

    def _initialize_once(self) -> None:
        metrics_readers = (
            []
            if os.environ.get("PYTEST_VERSION")
            is not None  # see https://doc.pytest.org/en/latest/example/simple.html#detect-if-running-from-within-a-pytest-run
            else [
                PeriodicExportingMetricReader(
                    # https://cloud.google.com/monitoring/api/metrics_other
                    CloudMonitoringMetricsExporter(prefix="external.googleapis.com/user"),
                    export_interval_millis=5000,
                )
            ]
        )
        metrics.set_meter_provider(
            MeterProvider(
                metric_readers=metrics_readers,
                resource=_get_resource(),
            ),
        )
        self._meter: Meter = metrics.get_meter("opentelemetry.instrumentation.pipelines")
        self._common_counters: dict[str, Any] = {
            "pipeline_run.count": self._meter.create_counter(
                name="pipeline_run.count",
                description="number of pipeline runs",
                unit="1",
            ),
        }
        self._common_gauges: dict[str, Any] = {
            "pipeline_run.duration": self._meter.create_gauge(
                name="pipeline_run.duration", description="duration of pipeline runs", unit="s"
            ),
            "pipeline_run.processed_entities": self._meter.create_gauge(
                name="pipeline_run.processed_entities",
                description="number of processed entities in a pipeline run",
                unit="1",
            ),
        }
        self._static_labels: Optional[dict[str, str]] = None

    def get_meter(self) -> Meter:
        """
        Get the meter in order to create custom metrics. Before creating custom metrics, try to design metrics common to all
        pipelines and keep them contained in this module, accessible via a report_* function.
        See report_number_of_processed_entities
        :return: the meter instance for creating custom metrics
        """
        return self._meter

    def _get_static_labels(self) -> dict[str, str]:
        """
        Get the static labels that are common to all metrics. These labels are set once and are not changed during the
        lifetime of the process in order to avoid querying prefect variables every time a metric is reported.

        :return: the labels that are static over the lifetime of the process.
        """
        if self._static_labels is None:
            self._static_labels = {
                "service": "prefect",
                "component": "pipeline",
                "prefect_environment": variable_value_as(str, "prefect_environment", "undefined"),
            }
        return self._static_labels

    def get_labels(self) -> dict[str, str]:
        """
        Get the labels that are common to all metrics. Combine these with any custom labels when you define custom metrics.
        Some labels included are the prefect environment, deployment name, task name, flow run name and flow name.

        :return: the labels that are common to all metrics.
        """
        return self._get_static_labels() | _get_prefect_runtime_labels()

    def record_after_flow_metrics(
        self,
        flow: Flow[Any, Any],
        flow_run: FlowRun,
        state: State,
    ) -> None:
        """
        This function is called whenever a flow is completed, cancelled, crashed or failed.
        It increments the pipeline_runs counter.
        """
        self._common_counters["pipeline_run.count"].add(
            1, self.get_labels() | {"prefect_flow_state": state.type.lower()}
        )

        start_time = flow_run.start_time
        if start_time:
            full_timedelta = Instant.now() - Instant.from_py_datetime(start_time)
            self._common_gauges["pipeline_run.duration"].set(
                full_timedelta.in_seconds(),
                self.get_labels() | {"prefect_flow_state": state.type.lower()},
            )
        else:
            _logger.warning(f"Flow run has no start time, cannot calculate duration. State {state.type}")
