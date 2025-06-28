from typing import Any, List

from prefect.flows import Flow, flow, P, R, FlowStateHook

from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.alerting import alert_hook
from src.common.monitoring.metrics import Metrics

_logger = PipelineLogger(__name__)

on_cancellation_hooks: List[FlowStateHook[..., Any]] = [Metrics().record_after_flow_metrics, alert_hook]
on_crashed_hooks: List[FlowStateHook[..., Any]] = [Metrics().record_after_flow_metrics, alert_hook]
on_failure_hooks: List[FlowStateHook[..., Any]] = [Metrics().record_after_flow_metrics, alert_hook]
on_completion_hooks: List[FlowStateHook[..., Any]] = [Metrics().record_after_flow_metrics]

flow_hooks: dict[str, List[FlowStateHook[..., Any]]] = {
    "on_cancellation": on_cancellation_hooks,
    "on_crashed": on_crashed_hooks,
    "on_failure": on_failure_hooks,
    "on_completion": on_completion_hooks,
}


def monitored_flow(*args: Any, **kwargs: Any) -> Flow[P, R]:
    all_kwargs = {**kwargs, **flow_hooks}
    return flow(*args, **all_kwargs)
