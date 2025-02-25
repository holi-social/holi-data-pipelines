from functools import partial
from typing import Any, Optional, List, Callable, cast, Mapping

from prefect.client.schemas.objects import FlowRun, State
from prefect.flows import Flow, flow, P, R
from prefect.types import StrictVariableValue

from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.alerting import alert
from src.common.monitoring.metrics import record_after_flow_metrics

_logger = PipelineLogger(__name__)

# This is the configuration for the hooks that will be added to the flow. Alerting happens on run cancellation, crashing
# and failure. Metrics collection happens at the end of the run.
_HOOK_CONFIGURATION: dict[
    str, Callable[[Mapping[str, StrictVariableValue]], list[Callable[[Flow[Any, Any], FlowRun, State], None]]]
] = {
    "on_cancellation": lambda context: [record_after_flow_metrics, partial(alert, context)],
    "on_crashed": lambda context: [record_after_flow_metrics, partial(alert, context)],
    "on_failure": lambda context: [record_after_flow_metrics, partial(alert, context)],
    "on_completion": lambda context: [record_after_flow_metrics],
}


def monitored_flow(
    *args: Any,
    **kwargs: Any,
) -> Flow[P, R]:
    """
    This decorator extends :func:`~prefect.flows.flow`. It sets up the appropriate on_* hooks to alert on the flow as
    well as to write metrics to GCP.

    :param args: any unnamed args to the function
    :param kwargs: any arguments defined by :func:`~prefect.flows.flow`
    :return: a configured :func:`~prefect.flows.flow` decorator.
    """

    context: Optional[dict[str, StrictVariableValue]] = cast(
        Optional[dict[str, StrictVariableValue]], kwargs.pop("context", [])
    )
    safe_context = context if context else {}

    for key in _HOOK_CONFIGURATION.keys():
        passed_hooks: Optional[List[Callable[[Flow[P, R], FlowRun, State], None]]] = cast(
            List[Callable[[Flow[P, R], FlowRun, State], None]], kwargs.pop(key, [])
        )
        kwargs[key] = _HOOK_CONFIGURATION[key](safe_context) + (
            [passed_hooks] if not isinstance(passed_hooks, list) else passed_hooks
        )

    return flow(*args, **kwargs)
