import json
from typing import Any, Mapping

from prefect import runtime
from prefect.blocks.notifications import SlackWebhook
from prefect.client.schemas.objects import FlowRun, State, StateType
from prefect.flows import Flow
from prefect.types import StrictVariableValue
from prefect.variables import Variable
from typing_extensions import assert_never

from src.common.logging.PipelineLogger import PipelineLogger

logger = PipelineLogger(__name__)


def _msg_run_link(run: FlowRun) -> str:
    return f"{_prefect_base_url()}/runs/flow-run/{run.id}"


def _compile_context_info(context: Mapping[str, StrictVariableValue], run: FlowRun, state: State) -> str:
    context_info = []
    if state.message:
        context_info.append(f"*{state.message}*\n")
    context_info.append(f"Flow run URL: {_msg_run_link(run)}\n")
    if context:
        context_info.append(f"Context information: `{json.dumps(context)}`\n")
    if runtime.task_run.task_name:
        context_info.append(f"Current task `{runtime.task_run.task_name}`.\n")
    if run.context:
        context_info.append(f"Run context `{json.dumps(run.context)}`.\n")
    if run.parameters:
        context_info.append(f"Run parameters `{json.dumps(run.parameters)}`.\n")
    if run.job_variables:
        context_info.append(f"Run job variables `{json.dumps(run.job_variables)}`.\n")
    if run.tags:
        context_info.append(f"Run tags `{','.join(run.tags)}`\n")
    if run.labels:
        context_info.append(f"Run labels `{','.join(run.labels)}`.\n")
    if (
        run.expected_start_time
        or run.next_scheduled_start_time
        or run.start_time
        or run.end_time
        or run.total_run_time
        or run.estimated_run_time
        or run.estimated_start_time_delta
    ):
        context_info.append("Temporal information:\n")
        if run.expected_start_time:
            context_info.append(f"\tExpected start time `{run.expected_start_time}`\n")
        if run.next_scheduled_start_time:
            context_info.append(f"\tNext scheduled start time `{run.next_scheduled_start_time}`\n")
        if run.start_time:
            context_info.append(f"\tStart time `{run.start_time}`\n")
        if run.end_time:
            context_info.append(f"\tEnd time `{run.end_time}`\n")
        if run.total_run_time:
            context_info.append(f"\tTotal run duration `{run.total_run_time}`\n")
        if run.estimated_run_time:
            context_info.append(f"\tEstimated run duration `{run.estimated_run_time}`\n")
        if run.estimated_start_time_delta:
            context_info.append(f"\tEstimated start time delta `{run.estimated_start_time_delta}`\n")
    return "".join(context_info)


def _in_development(run: FlowRun) -> bool:
    # We don't want to alert on runs happening during development, e.g. runs on developer's boxes.
    # These runs don't have a deployment and are not auto-scheduled
    return run.deployment_id is None and not run.auto_scheduled


def _prefect_base_url() -> str:
    # this is hacky as the code shouldn't know about it's deployment environment but a very quick pragmatic solution
    # right now
    if Variable.get("prefect_environment") == "production":
        return "http://prefect-server-production.holi.internal:8420"
    else:
        return "http://prefect-server-staging.holi.internal:8421"


def _msg_state(state: State) -> str:
    match state.type:
        case StateType.SCHEDULED:
            return "was scheduled ⏱️"
        case StateType.PENDING:
            return "is pending 🫸"
        case StateType.RUNNING:
            return "is running 🏃"
        case StateType.COMPLETED:
            return "completed ✅"
        case StateType.FAILED:
            return "failed 😩"
        case StateType.CANCELLED:
            return "was cancelled ❌"
        case StateType.CRASHED:
            return "crashed 💥"
        case StateType.PAUSED:
            return "paused ⏸️"
        case StateType.CANCELLING:
            return "is cancelling ❌⚙️"
        case _:
            raise ValueError(f"Unexpected state type: {state.type}")


def alert(
    context: Mapping[str, StrictVariableValue],
    flow: Flow[Any, Any],
    run: FlowRun,
    state: State,
) -> None:
    if _in_development(run):
        return
    else:
        alerter: SlackWebhook = SlackWebhook.load("slack-alert")  # type: ignore[assignment]
        subject = f'Prefect flow "{flow.name}" {_msg_state(state)}'
        match state.type:
            case StateType.SCHEDULED | StateType.PENDING | StateType.RUNNING | StateType.COMPLETED:
                # not interesting to report on
                return
            # PAUSED and CANCELLING are maybe not interesting to us, but for now we include them
            case StateType.FAILED | StateType.CANCELLED | StateType.CRASHED | StateType.PAUSED | StateType.CANCELLING:
                alerter.notify(  # type: ignore[unused-coroutine]
                    subject=subject, body=_compile_context_info(context, run, state)
                )
                return
            case _:
                assert_never(state.type)
