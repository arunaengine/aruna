use std::time::Duration;

use aruna_core::compute::{AttemptPhase, AttemptStatus};
use aruna_core::structs::execution::job::tail_str;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ContainerState, ContainerStateTerminated, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::jiff::Timestamp;
use kube::ResourceExt;

use super::{MAX_TERMINATION_DETAIL, job_state};

pub(super) fn job_status(job: &Job) -> AttemptStatus {
    let phase = if job_state(job) == Some("cancelled") {
        AttemptPhase::Cancelled
    } else if job
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Complete" && condition.status == "True")
        })
    {
        AttemptPhase::Exited { code: 0 }
    } else if let Some(condition) = job
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.type_ == "Failed" && condition.status == "True")
        })
    {
        // A Job-level failure with no terminated container carries no evidence
        // about the payload itself; a terminated one overrides this phase.
        AttemptPhase::SystemError {
            reason: condition
                .message
                .clone()
                .or_else(|| condition.reason.clone())
                .unwrap_or_else(|| "Kubernetes Job failed".to_string()),
        }
    } else if job
        .status
        .as_ref()
        .and_then(|status| status.active)
        .unwrap_or(0)
        > 0
    {
        AttemptPhase::Running
    } else {
        AttemptPhase::Submitted
    };
    let times = job.status.as_ref();
    AttemptStatus {
        phase,
        backend_ref: job.metadata.uid.clone().unwrap_or_else(|| job.name_any()),
        started_at_ms: time_ms(times.and_then(|status| status.start_time.as_ref())),
        finished_at_ms: time_ms(times.and_then(|status| status.completion_time.as_ref())),
        detail: None,
    }
}

/// Reason and message of a terminated container, bounded from the end because
/// the fallback message holds the last log lines.
pub(super) fn termination_detail(state: &ContainerStateTerminated) -> Option<String> {
    let reason = state.reason.as_deref().unwrap_or_default().trim();
    let message = state.message.as_deref().unwrap_or_default().trim();
    let detail = match (reason.is_empty(), message.is_empty()) {
        (true, true) => return None,
        (false, true) => reason.to_string(),
        (true, false) => message.to_string(),
        (false, false) => format!("{reason}: {message}"),
    };
    Some(tail_str(&detail, MAX_TERMINATION_DETAIL).to_string())
}

pub(super) fn time_ms(time: Option<&Time>) -> Option<u64> {
    u64::try_from(time?.0.as_millisecond()).ok()
}

/// Active but unready Pods are the only ones that can hold a stuck container.
pub(super) fn job_pending(job: &Job) -> bool {
    job.status
        .as_ref()
        .is_some_and(|status| status.active.unwrap_or(0) > 0 && status.ready.unwrap_or(0) == 0)
}

/// Reasons the kubelet reports only after repeated failed attempts. A bare
/// `ErrImagePull` is a single try, so it never proves a stuck container.
pub(super) fn repeated_wait(reason: &str) -> bool {
    matches!(
        reason,
        "ImagePullBackOff" | "CreateContainerConfigError" | "CreateContainerError"
    )
}

/// A registry that refuses the reference outright (unknown name, no access)
/// answers the same on every retry, so waiting out the deadline gains nothing.
pub(super) fn pull_refused(reason: &str, message: Option<&str>) -> bool {
    if !matches!(reason, "ErrImagePull" | "ImagePullBackOff") {
        return false;
    }
    let message = message.unwrap_or_default().to_ascii_lowercase();
    [
        "401 unauthorized",
        "403 forbidden",
        "404 not found",
        "manifest unknown",
        "name unknown",
        "pull access denied",
        "repository does not exist",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

/// Kubernetes retries a failing image pull forever, so the Job counts such a Pod
/// as active and never reports a terminal condition. A malformed or refused
/// reference fails at once; a repeated one fails once it outlives `deadline`.
pub(super) fn pod_stuck_reason(pod: &Pod, deadline: Duration, now: Timestamp) -> Option<String> {
    let status = pod.status.as_ref()?;
    let waited = status
        .start_time
        .as_ref()
        .or(pod.metadata.creation_timestamp.as_ref())
        .map(|since| Duration::try_from(now.duration_since(since.0)).unwrap_or_default())
        .unwrap_or_default();
    status
        .init_container_statuses
        .iter()
        .chain(status.container_statuses.iter())
        .flatten()
        .find_map(|container| {
            let waiting = container.state.as_ref()?.waiting.as_ref()?;
            let reason = waiting.reason.as_deref()?;
            if reason != "InvalidImageName"
                && !pull_refused(reason, waiting.message.as_deref())
                && !(repeated_wait(reason) && waited > deadline)
            {
                return None;
            }
            let detail = waiting.message.as_deref().unwrap_or("no detail reported");
            Some(format!(
                "container `{}` cannot start ({reason}): {detail}",
                container.name
            ))
        })
}

/// Log reads only succeed once the task container has left its waiting state.
pub(super) fn task_started(pod: &Pod) -> bool {
    task_state(pod).is_some_and(|state| state.running.is_some() || state.terminated.is_some())
}

pub(super) fn task_state(pod: &Pod) -> Option<&ContainerState> {
    pod.status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .iter()
        .find(|status| status.name == "task")?
        .state
        .as_ref()
}

/// The container start survives its exit, so both states carry the evidence.
pub(super) fn pod_started(pod: &Pod) -> Option<u64> {
    let state = task_state(pod)?;
    match (state.running.as_ref(), state.terminated.as_ref()) {
        (Some(running), _) => time_ms(running.started_at.as_ref()),
        (None, Some(terminated)) => time_ms(terminated.started_at.as_ref()),
        (None, None) => None,
    }
}
