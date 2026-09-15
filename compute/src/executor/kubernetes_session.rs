use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use aruna_core::compute::runtimes::{SESSION_CLIENT_MODE, SESSION_HELPER_PATH};
use aruna_core::compute::{BackendError, FenceContext};
use k8s_openapi::api::authorization::v1::SelfSubjectAccessReview;
use kube::Client;
use kube::ResourceExt;
use kube::api::{Api, AttachParams, PostParams};
use serde_json::json;
use tokio::io::AsyncRead;

use super::status::task_state;
use super::{STREAM_BUF_BYTES, KubernetesBackend, SessionChannel, kube_error};

/// Bridges standard input and output of one exec to the helper socket. The
/// exec inherits the container's user and is never privileged.
pub(super) async fn open(
    backend: &KubernetesBackend,
    context: &FenceContext,
) -> Result<SessionChannel, BackendError> {
    let pods = backend.task_pods(context).await?;
    let pod = pods
        .iter()
        .find(|pod| task_state(pod).is_some_and(|state| state.running.is_some()))
        .ok_or_else(|| {
            BackendError::Conflict(format!(
                "attempt `{}` has no running task pod",
                context.attempt.external_name()
            ))
        })?;
    let params = AttachParams::default()
        .container("task")
        .stdin(true)
        .stdout(true)
        .stderr(false)
        .max_stdin_buf_size(STREAM_BUF_BYTES)
        .max_stdout_buf_size(STREAM_BUF_BYTES);
    let mut attached = backend
        .pods()
        .exec(
            &pod.name_any(),
            [SESSION_HELPER_PATH, SESSION_CLIENT_MODE],
            &params,
        )
        .await
        .map_err(kube_error)?;
    let input = attached.stdin().ok_or_else(|| {
        BackendError::Api("session exec did not expose standard input".to_string())
    })?;
    let output = attached.stdout().ok_or_else(|| {
        BackendError::Api("session exec did not expose standard output".to_string())
    })?;
    Ok(SessionChannel {
        input: Box::pin(input),
        output: Box::pin(SessionReader {
            inner: output,
            _process: attached,
        }),
    })
}

struct SessionReader<R> {
    inner: R,
    _process: kube::api::AttachedProcess,
}

impl<R: AsyncRead + Unpin> AsyncRead for SessionReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

pub(super) fn required_access(
    s3_mount: bool,
) -> Vec<(
    &'static str,
    &'static str,
    Option<&'static str>,
    &'static str,
)> {
    let mut access = vec![
        ("batch", "jobs", None, "create"),
        ("batch", "jobs", None, "get"),
        ("batch", "jobs", None, "list"),
        ("batch", "jobs", None, "watch"),
        ("batch", "jobs", None, "patch"),
        ("batch", "jobs", None, "delete"),
        ("", "pods", None, "create"),
        ("", "pods", None, "get"),
        ("", "pods", None, "list"),
        ("", "pods", None, "watch"),
        ("", "pods", None, "delete"),
        ("", "pods", Some("exec"), "create"),
        ("", "pods", Some("exec"), "get"),
        ("", "pods", Some("log"), "get"),
        ("", "persistentvolumes", None, "create"),
        ("", "persistentvolumes", None, "get"),
        ("", "persistentvolumes", None, "list"),
        ("", "persistentvolumes", None, "delete"),
        ("", "persistentvolumeclaims", None, "create"),
        ("", "persistentvolumeclaims", None, "get"),
        ("", "persistentvolumeclaims", None, "list"),
        ("", "persistentvolumeclaims", None, "watch"),
        ("", "persistentvolumeclaims", None, "delete"),
        ("", "secrets", None, "create"),
        ("", "secrets", None, "get"),
        ("", "secrets", None, "delete"),
        ("", "configmaps", None, "create"),
        ("", "configmaps", None, "get"),
        ("", "configmaps", None, "patch"),
        ("", "configmaps", None, "delete"),
        ("", "serviceaccounts", None, "get"),
        ("networking.k8s.io", "networkpolicies", None, "create"),
        ("networking.k8s.io", "networkpolicies", None, "get"),
        ("networking.k8s.io", "networkpolicies", None, "patch"),
        ("storage.k8s.io", "storageclasses", None, "get"),
    ];
    if s3_mount {
        access.push(("storage.k8s.io", "csidrivers", None, "get"));
    }
    access
}

pub(super) async fn check_access(
    client: Client,
    namespace: &str,
    group: &str,
    resource: &str,
    subresource: Option<&str>,
    verb: &str,
) -> Result<(), BackendError> {
    let reviews: Api<SelfSubjectAccessReview> = Api::all(client);
    let review: SelfSubjectAccessReview = serde_json::from_value(json!({
        "apiVersion":"authorization.k8s.io/v1",
        "kind":"SelfSubjectAccessReview",
        "spec":{"resourceAttributes":{
            "namespace":if matches!(resource,"storageclasses" | "csidrivers" | "persistentvolumes") { None } else { Some(namespace) },
            "group":group,
            "resource":resource,
            "subresource":subresource,
            "verb":verb,
            "name":if matches!(resource,"storageclasses" | "csidrivers") { Some("") } else { None }
        }}
    }))
    .map_err(|error| BackendError::Api(format!("build access review: {error}")))?;
    let result = reviews
        .create(&PostParams::default(), &review)
        .await
        .map_err(kube_error)?;
    if result.status.is_some_and(|status| status.allowed) {
        Ok(())
    } else {
        Err(BackendError::Unavailable(format!(
            "Kubernetes access denied for {verb} {group}/{resource}"
        )))
    }
}
