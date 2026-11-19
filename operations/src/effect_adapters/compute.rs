//! Carries out compute calls on the executor backends the registry holds. A running job's stop
//! reaches the backend calls that take a cancel token through the job's host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::sync::Arc;

use aruna_compute::ExecutorBackend;
use aruna_compute::ExecutorRegistry;
use aruna_compute::session::SessionConfig;
use aruna_core::compute::effect::{
    AttemptEnd, BackendFacts, ComputeAnswer, ComputeEffect, ComputeRequest, OutputBody,
    OutputFailure, SessionOpen, WorkspaceAddress,
};
use aruna_core::compute::session::EndReason;
use aruna_core::compute::{BackendError, FenceContext, OutputChunks, TaskOutput};
use aruna_core::stream::{BackendStream, StreamError};
use futures_util::StreamExt;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::job::{JobHost, current_host, run_session};
use crate::driver::DriverContext;

fn facts(backend: &Arc<dyn ExecutorBackend>) -> BackendFacts {
    let caps = backend.capabilities();
    BackendFacts {
        kind: backend.kind(),
        s3_mount: caps.s3_mount,
        session: caps.session,
        local_site: caps.local_site,
        worker_location: caps.worker_site.map(|site| site.location),
        run_as: backend.run_identity(),
    }
}

/// Runs `work`, cancelling `token` once the job's host says the job stops; `work` still ends
/// by itself, so the backend reports what the cancel left.
async fn cancelling<F: Future>(token: &CancellationToken, work: F) -> F::Output {
    let Some(host) = current_host() else {
        return work.await;
    };
    tokio::pin!(work);
    tokio::select! {
        output = &mut work => output,
        () = host.stopped() => {
            token.cancel();
            work.await
        }
    }
}

/// Runs `work` unless `until_stop` is set and the job stops first.
async fn unless_stopped<T>(until_stop: bool, work: impl Future<Output = T>) -> Option<T> {
    let host = current_host().filter(|_| until_stop);
    let Some(host) = host else {
        return Some(work.await);
    };
    tokio::select! {
        output = work => Some(output),
        () = host.stopped() => None,
    }
}

/// Ends the task that watches a job's stop once the stream that needs it is gone.
struct StopWatch(JoinHandle<()>);

impl Drop for StopWatch {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A token cancelled when the job stops, for the stream the returned watch stays with.
fn stop_token(host: Arc<dyn JobHost>) -> (CancellationToken, StopWatch) {
    let token = CancellationToken::new();
    let cancel = token.clone();
    let watch = tokio::spawn(async move {
        host.stopped().await;
        cancel.cancel();
    });
    (token, StopWatch(watch))
}

/// The output bytes until the job stops, which ends them with an error the reading put fails
/// on, so a put in flight cleans up after itself.
fn stoppable_chunks(
    chunks: OutputChunks,
    host: Option<Arc<dyn JobHost>>,
    failure: OutputFailure,
) -> BackendStream<Result<bytes::Bytes, StreamError>> {
    let (stop, watch) = match host.map(stop_token) {
        Some((token, watch)) => (token, Some(watch)),
        None => (CancellationToken::new(), None),
    };
    let start = Some((chunks, stop, watch));
    let body = futures_util::stream::unfold(start, |state| async move {
        let (mut chunks, stop, watch) = state?;
        let next = tokio::select! {
            biased;
            () = stop.cancelled() => Some(Err(BackendError::Cancelled)),
            chunk = chunks.next() => chunk,
        };
        let ended = matches!(next, Some(Err(BackendError::Cancelled)));
        let state = (!ended).then_some((chunks, stop, watch));
        next.map(|chunk| (chunk, state))
    });
    BackendStream(Box::pin(body.map(move |chunk| {
        chunk.map_err(|error| {
            failure.record(error.clone());
            StreamError(Box::new(error))
        })
    })))
}

fn output_body(output: TaskOutput, until_stop: bool) -> OutputBody {
    let failure = OutputFailure::default();
    let host = current_host().filter(|_| until_stop);
    OutputBody {
        size: output.size,
        bytes: stoppable_chunks(output.chunks, host, failure.clone()),
        failure,
    }
}

/// Waits for the stop of a session that a client ended, that went idle, or whose kernel exited;
/// any other end leaves the attempt wait in charge.
async fn session_stopped(registry: &ExecutorRegistry, job: Option<String>) -> EndReason {
    let session = job.and_then(|job| run_session(registry, &job));
    if let Some(session) = session {
        let reason = session.finished().await;
        if matches!(
            reason,
            EndReason::Ended | EndReason::Idle | EndReason::KernelExit
        ) {
            return reason;
        }
    }
    std::future::pending().await
}

fn open_session(
    registry: &ExecutorRegistry,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    open: SessionOpen,
) {
    let config = SessionConfig {
        job_id: open.job_id,
        public_job_id: open.public_job_id,
        runtime: open.runtime,
        workspace_bucket: open.workspace_bucket,
        executor_node_id: open.executor_node_id,
        idle_after_ms: open.idle_after_ms,
        credential_expires_ms: open.credential_expires_ms,
    };
    let session = (registry.sessions()).open(config, backend.clone(), fence.clone());
    if let Some(host) = current_host() {
        host.keep_session(session);
    }
}

async fn attempt_call(
    registry: &ExecutorRegistry,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    request: ComputeRequest,
) -> ComputeAnswer {
    match request {
        ComputeRequest::Fence => ComputeAnswer::Done(backend.fence(fence).await),
        ComputeRequest::Cleanup => ComputeAnswer::Done(backend.cleanup(fence).await),
        ComputeRequest::Tombstone(spec) => {
            ComputeAnswer::Tombstone(backend.tombstone(fence, &spec).await)
        }
        ComputeRequest::Submit(spec) => {
            let token = CancellationToken::new();
            let submitted = cancelling(&token, backend.submit(fence, &spec, &token)).await;
            ComputeAnswer::Submitted(submitted)
        }
        ComputeRequest::Cancel => ComputeAnswer::Cancelled(backend.cancel(fence).await),
        ComputeRequest::Reconcile => ComputeAnswer::Reconciled(backend.reconcile(fence).await),
        ComputeRequest::FetchLogs(limits) => {
            ComputeAnswer::Logs(backend.fetch_logs(fence, &limits).await)
        }
        ComputeRequest::ListOutputs {
            pattern,
            until_stop,
        } => match unless_stopped(until_stop, backend.list_outputs(fence, &pattern)).await {
            Some(matched) => ComputeAnswer::Outputs(matched),
            None => ComputeAnswer::Stopped,
        },
        ComputeRequest::FetchOutput { path, until_stop } => {
            match unless_stopped(until_stop, backend.fetch_output(fence, &path)).await {
                Some(output) => {
                    let body = output.map(|output| output_body(output, until_stop));
                    ComputeAnswer::Output(body)
                }
                None => ComputeAnswer::Stopped,
            }
        }
        ComputeRequest::Await {
            walltime_left,
            session_job,
        } => {
            let token = CancellationToken::new();
            let wait = cancelling(&token, backend.wait(fence, &token));
            ComputeAnswer::Ended(tokio::select! {
                result = wait => AttemptEnd::Attempt(result),
                () = tokio::time::sleep(walltime_left) => AttemptEnd::Walltime,
                reason = session_stopped(registry, session_job) => {
                    AttemptEnd::SessionStopped(reason)
                }
            })
        }
        ComputeRequest::OpenSession(open) => {
            open_session(registry, backend, fence, *open);
            ComputeAnswer::Done(Ok(()))
        }
    }
}

pub(super) async fn dispatch_compute(
    effect: ComputeEffect,
    context: &DriverContext,
) -> ComputeAnswer {
    let Some(registry) = context.compute_handle.as_ref() else {
        return ComputeAnswer::NoCompute;
    };
    let backend_of = |kind| registry.get(kind).cloned();
    match effect {
        ComputeEffect::Select { constraint } => {
            ComputeAnswer::Selected(registry.select(constraint.as_ref()).map(facts))
        }
        ComputeEffect::Facts { kind } => match backend_of(&kind) {
            Some(backend) => ComputeAnswer::Facts(facts(&backend)),
            None => ComputeAnswer::NoBackend,
        },
        ComputeEffect::Fenced {
            kind,
            subject,
            generation,
            digest,
        } => ComputeAnswer::Fenced(
            registry
                .fenced(&kind, &subject, generation, &digest)
                .map(facts),
        ),
        ComputeEffect::Endpoint => {
            let workspace = registry.workspace_endpoint();
            ComputeAnswer::Endpoint(WorkspaceAddress {
                endpoint: workspace.endpoint.clone(),
                session_endpoint: workspace.session_endpoint.clone(),
                region: workspace.region.clone(),
            })
        }
        ComputeEffect::ResolveImage { kind, image } => {
            let Some(backend) = backend_of(&kind) else {
                return ComputeAnswer::NoBackend;
            };
            let token = CancellationToken::new();
            let resolved = cancelling(&token, backend.resolve_image(&image, &token)).await;
            ComputeAnswer::Image(resolved)
        }
        ComputeEffect::Granted { kind, spec } => match backend_of(&kind) {
            Some(backend) => {
                let resources = backend.granted(&spec);
                ComputeAnswer::Granted { spec, resources }
            }
            None => ComputeAnswer::NoBackend,
        },
        ComputeEffect::Attempt {
            kind,
            fence,
            request,
        } => match backend_of(&kind) {
            Some(backend) => attempt_call(registry, &backend, &fence, request).await,
            None => ComputeAnswer::NoBackend,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use std::collections::BTreeSet;

    use aruna_core::jobs::{JobReport, JobStops};
    use futures_util::future::BoxFuture;
    use ulid::Ulid;

    struct TokenHost(CancellationToken);

    impl JobHost for TokenHost {
        fn stops(&self) -> JobStops {
            JobStops::default()
        }

        fn report(&self, _report: JobReport) {}

        fn stopped(&self) -> BoxFuture<'_, ()> {
            Box::pin(self.0.cancelled())
        }

        fn take_turns<'a>(
            &'a self,
            _turns: &'a BTreeSet<(Ulid, Ulid)>,
        ) -> BoxFuture<'a, Box<dyn Any + Send>> {
            Box::pin(std::future::ready(Box::new(()) as Box<dyn Any + Send>))
        }
    }

    // A stop ends a stalled output stream with an error the reading put fails on, instead of
    // the put being dropped halfway, and the failure stays readable.
    #[tokio::test]
    async fn stop_fails_stream() {
        let stalled = futures_util::stream::iter([Ok(bytes::Bytes::from_static(b"ab"))])
            .chain(futures_util::stream::pending());
        let cancel = CancellationToken::new();
        let host: Arc<dyn JobHost> = Arc::new(TokenHost(cancel.clone()));
        let failure = OutputFailure::default();
        let mut chunks = stoppable_chunks(Box::pin(stalled), Some(host), failure.clone());
        assert_eq!(chunks.0.next().await.unwrap().unwrap().as_ref(), b"ab");
        cancel.cancel();
        assert!(chunks.0.next().await.unwrap().is_err());
        assert!(chunks.0.next().await.is_none());
        assert_eq!(failure.take(), Some(BackendError::Cancelled));
    }

    // Without a host every byte passes.
    #[tokio::test]
    async fn hostless_stream_passes() {
        let whole = futures_util::stream::iter([
            Ok(bytes::Bytes::from_static(b"ab")),
            Ok(bytes::Bytes::from_static(b"cd")),
        ]);
        let chunks = stoppable_chunks(Box::pin(whole), None, OutputFailure::default());
        let read: Vec<_> = chunks.0.map(Result::unwrap).collect().await;
        assert_eq!(read.concat(), b"abcd");
    }
}
