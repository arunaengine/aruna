//! Answers the effects of a running job through the host its runtime installed for this task.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::any::Any;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::jobs::{
    ByteStream, GzipBroken, GzipWork, JobEffect, JobEvent, JobReport, JobStops, ObjectBody,
    ObjectRead, ObjectReadFailure,
};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use ulid::Ulid;

use super::dispatch_effect_until;
use crate::driver::DriverContext;

tokio::task_local! {
    static JOB_HOST: Arc<dyn JobHost>;
}

/// The runtime side of one running job: its stop signals, progress, turns and object reads.
pub trait JobHost: Send + Sync {
    fn stops(&self) -> JobStops;
    fn report(&self, report: JobReport);
    /// Completes once the job is cancelled or its node shuts down.
    fn stopped(&self) -> BoxFuture<'_, ()>;
    /// Takes the turns of `turns`; they end when the returned value drops.
    fn take_turns<'a>(
        &'a self,
        turns: &'a BTreeSet<(Ulid, Ulid)>,
    ) -> BoxFuture<'a, Box<dyn Any + Send>>;
    fn read_object(&self, read: ObjectRead)
    -> BoxFuture<'_, Result<ObjectBody, ObjectReadFailure>>;
}

/// Runs `future` with `host` answering the job effects of every operation it drives.
pub async fn with_job_host<F: Future>(host: Arc<dyn JobHost>, future: F) -> F::Output {
    JOB_HOST.scope(host, future).await
}

fn current_host() -> Option<Arc<dyn JobHost>> {
    JOB_HOST.try_with(Arc::clone).ok()
}

pub(super) async fn dispatch_job(
    effect: JobEffect,
    context: &DriverContext,
    depth: usize,
    deadline: Option<tokio::time::Instant>,
) -> Event {
    let host = current_host();
    let run = |child| dispatch_effect_until(Effect::SubOperation(child), context, depth, deadline);
    Event::Job(match effect {
        JobEffect::Stops => JobEvent::Stops(host.map_or_else(JobStops::default, |it| it.stops())),
        JobEffect::Report(report) => {
            if let Some(host) = host {
                host.report(report);
            }
            JobEvent::Reported
        }
        JobEffect::Pause(after) => {
            let sleep = tokio::time::sleep(after);
            match host {
                Some(host) => tokio::select! {
                    _ = host.stopped() => JobEvent::Paused { stopped: true },
                    _ = sleep => JobEvent::Paused { stopped: false },
                },
                None => {
                    sleep.await;
                    JobEvent::Paused { stopped: false }
                }
            }
        }
        JobEffect::InTurns { turns, child } => {
            let _held = match &host {
                Some(host) => Some(host.take_turns(&turns).await),
                None => None,
            };
            JobEvent::Finished(Box::new(run(child).await))
        }
        JobEffect::UntilStop(child) => match host {
            Some(host) => tokio::select! {
                event = run(child) => JobEvent::Finished(Box::new(event)),
                _ = host.stopped() => JobEvent::Stopped,
            },
            None => JobEvent::Finished(Box::new(run(child).await)),
        },
        JobEffect::Collect(stream) => JobEvent::Collected(collect(stream).await),
        JobEffect::FillGzip(work) => fill_gzip(work).await,
        JobEffect::ReadObject(read) => JobEvent::Object(match host {
            Some(host) => host.read_object(*read).await,
            None => Err(ObjectReadFailure::Failed("no job host".to_string())),
        }),
    })
}

async fn collect(mut stream: ByteStream) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.map_err(|error| error.to_string())?);
    }
    Ok(bytes)
}

/// Pulls compressed bytes until the cursor holds the next chunk or the table's end.
async fn fill_gzip(mut work: GzipWork) -> JobEvent {
    let result = loop {
        match work.cursor.fill() {
            Ok(true) => break Ok(()),
            Ok(false) => {}
            Err(error) => break Err(GzipBroken::Gzip(error.to_string())),
        }
        match work.stream.next().await {
            Some(Ok(bytes)) => work.cursor.push(&bytes),
            Some(Err(error)) => break Err(GzipBroken::Stream(error.to_string())),
            None => work.cursor.finish(),
        }
    };
    JobEvent::GzipFilled { work, result }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::stream::{BackendStream, StreamError};
    use aruna_core::structs::execution::table::GzipCursor;
    use bytes::Bytes;

    fn stream(pieces: Vec<Result<Bytes, StreamError>>) -> ByteStream {
        BackendStream::new(futures_util::stream::iter(pieces))
    }

    // A stream is read to its end, and a transport failure comes back as text.
    #[tokio::test]
    async fn collect_reads_stream() {
        let bytes = stream(vec![
            Ok(Bytes::from_static(b"ab")),
            Ok(Bytes::from_static(b"c")),
        ]);
        assert_eq!(collect(bytes).await, Ok(b"abc".to_vec()));
    }

    // Bytes that are no gzip are told apart from a stream that broke.
    #[tokio::test]
    async fn gzip_fill_reports() {
        let work = GzipWork {
            stream: stream(vec![Ok(Bytes::from(vec![0xFF; 1024]))]),
            cursor: GzipCursor::new(0, 16),
        };
        let JobEvent::GzipFilled { result, .. } = fill_gzip(work).await else {
            panic!("expected the filled chunk");
        };
        assert!(matches!(result, Err(GzipBroken::Gzip(_))));
    }

    // Without a host a job is never stopped and a pause simply passes.
    #[tokio::test]
    async fn hostless_effects_answer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let storage = aruna_storage::storage::FjallStorage::open(path).unwrap();
        let context = crate::testing::test_context(storage);
        let event = dispatch_job(JobEffect::Stops, &context, 0, None).await;
        assert_eq!(event, Event::Job(JobEvent::Stops(JobStops::default())));
        let pause = JobEffect::Pause(std::time::Duration::ZERO);
        let event = dispatch_job(pause, &context, 0, None).await;
        assert_eq!(event, Event::Job(JobEvent::Paused { stopped: false }));
    }
}
