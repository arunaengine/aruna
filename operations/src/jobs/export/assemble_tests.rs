//! Assembles export archives through their calls and answers, with real ZIP writes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_blob::archive::{open_zip, zip_close, zip_stream, zip_whole};
use aruna_core::jobs::{ByteStream, JobStops};
use aruna_core::structs::storage::blob::BackendLocation;
use futures_util::StreamExt;
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio_util::sync::CancellationToken;

use super::super::FIXTURE_MOMENT_MS;
use super::super::tests::{export_scope, local_candidate, source_scope};
use super::*;
use crate::jobs::repository::test_support::artifact;

/// An archive entry whose source stream is ready.
pub(in super::super) struct ReadyEntry {
    pub(in super::super) entity_index: usize,
    pub(in super::super) candidate_index: usize,
    pub(in super::super) path: String,
    pub(in super::super) stream: ByteStream,
    pub(in super::super) expected_blake3: [u8; 32],
    pub(in super::super) modified_ms: u64,
}

fn job_event(event: JobEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Job(event)))
}

/// Runs one job effect of the writer the way the job adapter does.
async fn answer_effect(
    effect: JobEffect,
    streams: &mut HashMap<String, ByteStream>,
    cancel: &CancellationToken,
    shutdown: &CancellationToken,
) -> IoResult {
    job_event(match effect {
        JobEffect::ZipWhole {
            zip,
            path,
            modified_ms,
            bytes,
        } => {
            let (zip, result) = zip_whole(zip, &path, modified_ms, &bytes).await;
            JobEvent::ZipWritten { zip, result }
        }
        JobEffect::ZipStream {
            zip,
            path,
            modified_ms,
            stream,
        } => {
            streams.remove(&path);
            let stop = async {
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    _ = shutdown.cancelled() => {}
                }
            };
            let (zip, result) = zip_stream(zip, &path, modified_ms, stream, stop).await;
            JobEvent::ZipStreamed { zip, result }
        }
        JobEffect::ZipClose(zip) => JobEvent::ZipClosed(zip_close(zip).await),
        JobEffect::Stops => JobEvent::Stops(JobStops {
            cancelled: cancel.is_cancelled(),
            shutdown: shutdown.is_cancelled(),
        }),
        other => panic!("unexpected job effect: {other:?}"),
    })
}

/// Writes `entries` into `writer` through the operation, with every source already open.
pub(in super::super) async fn write_archive(
    mut writer: DuplexStream,
    metadata: Vec<u8>,
    entries: Vec<ReadyEntry>,
    report: Option<Vec<u8>>,
    cancel: CancellationToken,
    shutdown: CancellationToken,
) -> Result<(), ExportFailure> {
    let (zip, mut bytes) = open_zip();
    let pump = tokio::spawn(async move {
        while let Some(Ok(chunk)) = bytes.next().await {
            if writer.write_all(&chunk).await.is_err() {
                break;
            }
        }
    });
    let mut streams = HashMap::new();
    let mut planned = Vec::new();
    for entry in entries {
        streams.insert(entry.path.clone(), entry.stream);
        planned.push(PlannedEntry {
            entity_index: entry.entity_index,
            candidate_index: entry.candidate_index,
            path: entry.path,
            candidate: local_candidate(),
            expected_blake3: entry.expected_blake3,
            modified_ms: entry.modified_ms,
        });
    }
    planned.sort_by(|left, right| left.path.cmp(&right.path));
    let parts = ArchiveParts {
        metadata,
        entries: planned,
        report,
        snapshot_ms: FIXTURE_MOMENT_MS,
    };
    let mut write = WriteArchiveOperation::new(source_scope(), zip, parts);
    let mut next = write.first();
    let ended = loop {
        let call = match next {
            Ok(Next::Call(call)) => call,
            Ok(Next::Done(())) => break Ok(()),
            Ok(_) => panic!("the writer only calls or ends"),
            Err(error) => break Err(error),
        };
        let result = match call {
            IoCall::Effect(effect) => match *effect {
                Effect::Job(effect) => {
                    answer_effect(effect, &mut streams, &cancel, &shutdown).await
                }
                other => panic!("unexpected effect: {other:?}"),
            },
            IoCall::Child(_) => {
                let entry = write.current.as_ref().expect("an open entry");
                let blob = streams.remove(&entry.path).expect("a ready stream");
                let opened = CandidateOpen::Opened(BaoReadOutput::Stream {
                    blob,
                    size: 0,
                    blake3: entry.expected_blake3,
                    etag: None,
                    hashes: HashMap::new(),
                });
                IoResult::Child(ChildOutcome::new(Ok::<_, ExportFailure>(opened)))
            }
            other => panic!("unexpected call: {other:?}"),
        };
        next = write.next(result);
    };
    drop(write);
    pump.await.expect("the archive pump ended");
    ended
}

fn hidden() -> BackendLocation {
    let mut location = artifact().location;
    location.backend_path = "_jobs/01ARZ3NDEKTSV4RRFFQ69G5FAV/rocrate.zip".to_string();
    location
}

fn assembling() -> AssembleOperation {
    AssembleOperation {
        run: export_scope(),
        parts: None,
        now_ms: 100,
        step: AssembleStep::Group,
    }
}

fn spooled(location: BackendLocation) -> GroupResult {
    GroupResult::Done(Box::new(Event::Blob(BlobEvent::HiddenSpooled {
        location,
        blake3: [1; 32],
        size: 4,
    })))
}

fn spool_error(error: BlobError) -> GroupResult {
    GroupResult::Done(Box::new(Event::Blob(BlobEvent::Error(error))))
}

// A checkpoint without the rewritten crate cannot be assembled.
#[test]
fn parts_need_metadata() {
    let failed = archive_parts(&ExportCheckpoint::default(), Vec::new());
    assert_eq!(
        failed,
        Err(ExportFailure::Permanent(
            "rewritten metadata is missing".to_string()
        ))
    );
}

// The artifact expires after the retention of the spec, counted from now.
#[test]
fn artifact_expires() {
    let mut assemble = assembling();
    let retention = assemble.run.spec.limits.artifact_retention_ms;
    let Ok(Next::Done(artifact)) = assemble.finished(spooled(hidden()), Ok(())) else {
        panic!("expected the artifact");
    };
    assert_eq!(artifact.expires_at_ms, 100 + retention);
    assert_eq!((artifact.size, artifact.blake3), (4, [1; 32]));
}

// A spooled blob of a failed write is deleted, and the write's failure ends the step.
#[test]
fn failed_write_discards() {
    let mut assemble = assembling();
    let failed = Err(ExportFailure::Retryable("broken".to_string()));
    let turn = assemble.finished(spooled(hidden()), failed);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
    let deleted = IoResult::Event(Box::new(Event::Blob(BlobEvent::HiddenDeleted)));
    assert_eq!(
        assemble.next(deleted),
        Err(ExportFailure::Retryable("broken".to_string()))
    );
}

// An archive above the artifact limit fails for good.
#[test]
fn oversized_fails() {
    let error = BlobError::SizeLimitExceeded { limit: 8 };
    assert_eq!(
        assembling().finished(spool_error(error), Ok(())),
        Err(ExportFailure::Permanent(
            "assembled ZIP exceeds the 8 byte artifact limit".to_string()
        ))
    );
}

// When the spool broke because the job stopped, the stop is the outcome.
#[test]
fn stop_beats_spool() {
    let stopped = Err(ExportFailure::Cancelled);
    assert_eq!(
        assembling().finished(spool_error(BlobError::ChannelClosed), stopped),
        Err(ExportFailure::Cancelled)
    );
}

// A failed stream is corrupt or offline, which decides how the next probe treats it.
#[test]
fn stream_status_maps() {
    assert_eq!(stream_status(true), OpenStatus::Corrupt);
    assert_eq!(stream_status(false), OpenStatus::Offline);
}
