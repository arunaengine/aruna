//! Reads a staging job's checksum manifests in bounded chunks and looks digests up per page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::{AuthorizationError, SourceResolutionError, StagingSourceError};
use aruna_core::events::Event;
use aruna_core::jobs::{JobEffect, JobEvent};
use aruna_core::keyspaces::STAGING_MANIFEST_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::checksum::{ChecksumAlgorithm, parse_manifest_line};
use aruna_core::structs::execution::job::{JobId, ManifestRead, StagingJobCheckpoint};
use aruna_core::structs::execution::source_access::{ObservationMatch, SourceMetadata};
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;

use super::walk::{canonical_walker_path, read_check_call, record_failure, source_permission_path};
use super::{ClearRowsOperation, PageFailure, StagingRun, committed};
use crate::staging::head_source::{
    HeadSourceError, HeadSourceInput, HeadSourceOperation, HeadSourceResult,
};
use crate::staging::read_source::{
    ReadSourceError, ReadSourceInput, ReadSourceOperation, ReadSourceResult,
};

/// Manifest bytes one chunk reads; a manifest line must fit into one chunk.
const MANIFEST_CHUNK: u64 = 256 * 1024;

pub(super) type ManifestDigest = (ChecksumAlgorithm, Vec<u8>);

#[derive(Debug, PartialEq)]
enum ChunkStep {
    Allowed,
    Head,
    Read,
    Collect,
    Clear,
    Commit,
}

/// Reads the next chunk of the manifest being ingested and stores its digests with the
/// checkpoint. Later chunks are read bound to the manifest's first observation.
#[derive(Debug, PartialEq)]
pub(super) struct ManifestChunkOperation {
    run: StagingRun,
    manifest: Option<ManifestRead>,
    next: StagingJobCheckpoint,
    observation: Option<SourceMetadata>,
    end: u64,
    writes: Vec<(KeySpace, Key, Value)>,
    step: ChunkStep,
}

impl ManifestChunkOperation {
    pub(super) fn new(run: StagingRun, checkpoint: &StagingJobCheckpoint) -> Self {
        let mut next = checkpoint.clone();
        let manifest = next.manifest.take();
        Self {
            run,
            manifest,
            next,
            observation: None,
            end: 0,
            writes: Vec::new(),
            step: ChunkStep::Allowed,
        }
    }

    fn manifest(&self) -> Result<&ManifestRead, PageFailure> {
        (self.manifest.as_ref()).ok_or_else(|| PageFailure::Retry("no open manifest".to_string()))
    }

    fn commit(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        self.step = ChunkStep::Commit;
        let writes = std::mem::take(&mut self.writes);
        let call = (self.run)
            .commit_call(None, &self.next, writes, Vec::new())
            .map_err(PageFailure::Retry)?;
        Ok(Next::Call(call))
    }

    /// Skips a manifest that cannot be read to its end. Digests its earlier chunks stored are
    /// deleted first, so no file of the prefix is bound to part of a manifest.
    fn fail(&mut self, message: String) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        let manifest = self.manifest()?.clone();
        record_failure(&mut self.next, &manifest.source_path, "", message);
        if manifest.offset == 0 {
            return self.commit();
        }
        let prefix = match manifest_directory(&manifest) {
            "" => manifest_key(self.run.job_id, ""),
            directory => manifest_key(self.run.job_id, &format!("{directory}/")),
        };
        self.step = ChunkStep::Clear;
        let clear = ClearRowsOperation::new(STAGING_MANIFEST_KEYSPACE, prefix);
        Ok(Next::Call(IoCall::child(clear)))
    }

    fn read(
        &mut self,
        observation: SourceMetadata,
    ) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        let manifest = self.manifest()?.clone();
        self.end = manifest
            .offset
            .saturating_add(MANIFEST_CHUNK)
            .min(observation.content_length);
        if manifest.offset >= self.end {
            return self.commit();
        }
        let spec = &self.run.spec;
        let read = ReadSourceOperation::new(ReadSourceInput {
            group_id: spec.group_id,
            connector_id: spec.connector_id,
            source_path: manifest.source_path.clone(),
            range: Some(manifest.offset..self.end),
            expected: Some(observation.clone()),
        });
        self.observation = Some(observation);
        self.step = ChunkStep::Read;
        Ok(Next::Call(IoCall::child(read)))
    }

    fn collected(&mut self, bytes: Vec<u8>) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        let manifest = self.manifest()?.clone();
        let observation = (self.observation.take())
            .ok_or_else(|| PageFailure::Retry("the manifest has no observation".to_string()))?;
        let complete = self.end == observation.content_length;
        let used = match (complete, bytes.iter().rposition(|byte| *byte == b'\n')) {
            (true, _) => bytes.len(),
            (false, Some(newline)) => newline + 1,
            (false, None) => {
                return self.fail(format!(
                    "a checksum manifest line exceeds {MANIFEST_CHUNK} bytes"
                ));
            }
        };
        let directory = manifest_directory(&manifest);
        for line in String::from_utf8_lossy(&bytes[..used]).lines() {
            let Some((algorithm, digest, path)) = parse_manifest_line(line) else {
                continue;
            };
            let joined = match directory {
                "" => path.to_string(),
                directory => format!("{directory}/{path}"),
            };
            let Ok(source_path) = canonical_walker_path(&joined) else {
                continue;
            };
            let value = postcard::to_allocvec(&(algorithm, digest))
                .map_err(|error| PageFailure::Permanent(error.to_string()))?;
            self.writes.push((
                STAGING_MANIFEST_KEYSPACE.to_string(),
                manifest_key(self.run.job_id, &source_path),
                ByteView::from(value),
            ));
        }
        let offset = manifest.offset + used as u64;
        if offset < observation.content_length {
            self.next.manifest = Some(ManifestRead {
                offset,
                observation: Some(observation),
                ..manifest
            });
        }
        self.commit()
    }
}

impl Operation for ManifestChunkOperation {
    type Output = StagingJobCheckpoint;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        // Every chunk, a resumed one too, reads only what the submitter may read at this path.
        let path = source_permission_path(&self.run.spec, &self.manifest()?.source_path);
        Ok(Next::Call(read_check_call(&self.run.spec, path)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        match self.step {
            ChunkStep::Allowed => {
                let allowed = (result.child::<bool, AuthorizationError>()?)
                    .map_err(|error| PageFailure::Retry(error.to_string()))?;
                if !allowed {
                    return self.fail("checksum manifest read denied".to_string());
                }
                let manifest = self.manifest()?.clone();
                match manifest.observation {
                    Some(observation) => self.read(observation),
                    None => {
                        self.step = ChunkStep::Head;
                        let spec = &self.run.spec;
                        let head = HeadSourceOperation::new(HeadSourceInput {
                            group_id: spec.group_id,
                            connector_id: spec.connector_id,
                            source_path: manifest.source_path,
                        });
                        Ok(Next::Call(IoCall::child(head)))
                    }
                }
            }
            ChunkStep::Head => match result.child::<HeadSourceResult, HeadSourceError>()? {
                Ok(head) => self.read(head.metadata),
                Err(error) if lasting_head(&error) => {
                    let source = self.manifest()?.source_path.clone();
                    let message = format!("checksum manifest unavailable: {error}");
                    record_failure(&mut self.next, &source, "", message);
                    self.commit()
                }
                // A transient failure keeps the manifest and its offset for the job's retry.
                Err(error) => Err(PageFailure::Retry(error.to_string())),
            },
            ChunkStep::Read => {
                let unchanged = |read: &ReadSourceResult, seen: &SourceMetadata| {
                    read.metadata.compare(seen) != ObservationMatch::Changed
                };
                let seen = (self.observation.as_ref())
                    .ok_or_else(|| PageFailure::Retry("no observation".to_string()))?;
                match result.child::<ReadSourceResult, ReadSourceError>()? {
                    Ok(read) if unchanged(&read, seen) => {
                        self.step = ChunkStep::Collect;
                        let collect = Effect::Job(JobEffect::Collect(read.stream));
                        Ok(Next::Call(IoCall::Effect(Box::new(collect))))
                    }
                    Ok(_) | Err(ReadSourceError::Staging(StagingSourceError::ConditionFailed)) => {
                        self.fail("the checksum manifest changed while it was read".to_string())
                    }
                    Err(error) if lasting_read(&error) => {
                        self.fail(format!("checksum manifest read failed: {error}"))
                    }
                    Err(error) => Err(PageFailure::Retry(error.to_string())),
                }
            }
            ChunkStep::Collect => {
                let event = result.event()?;
                let Event::Job(JobEvent::Collected(bytes)) = event else {
                    return Err(IoError::Unexpected {
                        expected: "the collected manifest bytes",
                        got: event.label(),
                    }
                    .into());
                };
                self.collected(bytes.map_err(PageFailure::Retry)?)
            }
            ChunkStep::Clear => {
                (result.child::<(), IoError>()?)
                    .map_err(|error| PageFailure::Retry(error.to_string()))?;
                self.commit()
            }
            ChunkStep::Commit => {
                committed(result).map_err(PageFailure::Retry)?;
                Ok(Next::Done(self.next.clone()))
            }
        }
    }
}

fn manifest_directory(manifest: &ManifestRead) -> &str {
    (manifest.source_path.rsplit_once('/')).map_or("", |(directory, _)| directory)
}

/// Errors that stay on a retry: the manifest is then skipped instead of retried.
pub(super) fn lasting_head(error: &HeadSourceError) -> bool {
    match error {
        HeadSourceError::Resolve(error) => lasting_resolution(error),
        HeadSourceError::Staging(error) => lasting_source(error),
        HeadSourceError::Io(_) | HeadSourceError::UnexpectedEvent(_) => false,
    }
}

pub(super) fn lasting_read(error: &ReadSourceError) -> bool {
    match error {
        ReadSourceError::Resolve(error) => lasting_resolution(error),
        ReadSourceError::Staging(error) => lasting_source(error),
        ReadSourceError::Io(_) | ReadSourceError::UnexpectedEvent(_) => false,
    }
}

/// A missing or unusable connector stays; storage failures such as a timeout pass.
pub(super) fn lasting_resolution(error: &SourceResolutionError) -> bool {
    match error {
        SourceResolutionError::StorageError(_) | SourceResolutionError::ResolveFailed => false,
        SourceResolutionError::ConversionError(_)
        | SourceResolutionError::NotFound
        | SourceResolutionError::UnsupportedConnectorKind(_)
        | SourceResolutionError::InvalidSourcePath => true,
    }
}

pub(super) fn lasting_source(error: &StagingSourceError) -> bool {
    matches!(
        error,
        StagingSourceError::NotFound
            | StagingSourceError::AccessDenied
            | StagingSourceError::UnsupportedKind(_)
            | StagingSourceError::EgressDenied(_)
    )
}

/// Reads the manifest digests of a page's files in one batch.
pub(super) fn digest_reads(job_id: JobId, paths: &[&str]) -> IoCall {
    let reads = (paths.iter())
        .map(|path| {
            (
                STAGING_MANIFEST_KEYSPACE.to_string(),
                manifest_key(job_id, path),
            )
        })
        .collect();
    IoCall::Read(reads)
}

/// The answer to [`digest_reads`], one digest per path.
pub(super) fn parse_digests(result: IoResult) -> Result<Vec<Option<ManifestDigest>>, String> {
    (result
        .values()
        .map_err(|error| error.to_string())?
        .into_iter())
    .map(|value| {
        value
            .map(|value| postcard::from_bytes(value.as_ref()))
            .transpose()
            .map_err(|error| error.to_string())
    })
    .collect()
}

fn manifest_key(job_id: JobId, source_path: &str) -> Key {
    let mut key = job_id.to_bytes().to_vec();
    key.extend_from_slice(source_path.as_bytes());
    ByteView::from(key)
}

#[cfg(test)]
#[path = "manifest_tests.rs"]
mod tests;
