//! Reads a staging job's checksum manifests in bounded chunks and looks digests up per page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::STAGING_MANIFEST_KEYSPACE;
use aruna_core::structs::checksum::{ChecksumAlgorithm, parse_manifest_line};
use aruna_core::structs::execution::job::{
    JobId, ManifestRead, StagingJobCheckpoint, StagingJobSpec,
};
use aruna_core::structs::execution::source_access::ObservationMatch;
use aruna_core::types::Key;
use byteview::ByteView;
use futures_util::TryStreamExt;

use super::executor::JobContext;
use super::staging_walk::{PageFailure, canonical_walker_path, commit_rows, record_failure};
use crate::driver::drive;
use crate::staging::head_source::{HeadSourceInput, HeadSourceOperation};
use crate::staging::read_source::{ReadSourceInput, ReadSourceOperation};

/// Manifest bytes one chunk reads; a manifest line must fit into one chunk.
const MANIFEST_CHUNK: u64 = 256 * 1024;

pub(super) type ManifestDigest = (ChecksumAlgorithm, Vec<u8>);

/// Reads the next chunk of the manifest being ingested and stores its digests with the
/// checkpoint. Later chunks are read bound to the manifest's first observation.
pub(super) async fn read_manifest_chunk(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    checkpoint: &mut StagingJobCheckpoint,
) -> Result<(), PageFailure> {
    let Some(manifest) = checkpoint.manifest.clone() else {
        return Ok(());
    };
    let mut next = checkpoint.clone();
    next.manifest = None;
    let observation = match manifest.observation.clone() {
        Some(observation) => observation,
        None => match drive(
            HeadSourceOperation::new(HeadSourceInput {
                group_id: spec.group_id,
                connector_id: spec.connector_id,
                source_path: manifest.source_path.clone(),
            }),
            &ctx.driver,
        )
        .await
        {
            Ok(head) => head.metadata,
            Err(error) => {
                let message = format!("checksum manifest unavailable: {error}");
                record_failure(&mut next, &manifest.source_path, "", message);
                return commit_rows(ctx, checkpoint, next, Vec::new(), Vec::new()).await;
            }
        },
    };
    let end = manifest
        .offset
        .saturating_add(MANIFEST_CHUNK)
        .min(observation.content_length);
    if manifest.offset >= end {
        return commit_rows(ctx, checkpoint, next, Vec::new(), Vec::new()).await;
    }
    let read = drive(
        ReadSourceOperation::new(ReadSourceInput {
            group_id: spec.group_id,
            connector_id: spec.connector_id,
            source_path: manifest.source_path.clone(),
            range: Some(manifest.offset..end),
            expected: Some(observation.clone()),
        }),
        &ctx.driver,
    )
    .await;
    let bytes = match read {
        Ok(read) if read.metadata.compare(&observation) == ObservationMatch::Changed => {
            Err("the manifest changed while it was read".to_string())
        }
        Ok(read) => read
            .stream
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    };
    let bytes = match bytes {
        Ok(bytes) => bytes,
        Err(error) => {
            let message = format!("checksum manifest read failed: {error}");
            record_failure(&mut next, &manifest.source_path, "", message);
            return commit_rows(ctx, checkpoint, next, Vec::new(), Vec::new()).await;
        }
    };
    let complete = end == observation.content_length;
    let used = match (complete, bytes.iter().rposition(|byte| *byte == b'\n')) {
        (true, _) => bytes.len(),
        (false, Some(newline)) => newline + 1,
        (false, None) => {
            let message = format!("a checksum manifest line exceeds {MANIFEST_CHUNK} bytes");
            record_failure(&mut next, &manifest.source_path, "", message);
            return commit_rows(ctx, checkpoint, next, Vec::new(), Vec::new()).await;
        }
    };
    let directory = manifest
        .source_path
        .rsplit_once('/')
        .map_or("", |(directory, _)| directory);
    let mut writes = Vec::new();
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
        writes.push((
            STAGING_MANIFEST_KEYSPACE.to_string(),
            manifest_key(ctx.job_id, &source_path),
            ByteView::from(value),
        ));
    }
    let offset = manifest.offset + used as u64;
    if offset < observation.content_length {
        next.manifest = Some(ManifestRead {
            offset,
            observation: Some(observation),
            ..manifest
        });
    }
    commit_rows(ctx, checkpoint, next, writes, Vec::new()).await
}

/// The manifest digests of a page's files, by one batched point read.
pub(super) async fn manifest_digests(
    ctx: &JobContext,
    paths: &[&str],
) -> Result<Vec<Option<ManifestDigest>>, String> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let reads = paths
        .iter()
        .map(|path| {
            (
                STAGING_MANIFEST_KEYSPACE.to_string(),
                manifest_key(ctx.job_id, path),
            )
        })
        .collect();
    let event = ctx
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await;
    let values = match event {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => return Err(format!("unexpected manifest read event: {other:?}")),
    };
    values
        .into_iter()
        .map(|(_, value)| {
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
