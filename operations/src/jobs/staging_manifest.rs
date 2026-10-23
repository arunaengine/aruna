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
use super::staging_walk::{
    PageFailure, canonical_walker_path, commit_rows, record_failure, source_permission_path,
};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::drive;
use crate::staging::head_source::{HeadSourceError, HeadSourceInput, HeadSourceOperation};
use crate::staging::read_source::{ReadSourceError, ReadSourceInput, ReadSourceOperation};
use aruna_core::errors::StagingSourceError;
use aruna_core::structs::identity::auth::Permission;

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
    // Every chunk, a resumed one too, reads only what the submitter may read at this exact path.
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: spec.auth_context.clone(),
            path: source_permission_path(spec, &manifest.source_path),
            required_permission: Permission::READ,
        }),
        &ctx.driver,
    )
    .await
    .map_err(|error| PageFailure::Retry(error.to_string()))?;
    if !allowed {
        let message = "checksum manifest read denied".to_string();
        record_failure(&mut next, &manifest.source_path, "", message);
        return commit_rows(ctx, checkpoint, next, Vec::new(), Vec::new()).await;
    }
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
            Err(error) if lasting_head(&error) => {
                let message = format!("checksum manifest unavailable: {error}");
                record_failure(&mut next, &manifest.source_path, "", message);
                return commit_rows(ctx, checkpoint, next, Vec::new(), Vec::new()).await;
            }
            // A transient failure keeps the manifest and its offset for the job's retry.
            Err(error) => return Err(PageFailure::Retry(error.to_string())),
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
    let read = match read {
        Ok(read) if read.metadata.compare(&observation) != ObservationMatch::Changed => read,
        Ok(_) | Err(ReadSourceError::Staging(StagingSourceError::ConditionFailed)) => {
            let message = "the checksum manifest changed while it was read".to_string();
            record_failure(&mut next, &manifest.source_path, "", message);
            return commit_rows(ctx, checkpoint, next, Vec::new(), Vec::new()).await;
        }
        Err(error) if lasting_read(&error) => {
            let message = format!("checksum manifest read failed: {error}");
            record_failure(&mut next, &manifest.source_path, "", message);
            return commit_rows(ctx, checkpoint, next, Vec::new(), Vec::new()).await;
        }
        Err(error) => return Err(PageFailure::Retry(error.to_string())),
    };
    let bytes = read
        .stream
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
        .map_err(|error| PageFailure::Retry(error.to_string()))?;
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

/// Errors that stay on a retry: the manifest is then skipped instead of retried.
fn lasting_head(error: &HeadSourceError) -> bool {
    match error {
        HeadSourceError::Resolve(_) => true,
        HeadSourceError::Staging(error) => lasting_source(error),
        HeadSourceError::UnexpectedEvent { .. } | HeadSourceError::HeadSourceFailed => false,
    }
}

fn lasting_read(error: &ReadSourceError) -> bool {
    match error {
        ReadSourceError::Resolve(_) => true,
        ReadSourceError::Staging(error) => lasting_source(error),
        ReadSourceError::UnexpectedEvent { .. } | ReadSourceError::ReadSourceFailed => false,
    }
}

fn lasting_source(error: &StagingSourceError) -> bool {
    matches!(
        error,
        StagingSourceError::NotFound
            | StagingSourceError::AccessDenied
            | StagingSourceError::UnsupportedKind(_)
            | StagingSourceError::EgressDenied(_)
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::DriverContext;
    use crate::jobs::executor::ProgressReporter;
    use aruna_core::structs::execution::job::{
        JobClaim, JobPayload, JobProgress, JobRecord, StagingJobPhase, StagingJobPrefix,
    };
    use aruna_core::structs::execution::staging::StagingStrategy;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;
    use ulid::Ulid;

    fn spec() -> StagingJobSpec {
        let realm_id = RealmId::from_bytes([1; 32]);
        StagingJobSpec {
            auth_context: AuthContext {
                user_id: aruna_core::UserId::local(Ulid::from_bytes([2; 16]), realm_id),
                realm_id,
                path_restrictions: None,
                session: None,
            },
            group_id: Ulid::from_bytes([3; 16]),
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            connector_id: Ulid::from_bytes([5; 16]),
            bucket: "bucket".to_string(),
            bucket_identity: (
                Ulid::nil(),
                std::time::SystemTime::UNIX_EPOCH,
                Default::default(),
            ),
            strategy: StagingStrategy::Reference,
            items: Vec::new(),
            prefixes: vec![StagingJobPrefix {
                source_prefix: "data".to_string(),
                target_prefix: String::new(),
            }],
            checksum_manifest: Some("MD5SUMS".to_string()),
        }
    }

    fn checkpoint() -> StagingJobCheckpoint {
        StagingJobCheckpoint {
            phase: StagingJobPhase::Discovering,
            flat: Some(true),
            items_done: 0,
            prefixes_done: 1,
            walk: None,
            manifest: Some(ManifestRead {
                source_path: "data/MD5SUMS".to_string(),
                offset: 4096,
                observation: None,
            }),
            queue_head: 0,
            queue_tail: 0,
            items_current: 0,
            items_succeeded: 0,
            items_failed: 0,
            items_total: None,
            bytes_current: 0,
            bytes_total: None,
            bytes_discovered: 0,
            current_path: None,
            errors: Vec::new(),
        }
    }

    // Transient failures keep the manifest for a retry; only lasting ones skip it.
    #[test]
    fn transient_errors_retry() {
        let transient = || {
            [
                StagingSourceError::ReadError("reset".to_string()),
                StagingSourceError::HandleMissing,
                StagingSourceError::StatError("timeout".to_string()),
            ]
        };
        assert!(transient().into_iter().all(|error| !lasting_source(&error)));
        for error in transient() {
            assert!(!lasting_read(&ReadSourceError::Staging(error)));
        }
        for error in transient() {
            assert!(!lasting_head(&HeadSourceError::Staging(error)));
        }
        assert!(lasting_read(&ReadSourceError::Staging(
            StagingSourceError::NotFound
        )));
        assert!(lasting_head(&HeadSourceError::Staging(
            StagingSourceError::AccessDenied
        )));
    }

    // A resumed manifest read is authorized again on its own path before anything is read.
    #[tokio::test]
    async fn manifest_read_authorized() {
        let tempdir = tempfile::tempdir().unwrap();
        let storage_handle =
            aruna_storage::storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let node_id = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let claim_token = Ulid::from_bytes([6; 16]);
        let job_id = JobId::from_bytes([7; 16]);
        let mut record = JobRecord::new(
            job_id,
            JobPayload::Staging(spec()),
            Default::default(),
            node_id,
            1,
            1,
            None,
        );
        record.claim = Some(JobClaim {
            holder_node_id: node_id,
            claim_token,
            lease_expires_ms: u64::MAX,
        });
        crate::jobs::store::insert_job(&storage_handle, &record)
            .await
            .unwrap();
        let ctx = JobContext {
            driver: Arc::new(DriverContext {
                storage_handle,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            }),
            job_id,
            owner_node_id: node_id,
            claim_token,
            final_attempt: false,
            cancel: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            progress: ProgressReporter::from_progress(&JobProgress::new("items")),
        };
        let mut checkpoint = checkpoint();

        assert!(
            read_manifest_chunk(&ctx, &spec(), &mut checkpoint)
                .await
                .is_ok()
        );

        assert_eq!(checkpoint.manifest, None);
        assert_eq!(checkpoint.items_failed, 1);
        assert_eq!(checkpoint.errors[0].error, "checksum manifest read denied");
    }
}
