//! Runs the staging job: walks source listings page by page with a provider cursor and records
//! each page together with a small checkpoint.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod item;
mod job;
mod lanes;
mod limiter;
mod manifest;
mod mirrors;
mod page;
mod table;
mod walk;

use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{
    STAGING_MANIFEST_KEYSPACE, STAGING_QUEUE_KEYSPACE, STAGING_STATE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{done, values};
use aruna_core::structs::execution::job::{
    JobId, ManifestRead, StagingJobCheckpoint, StagingJobPhase, StagingJobSpec,
};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::routing::NodeRouting;
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

pub use self::job::StagingJobOperation;
use super::run_rows::SettledRowsOperation;
use super::store::mutation_error::JobMutationError;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// Queue rows one cleanup pass deletes.
const QUEUE_CLEANUP_PAGE: usize = 256;

/// What every operation of one staging run knows about it.
#[derive(Clone, Debug, PartialEq)]
pub struct StagingRun {
    pub spec: Arc<StagingJobSpec>,
    pub job_id: JobId,
    pub claim_token: Ulid,
    pub routing: NodeRouting,
    /// Spreads retry waits and makes the version ids this run mints distinct.
    pub seed: u64,
}

impl StagingRun {
    /// A version id for item `index` of this run, minted at `now_ms`.
    fn fresh_id(&self, now_ms: u64, index: u64) -> Ulid {
        let random = (u128::from(self.seed) << 32) | u128::from(index);
        Ulid::from_parts(now_ms, random)
    }

    fn bucket_call(&self) -> IoCall {
        IoCall::child(GetBucketOperation::new(self.spec.bucket.clone()))
    }

    /// The write of `checkpoint`, of the job or of lane `slot`, with the given job rows.
    fn commit_call(
        &self,
        slot: Option<u64>,
        checkpoint: &StagingJobCheckpoint,
        mut writes: Vec<(KeySpace, Key, Value)>,
        deletes: Vec<(KeySpace, Key)>,
    ) -> Result<IoCall, String> {
        let value = postcard::to_allocvec(checkpoint).map_err(|error| error.to_string())?;
        writes.push((
            STAGING_STATE_KEYSPACE.to_string(),
            checkpoint_key(self.job_id, slot),
            ByteView::from(value),
        ));
        Ok(IoCall::child(SettledRowsOperation::new(
            self.job_id,
            self.claim_token,
            writes,
            deletes,
        )))
    }
}

/// The answer to [`StagingRun::commit_call`].
fn committed(result: IoResult) -> Result<(), String> {
    let written = result.child::<(), JobMutationError>();
    written
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())
}

/// The live destination bucket, only while it is still the one the job was submitted for.
fn pinned(spec: &StagingJobSpec, result: IoResult) -> Result<BucketInfo, ItemFailure> {
    let found = result.child::<BucketInfo, GetBucketError>()?;
    let bucket_info = match found {
        Ok(bucket_info) => bucket_info,
        Err(GetBucketError::NotFound) => {
            return Err(ItemFailure::Stage(
                "destination bucket no longer exists".to_string(),
            ));
        }
        Err(error) => return Err(ItemFailure::System(error.to_string())),
    };
    if bucket_info.identity() != spec.bucket_identity {
        return Err(ItemFailure::Lasting(
            "destination bucket was replaced after job submission".to_string(),
        ));
    }
    Ok(bucket_info)
}

/// Reads a staging store row or page and decodes it; a failure carries its message.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct StagingStoreError(pub String);

impl From<IoError> for StagingStoreError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

#[derive(Debug, PartialEq)]
enum ReadStep {
    Main,
    Lanes(Box<StagingJobCheckpoint>),
}

/// The job's progress with its open prefix lanes counted in; a lane's prefix reads as not
/// started yet, as a job handed over goes on from there.
#[derive(Debug, PartialEq)]
pub struct ReadCheckpointOperation {
    job_id: JobId,
    step: ReadStep,
}

impl ReadCheckpointOperation {
    pub fn new(job_id: JobId) -> Self {
        Self {
            job_id,
            step: ReadStep::Main,
        }
    }
}

impl Operation for ReadCheckpointOperation {
    type Output = Option<StagingJobCheckpoint>;
    type Error = StagingStoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, StagingStoreError> {
        Ok(Next::Call(main_read(self.job_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, StagingStoreError> {
        match std::mem::replace(&mut self.step, ReadStep::Main) {
            ReadStep::Main => {
                let Some(main) = decode_main(result)? else {
                    return Ok(Next::Done(None));
                };
                self.step = ReadStep::Lanes(Box::new(main));
                Ok(Next::Call(lanes::lanes_page(self.job_id)))
            }
            ReadStep::Lanes(main) => {
                let (rows, _) = result.page()?;
                let open = lanes::parse_lanes(rows).map_err(StagingStoreError)?;
                Ok(Next::Done(Some(lanes::merged(*main, open))))
            }
        }
    }
}

fn main_read(job_id: JobId) -> IoCall {
    let row = (
        STAGING_STATE_KEYSPACE.to_string(),
        checkpoint_key(job_id, None),
    );
    IoCall::Read(vec![row])
}

fn decode_main(result: IoResult) -> Result<Option<StagingJobCheckpoint>, StagingStoreError> {
    let [row] = values::<1>(result)?;
    row.map(|value| postcard::from_bytes(&value))
        .transpose()
        .map_err(|error| StagingStoreError(error.to_string()))
}

/// Stores the checkpoint a job submitted as `job_id` starts from, before it can run.
#[derive(Debug, PartialEq)]
pub struct SeedCheckpointOperation {
    job_id: JobId,
    checkpoint: StagingJobCheckpoint,
}

impl SeedCheckpointOperation {
    pub fn new(job_id: JobId, checkpoint: StagingJobCheckpoint) -> Self {
        Self { job_id, checkpoint }
    }
}

impl Operation for SeedCheckpointOperation {
    type Output = ();
    type Error = StagingStoreError;

    fn first(&mut self) -> Result<Next<()>, StagingStoreError> {
        let value = postcard::to_allocvec(&self.checkpoint)
            .map_err(|error| StagingStoreError(error.to_string()))?;
        let row = (
            STAGING_STATE_KEYSPACE.to_string(),
            checkpoint_key(self.job_id, None),
            ByteView::from(value),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, StagingStoreError> {
        done(result)?;
        Ok(Next::Done(()))
    }
}

#[derive(Debug, PartialEq)]
enum ClearStep {
    Page,
    Delete,
}

/// Deletes every row of `key_space` below `prefix`, page by page.
#[derive(Debug, PartialEq)]
struct ClearRowsOperation {
    key_space: KeySpace,
    prefix: Key,
    step: ClearStep,
}

impl ClearRowsOperation {
    fn new(key_space: &str, prefix: Key) -> Self {
        Self {
            key_space: key_space.to_string(),
            prefix,
            step: ClearStep::Page,
        }
    }

    fn page(&mut self) -> Next<()> {
        self.step = ClearStep::Page;
        Next::Call(IoCall::Page {
            space: self.key_space.clone(),
            prefix: self.prefix.clone(),
            after: None,
            limit: QUEUE_CLEANUP_PAGE,
        })
    }
}

impl Operation for ClearRowsOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(self.page())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        match self.step {
            ClearStep::Page => {
                let (rows, _) = result.page()?;
                if rows.is_empty() {
                    return Ok(Next::Done(()));
                }
                self.step = ClearStep::Delete;
                let space = &self.key_space;
                let deletes = (rows.into_iter())
                    .map(|(key, _)| (space.clone(), key))
                    .collect();
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            ClearStep::Delete => {
                done(result)?;
                Ok(self.page())
            }
        }
    }
}

/// Deletes the job's queue and manifest rows; rows a failed pass leaves stay until the next run
/// and do not change the outcome.
#[derive(Debug, PartialEq)]
struct ClearQueueOperation {
    job_id: JobId,
    spaces: Vec<&'static str>,
}

impl ClearQueueOperation {
    fn new(job_id: JobId) -> Self {
        Self {
            job_id,
            spaces: vec![STAGING_MANIFEST_KEYSPACE, STAGING_QUEUE_KEYSPACE],
        }
    }

    fn clear_next(&mut self) -> Next<()> {
        match self.spaces.pop() {
            Some(space) => {
                let prefix = ByteView::from(self.job_id.to_bytes().to_vec());
                Next::Call(IoCall::child(ClearRowsOperation::new(space, prefix)))
            }
            None => Next::Done(()),
        }
    }
}

impl Operation for ClearQueueOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(self.clear_next())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if let Err(error) = result.child::<(), IoError>()? {
            warn!(job_id = %self.job_id, error = %error, "Staging job row cleanup failed");
        }
        Ok(self.clear_next())
    }
}

fn initial_checkpoint(spec: &StagingJobSpec) -> StagingJobCheckpoint {
    StagingJobCheckpoint {
        phase: StagingJobPhase::Queued,
        flat: None,
        items_done: 0,
        prefixes_done: 0,
        walk: None,
        manifest: None,
        table: spec.table.as_ref().map(|_| Default::default()),
        queue_head: 0,
        queue_tail: 0,
        items_current: 0,
        items_succeeded: 0,
        items_failed: 0,
        items_total: (spec.prefixes.is_empty() && spec.table.is_none())
            .then_some(spec.items.len() as u64),
        bytes_current: 0,
        bytes_total: None,
        bytes_discovered: 0,
        current_path: None,
        errors: Vec::new(),
    }
}

/// Where a job taken over by a range's new coordinator goes on: the former position without
/// counters, errors or directory queue. A flat walk keeps its cursor and rereads its manifest;
/// a manifest read, a table or a directory walk, whose queue stays behind, starts over.
pub(crate) fn handed_checkpoint(
    spec: &StagingJobSpec,
    at: &StagingJobCheckpoint,
) -> StagingJobCheckpoint {
    let mut next = initial_checkpoint(spec);
    // The table is read again from its start, pinned to what the former job first read.
    if let (Some(table), Some(read)) = (next.table.as_mut(), at.table.as_ref()) {
        table.version_id = read.version_id;
        table.observation = read.observation.clone();
    }
    next.flat = at.flat;
    next.items_done = at.items_done;
    let queued = at.queue_head < at.queue_tail;
    let open = at.walk.is_some() || at.manifest.is_some() || queued;
    let flat_walk = at.flat == Some(true)
        && at.manifest.is_none()
        && !queued
        && at
            .walk
            .as_ref()
            .is_some_and(|walk| walk.queue_row.is_none());
    match flat_walk {
        true => {
            next.prefixes_done = at.prefixes_done;
            next.walk = at.walk.clone();
            // The manifest's rows stay with the former job, so this one reads it before its walk.
            let manifest = spec.checksum_manifest.as_ref().zip(at.walk.as_ref());
            next.manifest = manifest.map(|(name, walk)| ManifestRead {
                source_path: format!("{}{name}", walk.source_path),
                offset: 0,
                observation: None,
            });
        }
        false => next.prefixes_done = at.prefixes_done.saturating_sub(u64::from(open)),
    }
    next
}

/// The checkpoint row of a job, or of its prefix lane `slot`, which extends the job's key.
pub(crate) fn checkpoint_key(job_id: JobId, slot: Option<u64>) -> ByteView {
    let mut key = job_id.to_bytes().to_vec();
    if let Some(slot) = slot {
        key.extend_from_slice(&slot.to_be_bytes());
    }
    ByteView::from(key)
}

pub(super) fn queue_key(job_id: JobId, position: u64) -> Key {
    let mut key = job_id.to_bytes().to_vec();
    key.extend_from_slice(&position.to_be_bytes());
    ByteView::from(key)
}

/// How one item or file failed.
#[derive(Debug, PartialEq)]
pub(super) enum ItemFailure {
    Denied(String),
    Stage(String),
    System(String),
    /// Ends the whole job, such as a replaced destination bucket.
    Lasting(String),
}

impl From<IoError> for ItemFailure {
    fn from(error: IoError) -> Self {
        Self::System(error.to_string())
    }
}

/// How a page, chunk or lane step ended early.
#[derive(Debug, PartialEq)]
pub(super) enum PageFailure {
    Retry(String),
    Permanent(String),
    /// Page transactions kept conflicting with other writers; the job is deferred.
    Conflict(String),
}

impl From<IoError> for PageFailure {
    fn from(error: IoError) -> Self {
        Self::Retry(error.to_string())
    }
}

impl From<ItemFailure> for PageFailure {
    /// A system failure retries the page; any other ends the job.
    fn from(failure: ItemFailure) -> Self {
        match failure {
            ItemFailure::System(message) => Self::Retry(message),
            ItemFailure::Denied(message)
            | ItemFailure::Stage(message)
            | ItemFailure::Lasting(message) => Self::Permanent(message),
        }
    }
}

#[cfg(test)]
pub(crate) mod pure_tests {
    use super::walk::{directory_list_path, record_failure};
    use super::*;
    use crate::staging::page::rows::MAX_ERROR_SAMPLE;
    use aruna_core::structs::execution::source_connector::{SourceConnector, SourceConnectorKind};
    use aruna_core::structs::execution::staging::StagingStrategy;

    pub(super) fn test_spec() -> StagingJobSpec {
        let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]);
        StagingJobSpec {
            auth_context: aruna_core::structs::identity::auth::AuthContext {
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
            prefixes: vec![aruna_core::structs::execution::job::StagingJobPrefix {
                source_prefix: "refseq".to_string(),
                target_prefix: String::new(),
            }],
            checksum_manifest: None,
            table: None,
            mirrors: Vec::new(),
        }
    }

    /// A run of the test spec.
    pub(super) fn test_run() -> StagingRun {
        StagingRun {
            spec: Arc::new(test_spec()),
            job_id: JobId::from_bytes([7; 16]),
            claim_token: Ulid::from_bytes([6; 16]),
            routing: Default::default(),
            seed: 9,
        }
    }

    /// The bucket the test spec was submitted for.
    pub(super) fn test_bucket(spec: &StagingJobSpec) -> BucketInfo {
        let (group_id, created_at, created_by) = spec.bucket_identity.clone();
        BucketInfo {
            group_id,
            created_at,
            created_by,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: None,
        }
    }

    /// A connector of the test spec's group.
    pub(super) fn test_connector() -> SourceConnector {
        let spec = test_spec();
        SourceConnector {
            connector_id: spec.connector_id,
            group_id: spec.group_id,
            name: "source".to_string(),
            kind: SourceConnectorKind::S3,
            public_config: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: spec.auth_context.user_id,
            namespace: None,
        }
    }

    /// The answer of a child with this outcome.
    pub(super) fn child_answer<T, E>(outcome: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(outcome))
    }

    // A handed-over job keeps a flat walk's position and restarts a prefix whose directory
    // queue stayed with the former coordinator; counters start at zero.
    #[test]
    fn handed_job_resumes() {
        use aruna_core::structs::execution::job::StagingWalk;
        let spec = test_spec();
        let walk = StagingWalk {
            source_path: "refseq/".to_string(),
            target_prefix: String::new(),
            cursor: None,
            queue_row: None,
            from: Some("refseq/k".to_string()),
        };
        let mut at = initial_checkpoint(&spec);
        at.flat = Some(true);
        at.items_done = 3;
        at.prefixes_done = 1;
        at.walk = Some(walk.clone());
        at.items_succeeded = 40;
        record_failure(&mut at, "a", "b", "denied".to_string());
        let flat = handed_checkpoint(&spec, &at);
        assert_eq!((flat.items_done, flat.prefixes_done), (3, 1));
        assert_eq!(flat.walk, Some(walk.clone()));
        assert_eq!((flat.items_succeeded, flat.items_failed), (0, 0));
        assert!(flat.errors.is_empty());
        let mut queued = at.clone();
        queued.flat = Some(false);
        queued.queue_tail = 2;
        queued.walk = Some(StagingWalk {
            queue_row: Some(0),
            ..walk
        });
        let restarted = handed_checkpoint(&spec, &queued);
        assert_eq!((restarted.prefixes_done, restarted.walk), (0, None));
        assert_eq!((restarted.queue_head, restarted.queue_tail), (0, 0));
        let mut finished = at;
        finished.walk = None;
        assert_eq!(handed_checkpoint(&spec, &finished).prefixes_done, 1);
    }

    // The manifest's checksums stay with the former job's rows, so a flat walk taken over
    // reads its manifest again from the start before its next page.
    #[test]
    fn handed_manifest_rereads() {
        use aruna_core::structs::execution::job::StagingWalk;
        let spec = StagingJobSpec {
            checksum_manifest: Some("SHA256SUMS".to_string()),
            ..test_spec()
        };
        let walk = StagingWalk {
            source_path: "refseq/".to_string(),
            target_prefix: String::new(),
            cursor: None,
            queue_row: None,
            from: Some("refseq/k".to_string()),
        };
        let mut at = initial_checkpoint(&spec);
        at.flat = Some(true);
        at.prefixes_done = 1;
        at.walk = Some(walk.clone());
        let handed = handed_checkpoint(&spec, &at);
        assert_eq!(handed.walk, Some(walk));
        let manifest = handed.manifest.expect("the manifest is read again");
        assert_eq!(manifest.source_path, "refseq/SHA256SUMS");
        assert_eq!((manifest.offset, manifest.observation), (0, None));
        assert_eq!(handed_checkpoint(&test_spec(), &at).manifest, None);
    }

    // An uploaded table read to its end while folders still wait keeps its version on handover.
    #[test]
    fn handed_table_pinned() {
        use aruna_core::structs::execution::job::TableRead;
        let mut spec = test_spec();
        spec.prefixes.clear();
        let rows = serde_json::json!({ "kind": "folder", "source": "{p}", "target": "{p}" });
        let table = serde_json::json!({ "source": { "object": { "bucket": "b", "key": "t.csv" } },
            "rows": rows });
        spec.table = Some(serde_json::from_value(table).unwrap());
        let mut at = initial_checkpoint(&spec);
        let version = Ulid::from_bytes([9; 16]);
        at.table = Some(Box::new(TableRead {
            offset: 4096,
            version_id: Some(version),
            columns: Some(vec!["p".to_string()]),
            done: true,
            ..TableRead::default()
        }));
        at.queue_tail = 3;
        let handed = handed_checkpoint(&spec, &at);
        let table = handed.table.expect("the table is read again");
        assert_eq!((table.offset, table.done, table.columns), (0, false, None));
        assert_eq!(table.version_id, Some(version));
        assert_eq!((handed.queue_head, handed.queue_tail), (0, 0));
    }

    #[test]
    fn checkpoint_stays_small() {
        let spec = test_spec();
        let mut checkpoint = initial_checkpoint(&spec);
        for _ in 0..1000 {
            record_failure(&mut checkpoint, "a", "b", "denied".to_string());
        }

        // Failures are counted, but only a bounded sample is kept in the checkpoint.
        assert_eq!(checkpoint.items_failed, 1000);
        assert_eq!(checkpoint.errors.len(), MAX_ERROR_SAMPLE);
        assert_eq!(checkpoint.items_total, None);
        assert_eq!(
            directory_list_path(&spec.prefixes[0].source_prefix),
            "refseq/"
        );
    }

    // A bucket recreated under the submitted name ends the job instead of receiving its files.
    #[test]
    fn replaced_bucket_ends() {
        let spec = test_spec();
        let mut bucket = test_bucket(&spec);
        let answer = |bucket: BucketInfo| child_answer(Ok::<_, GetBucketError>(bucket));
        assert!(pinned(&spec, answer(bucket.clone())).is_ok());
        bucket.group_id = Ulid::from_bytes([8; 16]);
        assert!(matches!(
            pinned(&spec, answer(bucket)),
            Err(ItemFailure::Lasting(_))
        ));
    }

    // The checkpoint read adds the open lanes to the job's own counters.
    #[test]
    fn checkpoint_read_merges() {
        let job_id = JobId::from_bytes([7; 16]);
        let spec = test_spec();
        let mut main = initial_checkpoint(&spec);
        main.items_current = 2;
        let mut read = ReadCheckpointOperation::new(job_id);
        assert_eq!(read.first(), Ok(Next::Call(main_read(job_id))));
        let value = ByteView::from(postcard::to_allocvec(&main).unwrap());
        let Ok(Next::Call(IoCall::Page { limit, .. })) =
            read.next(IoResult::Values(vec![Some(value)]))
        else {
            panic!("expected the lane page");
        };
        assert_eq!(limit, lanes::LANE_ROWS);
        let mut lane = main.clone();
        lane.items_current = 3;
        let key = checkpoint_key(job_id, Some(0));
        let row = (key, ByteView::from(postcard::to_allocvec(&lane).unwrap()));
        let page = IoResult::Page {
            rows: vec![row],
            next: None,
        };
        let Ok(Next::Done(Some(view))) = read.next(page) else {
            panic!("expected the merged checkpoint");
        };
        assert_eq!(view.items_current, 5);
    }

    // A job without a checkpoint row has nothing to read further.
    #[test]
    fn missing_checkpoint_none() {
        let mut read = ReadCheckpointOperation::new(JobId::from_bytes([7; 16]));
        read.first().unwrap();
        assert_eq!(
            read.next(IoResult::Values(vec![None])),
            Ok(Next::Done(None))
        );
    }

    // The queue cleanup pages through a key space until it finds no row.
    #[test]
    fn clear_rows_pages() {
        let prefix = ByteView::from(vec![1]);
        let mut clear = ClearRowsOperation::new(STAGING_QUEUE_KEYSPACE, prefix.clone());
        assert!(matches!(clear.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let key = ByteView::from(vec![1, 2]);
        let rows = vec![(key.clone(), ByteView::from(vec![0]))];
        let page = IoResult::Page { rows, next: None };
        assert_eq!(
            clear.next(page),
            Ok(Next::Call(IoCall::Delete(vec![(
                STAGING_QUEUE_KEYSPACE.to_string(),
                key
            )])))
        );
        assert!(matches!(
            clear.next(IoResult::Done),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(clear.next(empty), Ok(Next::Done(())));
    }

    // A failed cleanup of one key space is logged and the other is still cleared.
    #[test]
    fn clear_queue_continues() {
        let mut clear = ClearQueueOperation::new(JobId::from_bytes([7; 16]));
        assert!(matches!(clear.first(), Ok(Next::Call(IoCall::Child(_)))));
        let failed = child_answer(Err::<(), _>(IoError::Aborted));
        assert!(matches!(
            clear.next(failed),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let cleared = child_answer(Ok::<(), IoError>(()));
        assert_eq!(clear.next(cleared), Ok(Next::Done(())));
    }
}
