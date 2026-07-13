use std::time::Duration;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{JOB_DEDUP_INDEX_KEYSPACE, JOB_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{JobId, JobPayload, JobRecord, job_record_key};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, NodeId, UserId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use super::store::{decode_job_record, job_insert_entries};

/// Kick the drain so a submitted job is claimed promptly; this timer is never persisted.
pub fn schedule_job_drain_effect() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainJobQueue,
        after: Duration::ZERO,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitJobSpec {
    pub payload: JobPayload,
    pub created_by: UserId,
    pub owner_node_id: NodeId,
    pub dedup_key: Option<Vec<u8>>,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitJobResult {
    pub job_id: JobId,
    /// `false` when a live job with the same `dedup_key` already existed.
    pub created: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum SubmitJobError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event while submitting job: {0}")]
    UnexpectedEvent(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SubmitState {
    Init,
    ReadDedup,
    VerifyDedup(JobId),
    WriteJob,
    ScheduleDrain,
    Finish,
    Error,
}

/// Effect-driven submit; a live `job_dedup_index` entry short-circuits to the existing
/// id after verifying that job's record still decodes. A dangling entry (record
/// quarantined or gone) falls through to a fresh create whose batch write repoints the
/// dedup row, so a corrupt record cannot poison its key. Dedup is best-effort
/// (read-then-write, no cross-submit lock), so under a concurrent race two jobs may
/// share a key. Execution is at-least-once: consumers must be idempotent (`Probe`'s
/// marker file is the example).
#[derive(Debug, PartialEq)]
pub struct SubmitJobOperation {
    record: JobRecord,
    state: SubmitState,
    output: Option<Result<SubmitJobResult, SubmitJobError>>,
}

impl SubmitJobOperation {
    pub fn new(spec: SubmitJobSpec) -> Self {
        let record = JobRecord::new(
            JobId::new(),
            spec.payload,
            spec.created_by,
            spec.owner_node_id,
            spec.now_ms,
            spec.now_ms,
            spec.dedup_key,
        );
        Self {
            record,
            state: SubmitState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: SubmitJobError) -> Effects {
        self.state = SubmitState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn begin(&mut self) -> Effects {
        match self.record.dedup_key.clone() {
            Some(dedup_key) => {
                self.state = SubmitState::ReadDedup;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: JOB_DEDUP_INDEX_KEYSPACE.to_string(),
                    key: ByteView::from(dedup_key),
                    txn_id: None,
                })]
            }
            None => self.write_job(),
        }
    }

    fn verify_dedup(&mut self, job_id: JobId) -> Effects {
        self.state = SubmitState::VerifyDedup(job_id);
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: JOB_KEYSPACE.to_string(),
            key: job_record_key(job_id),
            txn_id: None,
        })]
    }

    fn write_job(&mut self) -> Effects {
        let writes = match job_insert_entries(&self.record) {
            Ok(writes) => writes,
            Err(error) => return self.fail(error.into()),
        };
        self.state = SubmitState::WriteJob;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })]
    }

    fn schedule_drain(&mut self) -> Effects {
        self.state = SubmitState::ScheduleDrain;
        smallvec![schedule_job_drain_effect()]
    }

    fn finish_existing(&mut self, job_id: JobId) -> Effects {
        self.state = SubmitState::Finish;
        self.output = Some(Ok(SubmitJobResult {
            job_id,
            created: false,
        }));
        smallvec![]
    }

    fn finish_created(&mut self) -> Effects {
        self.state = SubmitState::Finish;
        self.output = Some(Ok(SubmitJobResult {
            job_id: self.record.job_id,
            created: true,
        }));
        smallvec![]
    }
}

impl Operation for SubmitJobOperation {
    type Output = SubmitJobResult;
    type Error = SubmitJobError;

    fn start(&mut self) -> Effects {
        self.begin()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            SubmitState::Init => self.begin(),
            SubmitState::ReadDedup => match event {
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => match <[u8; 16]>::try_from(value.as_ref()) {
                    Ok(bytes) => self.verify_dedup(JobId::from_bytes(bytes)),
                    Err(_) => self.write_job(),
                },
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => self.write_job(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(SubmitJobError::UnexpectedEvent(format!("{other:?}"))),
            },
            SubmitState::VerifyDedup(job_id) => match event {
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => match decode_job_record(&value) {
                    Ok(_) => self.finish_existing(job_id),
                    Err(error) => {
                        warn!(job_id = %job_id, error = %error, "Dedup entry points at an undecodable job; creating fresh");
                        self.write_job()
                    }
                },
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
                    warn!(job_id = %job_id, "Dedup entry points at a missing job; creating fresh");
                    self.write_job()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(SubmitJobError::UnexpectedEvent(format!("{other:?}"))),
            },
            SubmitState::WriteJob => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => self.schedule_drain(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(SubmitJobError::UnexpectedEvent(format!("{other:?}"))),
            },
            SubmitState::ScheduleDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => self.finish_created(),
                Event::Task(TaskEvent::Error { .. }) => {
                    warn!("Job persisted but drain scheduling returned an error");
                    self.finish_created()
                }
                other => {
                    warn!(event = ?other, "Job persisted but drain scheduling returned an unexpected event");
                    self.finish_created()
                }
            },
            SubmitState::Finish | SubmitState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, SubmitState::Finish | SubmitState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(SubmitJobError::UnexpectedEvent(
            "submit operation finished without output".to_string(),
        )))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::jobs::store::read_job_record;
    use aruna_core::keyspaces::{
        JOB_KEYSPACE, JOB_OWNER_INDEX_KEYSPACE, JOB_SCHEDULE_INDEX_KEYSPACE,
    };
    use aruna_core::structs::{JobState, RealmId};
    use aruna_storage::{FjallStorage, StorageHandle};
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    fn context(storage: StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
        }
    }

    fn spec(dedup_key: Option<Vec<u8>>) -> SubmitJobSpec {
        SubmitJobSpec {
            payload: JobPayload::Probe {
                steps: 2,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            created_by: UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            owner_node_id: node_id(7),
            dedup_key,
            now_ms: 1_000,
        }
    }

    async fn count_keyspace(storage: &StorageHandle, key_space: &str) -> usize {
        crate::jobs::store::iter_prefix_page(storage, key_space, None, None, 64, None)
            .await
            .expect("scan")
            .0
            .len()
    }

    async fn write_raw(storage: &StorageHandle, key_space: &str, key: ByteView, value: ByteView) {
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn submit_creates_job() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());

        let result = drive(SubmitJobOperation::new(spec(None)), &ctx)
            .await
            .expect("submit succeeds");
        assert!(result.created);

        let record = read_job_record(&storage, result.job_id, None)
            .await
            .unwrap()
            .expect("job persisted");
        assert_eq!(record.state, JobState::Queued);
        assert_eq!(
            count_keyspace(&storage, JOB_SCHEDULE_INDEX_KEYSPACE).await,
            1
        );
        assert_eq!(count_keyspace(&storage, JOB_OWNER_INDEX_KEYSPACE).await, 1);
        assert_eq!(count_keyspace(&storage, JOB_DEDUP_INDEX_KEYSPACE).await, 0);
    }

    #[tokio::test]
    async fn dedup_returns_existing() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());

        let first = drive(SubmitJobOperation::new(spec(Some(b"k".to_vec()))), &ctx)
            .await
            .unwrap();
        assert!(first.created);
        assert_eq!(count_keyspace(&storage, JOB_DEDUP_INDEX_KEYSPACE).await, 1);

        let second = drive(SubmitJobOperation::new(spec(Some(b"k".to_vec()))), &ctx)
            .await
            .unwrap();
        assert!(!second.created);
        assert_eq!(second.job_id, first.job_id);
        // No duplicate record created.
        assert_eq!(count_keyspace(&storage, JOB_KEYSPACE).await, 1);
    }

    // Perf budget: a non-dedup submit is one atomic batch write of <=4 keys plus a
    // non-persisted drain kick.
    #[tokio::test]
    async fn submit_writes_once() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());

        let before = storage.snapshot_metrics().requests_total;
        drive(SubmitJobOperation::new(spec(None)), &ctx)
            .await
            .unwrap();
        let after = storage.snapshot_metrics().requests_total;
        assert_eq!(
            after - before,
            1,
            "submit performs exactly one storage request"
        );

        let keys = count_keyspace(&storage, JOB_KEYSPACE).await
            + count_keyspace(&storage, JOB_SCHEDULE_INDEX_KEYSPACE).await
            + count_keyspace(&storage, JOB_OWNER_INDEX_KEYSPACE).await
            + count_keyspace(&storage, JOB_DEDUP_INDEX_KEYSPACE).await;
        assert!(keys <= 4, "submit writes at most four keys, got {keys}");
    }

    // A dedup row left dangling by a quarantined record must not ghost future submits.
    #[tokio::test]
    async fn ghost_dedup_heals() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let ghost = JobId::from_bytes([9u8; 16]);
        write_raw(
            &storage,
            JOB_DEDUP_INDEX_KEYSPACE,
            ByteView::from(b"k".to_vec()),
            ByteView::from(ghost.to_bytes().to_vec()),
        )
        .await;

        let result = drive(SubmitJobOperation::new(spec(Some(b"k".to_vec()))), &ctx)
            .await
            .unwrap();
        assert!(result.created);
        assert_ne!(result.job_id, ghost);
        assert_eq!(
            crate::jobs::store::find_dedup_job(&storage, b"k", None)
                .await
                .unwrap(),
            Some(result.job_id)
        );
    }

    // Same when the record row still exists but no longer decodes.
    #[tokio::test]
    async fn corrupt_dedup_heals() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let ghost = JobId::from_bytes([9u8; 16]);
        write_raw(
            &storage,
            JOB_KEYSPACE,
            job_record_key(ghost),
            ByteView::from(vec![1, 2, 3]),
        )
        .await;
        write_raw(
            &storage,
            JOB_DEDUP_INDEX_KEYSPACE,
            ByteView::from(b"k".to_vec()),
            ByteView::from(ghost.to_bytes().to_vec()),
        )
        .await;

        let result = drive(SubmitJobOperation::new(spec(Some(b"k".to_vec()))), &ctx)
            .await
            .unwrap();
        assert!(result.created);
        assert_ne!(result.job_id, ghost);
        assert_eq!(
            crate::jobs::store::find_dedup_job(&storage, b"k", None)
                .await
                .unwrap(),
            Some(result.job_id)
        );
    }

    #[tokio::test]
    async fn dedup_after_terminal() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());

        let first = drive(SubmitJobOperation::new(spec(Some(b"k".to_vec()))), &ctx)
            .await
            .unwrap();
        // Take the job through claim + terminal so the dedup entry is cleared.
        crate::jobs::store::claim_job(&storage, first.job_id, node_id(3), 2_000)
            .await
            .unwrap();
        let claimed = read_job_record(&storage, first.job_id, None)
            .await
            .unwrap()
            .unwrap();
        let token = claimed.claim.unwrap().claim_token;
        crate::jobs::store::transition_to_running(&storage, first.job_id, token, 2_100)
            .await
            .unwrap();
        crate::jobs::store::cancel_running_job(&storage, first.job_id, token, 2_200)
            .await
            .unwrap();

        let second = drive(SubmitJobOperation::new(spec(Some(b"k".to_vec()))), &ctx)
            .await
            .unwrap();
        assert!(second.created);
        assert_ne!(second.job_id, first.job_id);
    }
}
