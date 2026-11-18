//! Runs the work that waits for a bucket key once that key is installed on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::BucketKeyRef;
use thiserror::Error;

use crate::blob::migration::queue::ResumeTransitionsOperation;
use crate::blob::migration::shared::MigrationError;
use crate::blob::promote::PromoteError;
use crate::blob::unlocked::PromoteUnlockedOperation;
use crate::jobs::remote_key::RemoteWakesOperation;
use crate::jobs::store::JobMutationError;
use crate::jobs::store::row_calls;
use crate::replication::parking::WakeParkedOperation;
use crate::replication::queue::BlobQueueError;
use crate::s3::abe_copies::{CompleteCopiesError, CompleteCopiesOperation};

#[derive(Debug, Error, PartialEq)]
pub enum KeyWakeError {
    #[error(transparent)]
    Jobs(#[from] JobMutationError),
    #[error(transparent)]
    Promote(#[from] PromoteError),
    #[error("could not resume encryption transitions: {0}")]
    Transition(String),
    #[error("could not wake copies waiting for the key: {0}")]
    Copies(String),
    #[error("could not write envelopes of pending copies: {0}")]
    Envelopes(String),
}

impl From<IoError> for KeyWakeError {
    fn from(error: IoError) -> Self {
        Self::Jobs(error.into())
    }
}

#[derive(Debug, PartialEq)]
enum WakeStep {
    Jobs,
    Remote,
    Resume,
    Promote,
    ResumeAgain,
    Copies,
    Envelopes,
}

/// Resumes jobs, remote wakes, transitions, pending promotion, waiting copies and pending copy
/// envelopes whenever a key becomes usable. Every step runs even after another failed, so one
/// corrupt archive holds nothing back; the first failure in report order is the answer.
#[derive(Debug, PartialEq)]
pub struct WakeUnlockedOperation {
    key: BucketKeyRef,
    now_ms: u64,
    origin: (RealmId, NodeId),
    limits: RoCrateLimits,
    step: WakeStep,
    woken: Result<usize, JobMutationError>,
    queued: Result<(), JobMutationError>,
    resumed: Result<(), String>,
    promoted: Result<usize, PromoteError>,
    copies: Result<(), String>,
}

type WakeNext = Result<Next<(usize, usize)>, KeyWakeError>;

impl WakeUnlockedOperation {
    pub fn new(
        key: BucketKeyRef,
        now_ms: u64,
        origin: (RealmId, NodeId),
        limits: &RoCrateLimits,
    ) -> Self {
        Self {
            key,
            now_ms,
            origin,
            limits: limits.clone(),
            step: WakeStep::Jobs,
            woken: Ok(0),
            queued: Ok(()),
            resumed: Ok(()),
            promoted: Ok(0),
            copies: Ok(()),
        }
    }

    fn call(&mut self, step: WakeStep, call: IoCall) -> WakeNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn resume(&mut self, step: WakeStep) -> WakeNext {
        let resume = ResumeTransitionsOperation::new(self.key);
        self.call(step, IoCall::child(resume))
    }

    /// A failed earlier resume stays the answer.
    fn note_resume(&mut self, result: IoResult) -> Result<(), IoError> {
        let resumed = result.child::<(), MigrationError>()?;
        if self.resumed.is_ok() {
            self.resumed = resumed.map_err(|error| error.to_string());
        }
        Ok(())
    }

    /// Copies waiting for this source key run after promotion, so pending sources are hashed.
    fn wake_copies(&mut self) -> WakeNext {
        let wake = WakeParkedOperation::new(self.key, self.now_ms);
        self.call(WakeStep::Copies, IoCall::child(wake))
    }

    fn finish(&mut self, envelopes: Result<(), String>) -> WakeNext {
        std::mem::replace(&mut self.queued, Ok(()))?;
        std::mem::replace(&mut self.resumed, Ok(())).map_err(KeyWakeError::Transition)?;
        std::mem::replace(&mut self.copies, Ok(())).map_err(KeyWakeError::Copies)?;
        envelopes.map_err(KeyWakeError::Envelopes)?;
        let woken = std::mem::replace(&mut self.woken, Ok(0))?;
        let promoted = std::mem::replace(&mut self.promoted, Ok(0))?;
        Ok(Next::Done((woken, promoted)))
    }
}

impl Operation for WakeUnlockedOperation {
    type Output = (usize, usize);
    type Error = KeyWakeError;

    fn first(&mut self) -> WakeNext {
        let wake = row_calls::wake_key_waits(self.key, self.now_ms);
        Ok(Next::Call(IoCall::child(wake)))
    }

    fn next(&mut self, result: IoResult) -> WakeNext {
        match self.step {
            // Remote waiters get owed wakes before promotion, so a corrupt archive never
            // holds them back.
            WakeStep::Jobs => {
                self.woken = result.child::<usize, JobMutationError>()?;
                let remote = RemoteWakesOperation::new(self.key);
                self.call(WakeStep::Remote, IoCall::child(remote))
            }
            WakeStep::Remote => {
                self.queued = result.child::<(), JobMutationError>()?;
                self.resume(WakeStep::Resume)
            }
            WakeStep::Resume => {
                self.note_resume(result)?;
                let promote = PromoteUnlockedOperation::new(self.key, self.origin, &self.limits);
                self.call(WakeStep::Promote, IoCall::child(promote))
            }
            WakeStep::Promote => {
                self.promoted = result.child::<usize, PromoteError>()?;
                match self.promoted {
                    Ok(0) => self.wake_copies(),
                    _ => self.resume(WakeStep::ResumeAgain),
                }
            }
            WakeStep::ResumeAgain => {
                self.note_resume(result)?;
                self.wake_copies()
            }
            WakeStep::Copies => {
                let copies = result.child::<usize, BlobQueueError>()?;
                self.copies = copies.map(|_| ()).map_err(|error| error.to_string());
                let walk = CompleteCopiesOperation::new(self.key, self.origin);
                self.call(WakeStep::Envelopes, IoCall::child(walk))
            }
            WakeStep::Envelopes => {
                let envelopes = result.child::<usize, CompleteCopiesError>()?;
                self.finish(envelopes.map(|_| ()).map_err(|error| error.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::jobs::key_locks::{KeyCheckError, LockedInputsOperation, ParkLockedOperation};
    use crate::jobs::store::job_calls::ClaimOutcome;
    use crate::jobs::store::test_calls::insert_job;
    use crate::jobs::store::test_calls::park_job;
    use crate::jobs::store::test_calls::{claim_job, read_job_record};
    use aruna_core::UserId;
    use aruna_core::keyspaces::{
        BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE,
        PATHS_INDEX_KEYSPACE,
    };
    use aruna_core::structs::execution::job::CapturedInput;
    use aruna_core::structs::execution::job::{
        JobError, JobId, JobPayload, JobRecord, JobState, KeyWait,
    };
    use aruna_core::structs::storage::blob::{BackendLocation, BlobVersion, HashIndex, VersionKey};
    use aruna_core::structs::storage::encryption::BucketEncryption;
    use aruna_core::time::unix_timestamp_millis;
    use byteview::ByteView;
    use ulid::Ulid;

    async fn wake_unlocked(
        context: &DriverContext,
        key: BucketKeyRef,
        now_ms: u64,
        origin: (RealmId, NodeId),
        limits: &RoCrateLimits,
    ) -> Result<(usize, usize), KeyWakeError> {
        drive(
            WakeUnlockedOperation::new(key, now_ms, origin, limits),
            context,
        )
        .await
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    fn wake_op() -> WakeUnlockedOperation {
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        WakeUnlockedOperation::new(key, 5, (RealmId([1; 32]), node), &RoCrateLimits::default())
    }

    fn is_call(next: &WakeNext) -> bool {
        matches!(next, Ok(Next::Call(IoCall::Child(_))))
    }

    /// Runs the steps up to the promotion result.
    fn at_promotion(operation: &mut WakeUnlockedOperation) {
        assert!(is_call(&operation.first()));
        assert!(is_call(
            &operation.next(child(Ok::<_, JobMutationError>(1usize)))
        ));
        assert!(is_call(
            &operation.next(child(Ok::<_, JobMutationError>(())))
        ));
        assert!(is_call(&operation.next(child(Ok::<_, MigrationError>(())))));
        assert_eq!(operation.step, WakeStep::Promote);
    }

    fn after_copies(operation: &mut WakeUnlockedOperation) -> WakeNext {
        assert!(is_call(
            &operation.next(child(Ok::<_, BlobQueueError>(0usize)))
        ));
        operation.next(child(Ok::<_, CompleteCopiesError>(0usize)))
    }

    // Nothing promoted needs no second resume, and the counts are the answer.
    #[test]
    fn plain_wake_counts() {
        let mut operation = wake_op();
        at_promotion(&mut operation);
        assert!(is_call(
            &operation.next(child(Ok::<_, PromoteError>(0usize)))
        ));
        assert_eq!(operation.step, WakeStep::Copies);
        assert_eq!(after_copies(&mut operation), Ok(Next::Done((1, 0))));
    }

    // A promotion can unblock transitions, so they resume once more.
    #[test]
    fn promotion_resumes_again() {
        let mut operation = wake_op();
        at_promotion(&mut operation);
        assert!(is_call(
            &operation.next(child(Ok::<_, PromoteError>(2usize)))
        ));
        assert_eq!(operation.step, WakeStep::ResumeAgain);
        assert!(is_call(&operation.next(child(Ok::<_, MigrationError>(())))));
        assert_eq!(after_copies(&mut operation), Ok(Next::Done((1, 2))));
    }

    // A failed promotion stops nothing behind it and is reported at the end.
    #[test]
    fn promotion_failure_last() {
        let mut operation = wake_op();
        at_promotion(&mut operation);
        let failed = child(Err::<usize, _>(PromoteError::NoKey));
        assert!(is_call(&operation.next(failed)));
        assert!(is_call(&operation.next(child(Ok::<_, MigrationError>(())))));
        assert_eq!(
            after_copies(&mut operation),
            Err(KeyWakeError::Promote(PromoteError::NoKey))
        );
    }

    // The first resume failure is kept when the second resume passes.
    #[test]
    fn resume_failure_kept() {
        let mut operation = wake_op();
        operation.first().unwrap();
        operation
            .next(child(Ok::<_, JobMutationError>(0usize)))
            .unwrap();
        operation
            .next(child(Ok::<_, JobMutationError>(())))
            .unwrap();
        let failed = child(Err::<(), _>(MigrationError::Failed("stuck".to_string())));
        operation.next(failed).unwrap();
        operation
            .next(child(Ok::<_, PromoteError>(1usize)))
            .unwrap();
        operation.next(child(Ok::<_, MigrationError>(()))).unwrap();
        assert_eq!(
            after_copies(&mut operation),
            Err(KeyWakeError::Transition("stuck".to_string()))
        );
    }

    async fn park_on_lock(
        context: &DriverContext,
        record: &JobRecord,
        token: Ulid,
        node_id: NodeId,
        error: &JobError,
    ) -> bool {
        let node = (node_id, context.node_and_realm());
        let now_ms = unix_timestamp_millis();
        let park = ParkLockedOperation::on_error(record, token, node, error, now_ms);
        drive(park, context).await.unwrap_or(false)
    }

    #[tokio::test]
    async fn corrupt_archive_wakes() {
        use crate::driver::drive;
        use crate::jobs::store::remote_wait::{OwedWakesOperation, WaitRowOperation};
        use aruna_core::effects::StorageEffect;
        use aruna_core::keyspaces::PENDING_LOCATION_KEYSPACE;
        use aruna_core::structs::identity::auth::AuthContext;

        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let storage = &context.storage_handle;
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let waiter = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let auth = AuthContext {
            user_id: UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32])),
            realm_id: RealmId([1; 32]),
            path_restrictions: None,
            session: None,
        };
        let job_id = JobId::from_bytes([5; 16]);
        let register = WaitRowOperation::register(key, waiter, job_id, &auth);
        drive(register, &context).await.unwrap();
        // A pending row that cannot be decoded makes promotion fail.
        let corrupt = StorageEffect::Write {
            key_space: PENDING_LOCATION_KEYSPACE.to_string(),
            key: ByteView::from(b"archive".to_vec()),
            value: ByteView::from(b"corrupt".to_vec()),
            txn_id: None,
        };
        storage.send_storage_effect(corrupt).await;

        let origin = (RealmId([1; 32]), node);
        let limits = RoCrateLimits::default();
        let result = wake_unlocked(&context, key, 4_000, origin, &limits).await;

        assert!(matches!(result, Err(KeyWakeError::Promote(_))));
        let (owed, _) = drive(OwedWakesOperation::new(None), &context)
            .await
            .unwrap();
        assert_eq!(owed.len(), 1);
        assert_eq!((owed[0].waiter, owed[0].job_id), (waiter, job_id));
    }

    #[tokio::test]
    async fn wake_queues_parked() {
        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let job_id = JobId::from_bytes([5; 16]);
        let payload = JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32]));
        let record = JobRecord::new(job_id, payload, owner, node, 1_000, 1_000, None);
        insert_job(&context, &record).await.unwrap();
        let ClaimOutcome::Claimed(claimed) =
            claim_job(&context, job_id, node, 2_000).await.unwrap()
        else {
            panic!("job must be claimed")
        };
        let token = claimed.claim.unwrap().claim_token;
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let wait = KeyWait {
            node_id: node,
            bucket: "b".to_string(),
            group_id: None,
            key,
        };
        park_job(&context, job_id, token, 3_000, vec![wait])
            .await
            .unwrap();

        let origin = (RealmId([1; 32]), node);
        let limits = RoCrateLimits::default();
        let woken = wake_unlocked(&context, key, 4_000, origin, &limits)
            .await
            .unwrap();

        assert_eq!(woken, (1, 0));
        let record = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.state, JobState::Queued);
    }

    async fn locked_inputs(
        context: &DriverContext,
        inputs: &[CapturedInput],
        node_id: NodeId,
    ) -> Result<Vec<KeyWait>, KeyCheckError> {
        drive(LockedInputsOperation::new(inputs, node_id), context).await
    }

    /// A node holding one copy of content `[9; 32]`, sealed with a locked key of bucket "sealed".
    async fn sealed_node(sealed: bool) -> (tempfile::TempDir, DriverContext, NodeId, BucketKeyRef) {
        use aruna_core::effects::StorageEffect;
        use aruna_core::structs::storage::blob::{BackendRef, BlobLocationKey};
        use aruna_core::structs::storage::encryption::EncryptionMode;
        use aruna_core::structs::storage::format::{EncodingClass, PithosLayout, StoredFormat};
        use byteview::ByteView;
        use std::collections::HashMap;

        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(key.bucket_id),
            key_generation: 1,
            ..Default::default()
        };
        let layout = PithosLayout {
            stored_size: 130,
            metadata_digest: [4; 32],
            storage_generation: 0,
        };
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::from_bytes([3; 16]),
            // An unconverted copy is plain, yet its encrypting bucket still gates it.
            format: match sealed {
                true => StoredFormat::pithos(layout, key),
                false => StoredFormat::default(),
            },
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            staging: false,
            partial: false,
            blob_size: 100,
            hashes: HashMap::new(),
        };
        let row = BlobLocationKey::new([9; 32], EncodingClass::Raw, BackendRef::node_default());
        let version_id = Ulid::from_bytes([6; 16]);
        let version = BlobVersion::materialized(
            [9; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let version_key = VersionKey::new("sealed", "data.bin", version_id);
        let group_id = Ulid::from_bytes([4; 16]);
        let realm_id = RealmId([1; 32]);
        let alias = HashIndex::new(
            [9; 32], version_id, realm_id, group_id, node, "sealed", "data.bin",
        );
        let writes = vec![
            (
                BLOB_VERSIONS_KEYSPACE.to_string(),
                ByteView::from(version_key.to_bytes().unwrap()),
                ByteView::from(version.to_bytes().unwrap()),
            ),
            (
                PATHS_INDEX_KEYSPACE.to_string(),
                ByteView::from(alias.to_bytes().unwrap()),
                ByteView::from(Vec::new()),
            ),
            (
                BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                ByteView::from(b"sealed".to_vec()),
                ByteView::from(settings.to_bytes().unwrap()),
            ),
            (
                BLOB_LOCATIONS_KEYSPACE.to_string(),
                ByteView::from(row.to_bytes()),
                ByteView::from(location.to_bytes().unwrap()),
            ),
        ];
        let effect = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
        (dir, context, node, key)
    }

    #[tokio::test]
    async fn alias_beyond_page() {
        use aruna_core::effects::StorageEffect;

        let (_dir, context, node, key) = sealed_node(true).await;
        // Seventy older versions of the same content sort before the captured one.
        for version in 1..=70u128 {
            let decoy = HashIndex::new(
                [9; 32],
                Ulid::from(version),
                RealmId([1; 32]),
                Ulid::from_bytes([4; 16]),
                node,
                "sealed",
                format!("old-{version}"),
            );
            let effect = StorageEffect::Write {
                key_space: PATHS_INDEX_KEYSPACE.to_string(),
                key: decoy.to_bytes().unwrap().into(),
                value: Vec::new().into(),
                txn_id: None,
            };
            context.storage_handle.send_storage_effect(effect).await;
        }
        let input = CapturedInput {
            destination_key: "in".to_string(),
            source_node_id: node,
            version_id: Ulid::from_bytes([6; 16]),
            content: aruna_core::structs::execution::job::InputContent::Blake3([9; 32]),
            bytes: 100,
            policies: Vec::new(),
            source_group_id: Default::default(),
            public: false,
            link: None,
            source_pin: None,
            database: None,
            releases: Vec::new(),
            releases_complete: false,
            grant: None,
            served_by: None,
        };

        let waits = locked_inputs(&context, &[input], node).await.unwrap();

        assert_eq!(
            waits.iter().map(|wait| wait.key).collect::<Vec<_>>(),
            vec![key]
        );
    }

    #[tokio::test]
    async fn locked_input_waits() {
        let (_dir, context, node, key) = sealed_node(true).await;
        let input = |blake3: [u8; 32], source_node_id| CapturedInput {
            destination_key: "in".to_string(),
            source_node_id,
            version_id: Ulid::from_bytes([6; 16]),
            content: aruna_core::structs::execution::job::InputContent::Blake3(blake3),
            bytes: 100,
            policies: Vec::new(),
            source_group_id: Default::default(),
            public: false,
            link: None,
            source_pin: None,
            database: None,
            releases: Vec::new(),
            releases_complete: false,
            grant: None,
            served_by: None,
        };
        let remote = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let inputs = [
            input([9; 32], node),
            input([9; 32], remote),
            input([7; 32], node),
        ];

        let waits = locked_inputs(&context, &inputs, node).await.unwrap();

        assert_eq!(
            waits,
            vec![KeyWait {
                node_id: node,
                bucket: "sealed".to_string(),
                group_id: Some(Ulid::from_bytes([4; 16])),
                key,
            }]
        );
    }

    #[tokio::test]
    async fn lock_after_precheck() {
        use crate::jobs::lifecycle::stage::stage_error;
        use crate::jobs::store::test_calls::read_key_waits;
        use crate::jobs::workflow::workspace::LOCKED_INPUT;
        use crate::replication::bao_read::BaoReadError;
        use crate::replication::protocol::BaoReadRefusal;
        use aruna_core::structs::execution::job::JobError;

        // The input copy is not converted yet; the locked bucket key still parks the job.
        let (_dir, context, node, key) = sealed_node(false).await;
        let job_id = JobId::from_bytes([5; 16]);
        let payload = JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32]));
        let mut record = JobRecord::new(job_id, payload, owner, node, 1_000, 1_000, None);
        record.captured_inputs.push(CapturedInput {
            destination_key: "in".to_string(),
            source_node_id: node,
            version_id: Ulid::from_bytes([6; 16]),
            content: aruna_core::structs::execution::job::InputContent::Blake3([9; 32]),
            bytes: 100,
            policies: Vec::new(),
            source_group_id: Default::default(),
            public: false,
            link: None,
            source_pin: None,
            database: None,
            releases: Vec::new(),
            releases_complete: false,
            grant: None,
            served_by: None,
        });
        insert_job(&context, &record).await.unwrap();
        let ClaimOutcome::Claimed(claimed) =
            claim_job(&context, job_id, node, 2_000).await.unwrap()
        else {
            panic!("job must be claimed")
        };
        let token = claimed.claim.as_ref().unwrap().claim_token;

        // Any other preparation failure stays with the caller.
        let other = JobError::retryable("input read failed: offline");
        assert!(!park_on_lock(&context, &claimed, token, node, &other).await);
        // Text that only looks like a lock never decides the parking.
        let text = JobError::retryable(format!("{LOCKED_INPUT}: bucket is locked"));
        assert!(!park_on_lock(&context, &claimed, token, node, &text).await);
        // The precheck passed, then the key locked before the input was admitted.
        let refusal = BaoReadRefusal::BucketLocked(Ulid::from_bytes([8; 16]));
        let locked = stage_error("sealed", "in", Some(BaoReadError::Refused(refusal)));
        assert!(park_on_lock(&context, &claimed, token, node, &locked).await);

        let parked = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(parked.state, JobState::AwaitingKey);
        assert_eq!(parked.attempts, 0);
        let waits = read_key_waits(&context, job_id).await.unwrap();
        assert_eq!(
            waits.iter().map(|wait| wait.key).collect::<Vec<_>>(),
            vec![key]
        );
    }
}
