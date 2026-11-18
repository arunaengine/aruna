//! Runs the imports a former coordinator handed over as jobs of this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::StagingJobSpec;
use aruna_core::structs::storage::blob::BucketIdentity;
use aruna_core::types::{Key, Value};
use thiserror::Error;

use super::JOB_RETENTION_MS;
use super::records::pages::ServingNode;
use super::store::JobMutationError;
use super::store::store_call::RowBatch;
use super::submit::{SubmitJobError, SubmitJobResult};
use super::submit_local::HandedJobOperation;
use crate::distribution::imports::{Kept, KeptStart};

/// Handed over imports could not start, or their row could not follow.
#[derive(Debug, Error, PartialEq)]
#[error("{0}")]
pub(crate) struct StartKeptError(String);

impl From<IoError> for StartKeptError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

#[derive(Debug, PartialEq)]
enum StartStep {
    Handed,
    Kept,
    Dropped,
}

/// Runs each fetched job here as its own job from its handed position. A started job leaves
/// the row at once, so a later pass starts only what is left.
#[derive(Debug, PartialEq)]
pub(crate) struct StartKeptOperation {
    key: Key,
    kept: Kept,
    bucket: BucketIdentity,
    serving: ServingNode,
    now_ms: u64,
    step: StartStep,
}

type StartNext = Result<Next<()>, StartKeptError>;

impl StartKeptOperation {
    pub(crate) fn new(start: KeptStart, serving: ServingNode, now_ms: u64) -> Self {
        Self {
            key: Key::from(start.key),
            kept: start.kept,
            bucket: start.bucket.identity(),
            serving,
            now_ms,
            step: StartStep::Handed,
        }
    }

    fn next_job(&mut self) -> StartNext {
        let Some((local, _)) = self.serving else {
            return Ok(Next::Done(()));
        };
        let Some((spec, resume)) = self.kept.ready.first().cloned() else {
            self.step = StartStep::Dropped;
            let row = (NODE_STATE_KEYSPACE.to_string(), self.key.clone());
            return Ok(Next::Call(IoCall::child(RowBatch::delete(vec![row]))));
        };
        let spec = StagingJobSpec {
            bucket_identity: self.bucket,
            ..spec
        };
        self.step = StartStep::Handed;
        let handed = HandedJobOperation::new(
            spec,
            resume,
            local,
            JOB_RETENTION_MS,
            (self.serving, self.now_ms),
        );
        Ok(Next::Call(IoCall::child(handed)))
    }

    fn keep_rest(&mut self) -> StartNext {
        self.kept.ready.remove(0);
        let value =
            postcard::to_allocvec(&self.kept).map_err(|error| StartKeptError(error.to_string()))?;
        let row = (
            NODE_STATE_KEYSPACE.to_string(),
            self.key.clone(),
            Value::from(value),
        );
        self.step = StartStep::Kept;
        let write = RowBatch::write(Ok(vec![row]));
        Ok(Next::Call(IoCall::child(write)))
    }
}

fn row_failed(what: &str, error: JobMutationError) -> StartKeptError {
    StartKeptError(format!("handed over imports not {what}: {error}"))
}

impl Operation for StartKeptOperation {
    type Output = ();
    type Error = StartKeptError;

    fn first(&mut self) -> StartNext {
        self.next_job()
    }

    fn next(&mut self, result: IoResult) -> StartNext {
        match self.step {
            StartStep::Handed => {
                result
                    .child::<SubmitJobResult, SubmitJobError>()?
                    .map_err(|error| StartKeptError(error.to_string()))?;
                self.keep_rest()
            }
            StartStep::Kept => {
                let kept = result.child::<(), JobMutationError>()?;
                kept.map_err(|error| row_failed("kept", error))?;
                self.next_job()
            }
            StartStep::Dropped => {
                let dropped = result.child::<(), JobMutationError>()?;
                dropped.map_err(|error| row_failed("dropped", error))?;
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::id::NodeId;
    use aruna_core::structs::execution::job::{JobId, StagingJobCheckpoint};
    use aruna_core::structs::execution::staging::StagingStrategy;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    use super::*;

    const REALM: RealmId = RealmId([1u8; 32]);

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn identity() -> BucketIdentity {
        (Ulid::nil(), SystemTime::UNIX_EPOCH, UserId::nil(REALM))
    }

    fn spec() -> StagingJobSpec {
        StagingJobSpec {
            auth_context: AuthContext {
                user_id: UserId::local(Ulid::from_bytes([2u8; 16]), REALM),
                realm_id: REALM,
                path_restrictions: None,
                session: None,
            },
            group_id: Ulid::from_bytes([3u8; 16]),
            node_id: node(4),
            connector_id: Ulid::from_bytes([5u8; 16]),
            bucket: "bucket".to_string(),
            bucket_identity: identity(),
            strategy: StagingStrategy::Reference,
            items: Vec::new(),
            prefixes: Vec::new(),
            checksum_manifest: None,
            table: None,
            mirrors: Vec::new(),
        }
    }

    /// The row with `jobs` ready to start; `Kept` encodes as its fields in order.
    fn kept(jobs: usize) -> Kept {
        let ready: Vec<(StagingJobSpec, Option<StagingJobCheckpoint>)> =
            (0..jobs).map(|_| (spec(), None)).collect();
        let row = (node(6), Vec::<JobId>::new(), ready);
        let bytes = postcard::to_allocvec(&row).expect("the row encodes");
        postcard::from_bytes(&bytes).expect("the row decodes")
    }

    fn start(jobs: usize, serving: ServingNode) -> StartKeptOperation {
        StartKeptOperation {
            key: Key::from(vec![7u8]),
            kept: kept(jobs),
            bucket: identity(),
            serving,
            now_ms: 5,
            step: StartStep::Handed,
        }
    }

    fn serving() -> ServingNode {
        Some((node(1), REALM))
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn started() -> IoResult {
        child(Ok::<_, SubmitJobError>(SubmitJobResult {
            job_id: JobId::from_bytes([8u8; 16]),
            created: true,
        }))
    }

    fn stored() -> IoResult {
        child(Ok::<_, JobMutationError>(()))
    }

    fn is_child(next: &StartNext) -> bool {
        matches!(next, Ok(Next::Call(IoCall::Child(_))))
    }

    // A node without a network has nothing to start and keeps the row.
    #[test]
    fn offline_keeps_row() {
        let mut operation = start(1, None);
        assert_eq!(operation.first(), Ok(Next::Done(())));
    }

    // Each started job leaves the row before the next one starts, and the empty row goes.
    #[test]
    fn jobs_leave_row() {
        let mut operation = start(2, serving());
        assert!(is_child(&operation.first()));
        assert!(is_child(&operation.next(started())));
        assert_eq!(operation.step, StartStep::Kept);
        assert_eq!(operation.kept.ready.len(), 1);
        assert!(is_child(&operation.next(stored())));
        assert_eq!(operation.step, StartStep::Handed);
        assert!(is_child(&operation.next(started())));
        assert!(is_child(&operation.next(stored())));
        assert_eq!(operation.step, StartStep::Dropped);
        assert_eq!(operation.next(stored()), Ok(Next::Done(())));
    }

    // A job that cannot start stays in the row for the next pass.
    #[test]
    fn failed_start_stays() {
        let mut operation = start(1, serving());
        operation.first().unwrap();
        let refused = child(Err::<SubmitJobResult, _>(SubmitJobError::DocumentMissing));
        assert!(operation.next(refused).is_err());
        assert_eq!(operation.kept.ready.len(), 1);
    }

    // A row that cannot follow ends the pass with what could not be written.
    #[test]
    fn row_failures_named() {
        let failed = || child(Err::<(), _>(JobMutationError::Storage("down".to_string())));
        let mut operation = start(1, serving());
        operation.first().unwrap();
        operation.next(started()).unwrap();
        let error = operation.next(failed()).unwrap_err();
        assert!(error.0.starts_with("handed over imports not kept"));

        let mut operation = start(0, serving());
        assert!(is_child(&operation.first()));
        let error = operation.next(failed()).unwrap_err();
        assert!(error.0.starts_with("handed over imports not dropped"));
    }
}
