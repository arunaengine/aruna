//! Stages one stored input version on a target by reading it from a legal holder in turn.
//! The bytes are checked against the stored hash before the policy-gated workspace write.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::{InputSelection, InputSource, JobError, JobRecord};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use tracing::{debug, warn};
use ulid::Ulid;

use crate::blob::holders::{GetHoldersError, GetHoldersOperation};
use crate::jobs::records::pages::ServingNode;
use crate::jobs::workflow::workspace::LOCKED_INPUT;
use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::managed_read::ManagedReadOperation;
use crate::replication::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};

/// The staged bytes of one input plus the size the caller writes with.
#[derive(Debug, PartialEq)]
pub struct StagedInput {
    pub blob: BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>>,
    pub size: u64,
}

/// A staging failure; the job decides from it whether to retry, park or fail.
#[derive(Debug, PartialEq)]
pub struct StageError(pub JobError);

impl From<IoError> for StageError {
    fn from(error: IoError) -> Self {
        Self(JobError::retryable(format!(
            "input staging failed: {error}"
        )))
    }
}

/// Reads one exact input version from a legal holder. Holders are tried in canonical order and
/// a missing or corrupt source moves to the next one without ever changing the stored input.
#[derive(Debug, PartialEq)]
pub struct StageInputOperation {
    created_by: aruna_core::UserId,
    source_node: Option<NodeId>,
    /// The endpoint owning the bucket, key and version identity of this input.
    ingress: Option<NodeId>,
    bucket: String,
    key: String,
    version: Ulid,
    blake3: [u8; 32],
    serving: ServingNode,
    /// Holders still to try, the next one last.
    holders: Vec<NodeId>,
    /// Reads of the current holder still to try, the next one last.
    targets: Vec<BaoReadTarget>,
    holder: Option<NodeId>,
    last: Option<BaoReadError>,
}

type StageNext = Result<Next<StagedInput>, StageError>;

impl StageInputOperation {
    pub fn new(
        record: &JobRecord,
        input: &InputSelection,
        version: Ulid,
        blake3: [u8; 32],
        serving: ServingNode,
    ) -> Self {
        let InputSource::S3 { bucket, key, .. } = &input.source;
        // Only ingress owns the bucket/key/version identity; any other holder has a
        // registered copy, asked by content hash when the version is not its own.
        let ingress = record
            .captured_inputs
            .iter()
            .find(|captured| captured.destination_key == input.dest_key)
            .map(|captured| captured.source_node_id);
        Self {
            created_by: record.created_by,
            source_node: input.source_node_id,
            ingress,
            bucket: bucket.clone(),
            key: key.clone(),
            version,
            blake3,
            serving,
            holders: Vec::new(),
            targets: Vec::new(),
            holder: None,
            last: None,
        }
    }

    fn holders_found(&mut self, mut holders: Vec<NodeId>) -> StageNext {
        if let Some(source) = self.source_node {
            holders.push(source);
            holders.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
            holders.dedup();
        }
        if holders.is_empty() {
            let (bucket, key) = (&self.bucket, &self.key);
            return Err(StageError(JobError::retryable(format!(
                "no known holder for input {bucket}/{key}"
            ))));
        }
        holders.reverse();
        self.holders = holders;
        self.next_read()
    }

    fn next_read(&mut self) -> StageNext {
        let (_, realm_id) = self.serving.ok_or_else(no_network)?;
        loop {
            if let (Some(holder), Some(target)) = (self.holder, self.targets.pop()) {
                let request = BaoReadRequest {
                    auth_context: AuthContext {
                        user_id: self.created_by,
                        realm_id,
                        path_restrictions: None,
                        session: None,
                    },
                    realm_id,
                    target,
                    expected_blake3: Some(self.blake3),
                    metadata_only: false,
                    // The managed read fills in this node's own destination subject, so the
                    // source evaluates placement against where the bytes would land.
                    destination: None,
                    known_refs: Vec::new(),
                    range: None,
                };
                let read = ManagedReadOperation::new(holder, request);
                return Ok(Next::Call(IoCall::child(read)));
            }
            let Some(holder) = self.holders.pop() else {
                let last = self.last.take();
                return Err(StageError(stage_error(&self.bucket, &self.key, last)));
            };
            let source = VersionedObjectArn {
                realm_id,
                node_id: holder,
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                version: self.version,
            };
            let is_ingress = Some(holder) == self.ingress;
            self.targets = read_targets(source, is_ingress, self.blake3);
            self.targets.reverse();
            self.holder = Some(holder);
        }
    }
}

fn no_network() -> StageError {
    StageError(JobError::retryable("remote staging needs a net handle"))
}

impl Operation for StageInputOperation {
    type Output = StagedInput;
    type Error = StageError;

    fn first(&mut self) -> StageNext {
        let (local, realm_id) = self.serving.ok_or_else(no_network)?;
        let holders = GetHoldersOperation::new(self.blake3, realm_id, local);
        Ok(Next::Call(IoCall::child(holders)))
    }

    fn next(&mut self, result: IoResult) -> StageNext {
        let Some(holder) = self.holder else {
            let holders = result
                .child::<Vec<NodeId>, GetHoldersError>()?
                .map_err(|error| {
                    StageError(JobError::retryable(format!(
                        "input holder lookup failed: {error}"
                    )))
                })?;
            return self.holders_found(holders);
        };
        match result.child::<BaoReadOutput, BaoReadError>()? {
            Ok(BaoReadOutput::Stream { blob, size, .. }) => {
                debug!(peer = %holder, "Input staged from a remote holder");
                Ok(Next::Done(StagedInput { blob, size }))
            }
            Ok(BaoReadOutput::Metadata { .. }) => self.next_read(),
            Err(error) => {
                warn!(peer = %holder, error = %error, "Remote input read failed");
                self.last = Some(error);
                self.next_read()
            }
        }
    }
}

/// What one holder is asked for: its own stored version, and, when it is not
/// the endpoint that owns that object identity, the same bytes by content hash.
pub(crate) fn read_targets(
    source: VersionedObjectArn,
    is_ingress: bool,
    blake3: [u8; 32],
) -> Vec<BaoReadTarget> {
    let mut targets = vec![BaoReadTarget::ExactVersion(source)];
    if !is_ingress {
        targets.push(BaoReadTarget::Blake3(blake3));
    }
    targets
}

/// A denied or policy-blocked read fails this attempt permanently; anything
/// else may still succeed from another holder later.
pub(crate) fn stage_error(bucket: &str, key: &str, error: Option<BaoReadError>) -> JobError {
    let message = match &error {
        Some(error) => format!("staging {bucket}/{key} failed: {error}"),
        None => format!("staging {bucket}/{key} found no usable holder"),
    };
    match error {
        Some(BaoReadError::PolicyRequired { .. }) | Some(BaoReadError::NoDestination) => {
            JobError::permanent(message)
        }
        // The holder's key is locked: the job parks on it instead of spending an attempt.
        Some(BaoReadError::Refused(BaoReadRefusal::BucketLocked(_))) => {
            JobError::locked(format!("{LOCKED_INPUT}: {message}"))
        }
        _ => JobError::retryable(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{REALM, node, payload, user};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::job::{JobId, JobPayload};
    use futures_util::stream;

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn input() -> InputSelection {
        InputSelection {
            source: InputSource::S3 {
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                version_id: None,
            },
            source_node_id: Some(node(5)),
            dest_key: "input".to_string(),
            mode: aruna_core::structs::execution::job::InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        }
    }

    fn stage(serving: ServingNode) -> StageInputOperation {
        let record = JobRecord::new(
            JobId::from_bytes([1u8; 16]),
            JobPayload::Execution(payload()),
            user(),
            node(1),
            1,
            1,
            None,
        );
        StageInputOperation::new(&record, &input(), Ulid::nil(), [2u8; 32], serving)
    }

    fn holders(nodes: Vec<NodeId>) -> IoResult {
        child(Ok::<_, GetHoldersError>(nodes))
    }

    fn missing() -> IoResult {
        child(Err::<BaoReadOutput, _>(BaoReadError::NoDestination))
    }

    // Staging needs a network to find holders at all.
    #[test]
    fn network_required() {
        assert!(stage(None).first().is_err());
        let mut staged = stage(Some((node(1), REALM)));
        assert!(matches!(staged.first(), Ok(Next::Call(IoCall::Child(_)))));
    }

    // A holder that is not the ingress is asked for the version, then by content hash.
    #[test]
    fn holder_reads_both() {
        let mut staged = stage(Some((node(1), REALM)));
        staged.source_node = None;
        let found = staged.next(holders(vec![node(7)]));
        assert!(matches!(found, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(staged.targets.len(), 1);
        assert!(matches!(
            staged.next(missing()),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert!(staged.targets.is_empty());
    }

    // The bytes of the first answering holder are the result.
    #[test]
    fn stream_ends_staging() {
        let mut staged = stage(Some((node(1), REALM)));
        staged.holder = Some(node(7));
        let answer = child(Ok::<_, BaoReadError>(BaoReadOutput::Stream {
            blob: BackendStream(Box::pin(stream::empty())),
            size: 4,
            blake3: [2u8; 32],
            etag: None,
            hashes: Default::default(),
        }));
        assert!(matches!(
            staged.next(answer),
            Ok(Next::Done(StagedInput { size: 4, .. }))
        ));
    }

    // With every holder tried, the last failure decides how the attempt ends.
    #[test]
    fn exhausted_holders_fail() {
        let mut staged = stage(Some((node(1), REALM)));
        staged.holder = Some(node(7));
        let Err(StageError(error)) = staged.next(missing()) else {
            panic!("expected the staging error");
        };
        let expected = stage_error(
            &staged.bucket,
            &staged.key,
            Some(BaoReadError::NoDestination),
        );
        assert_eq!(error, expected);
    }
}
