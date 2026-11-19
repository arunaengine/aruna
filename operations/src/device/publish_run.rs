//! Publishes one finished local run: a realm node pulls each output, then its crate is written.
//! Outputs move by exact version through the synced-folder pull, so a retried publish replays.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashSet, VecDeque};

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::RUN_PUBLISH_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::rows::{done, read_row, value};
use aruna_core::structs::execution::job::{
    JobError, JobId, JobPayload, JobRecord, JobResultPayload, LocalCrate, OutputObject,
    RunCrateStatus, run_crate_key,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{NodeRealm, RealmConfigDocument, RealmId};
use aruna_core::structs::storage::data::identity::parse_hash;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::{RemoteBinding, SyncPull, SyncPullAck, SyncRefusal};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::device::sync::pull::ForwardPullOperation;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::transport::{no_peer_transport, peer_answer, peer_call};
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::store::row_calls;
use crate::jobs::workflow::run_crate::RecordCrateOperation;
use crate::metadata::protocol::MetadataTransportMessage;

#[derive(Clone, Debug, PartialEq)]
pub struct PublishRunInput {
    pub job_id: JobId,
    /// The user this device is enrolled for; only their runs are published.
    pub owner: UserId,
    /// This device, which ran the job and serves its outputs.
    pub node_id: NodeId,
    /// Realm node, bucket and prefix the outputs are copied into.
    pub target: RemoteBinding,
}

#[derive(Debug, Error, PartialEq)]
pub enum PublishRunError {
    #[error("no such local run")]
    NotFound,
    #[error("the run has not finished")]
    NotFinished,
    #[error("node {0} is not a realm server")]
    NotRealmNode(NodeId),
    #[error("the realm refused {key}: {refusal:?}")]
    Refused { key: String, refusal: SyncRefusal },
    #[error("the realm node is unreachable: {0}")]
    Unreachable(String),
    #[error("this run is already being published to another target")]
    TargetChanged,
    #[error("two outputs of the run would land on {0}")]
    Collision(String),
    #[error("the run crate was not written: {0}")]
    Crate(String),
    #[error("device state unavailable: {0}")]
    Unavailable(String),
}

impl From<IoError> for PublishRunError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

fn unavailable(error: impl std::fmt::Display) -> PublishRunError {
    PublishRunError::Unavailable(error.to_string())
}

/// The realm copies of the run's outputs and the crate outcome.
#[derive(Debug, PartialEq)]
pub struct PublishedRun {
    pub outputs: Vec<OutputObject>,
    pub run_crate: RunCrateStatus,
}

/// The target one publish froze before its first copy, and the copies once all are made.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FrozenPublish {
    target: RemoteBinding,
    copies: Option<Vec<OutputObject>>,
}

/// What the pending call of publishing a run answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishStep {
    Record,
    Status,
    Config,
    Frozen,
    Bucket,
    Freeze,
    Pull,
    StoreCopies,
    Crate,
    Written,
}

/// Copies a finished local run's outputs into a realm bucket, then writes its crate there.
/// Every output is pulled before the crate is written, so the crate names the realm copies.
pub fn publish_run(ids: NodeRealm, input: PublishRunInput) -> PublishRunOperation {
    PublishRunOperation {
        ids,
        input,
        step: PublishStep::Record,
        record: None,
        status: None,
        auth: None,
        frozen: None,
        pending: VecDeque::new(),
        copies: Vec::new(),
    }
}

#[derive(Debug, PartialEq)]
pub struct PublishRunOperation {
    ids: NodeRealm,
    input: PublishRunInput,
    step: PublishStep,
    record: Option<JobRecord>,
    status: Option<RunCrateStatus>,
    auth: Option<AuthContext>,
    frozen: Option<FrozenPublish>,
    pending: VecDeque<(OutputObject, String)>,
    copies: Vec<OutputObject>,
}

type PublishTurn = Result<Next<PublishedRun>, PublishRunError>;

impl PublishRunOperation {
    fn frozen_key(&self) -> aruna_core::types::Key {
        run_crate_key(self.input.job_id)
    }

    fn record(&self) -> Result<&JobRecord, IoError> {
        self.record.as_ref().ok_or(IoError::Unfinished)
    }

    fn auth(&self) -> Result<&AuthContext, IoError> {
        self.auth.as_ref().ok_or(IoError::Unfinished)
    }

    fn checked(&mut self, record: Option<JobRecord>) -> PublishTurn {
        let record = record.ok_or(PublishRunError::NotFound)?;
        if !matches!(record.payload, JobPayload::Execution(_))
            || record.created_by != self.input.owner
            || record.owner_node_id != self.input.node_id
        {
            return Err(PublishRunError::NotFound);
        }
        if !record.state.is_terminal() {
            return Err(PublishRunError::NotFinished);
        }
        self.record = Some(record);
        self.step = PublishStep::Status;
        let status = row_calls::read_crate_status(self.input.job_id);
        Ok(Next::Call(IoCall::child(status)))
    }

    fn with_status(&mut self, status: Option<RunCrateStatus>) -> PublishTurn {
        match status {
            // A notebook session owes no crate and is no run to publish.
            None => return Err(PublishRunError::NotFound),
            Some(RunCrateStatus::Pending) => return Err(PublishRunError::NotFinished),
            Some(status) => self.status = Some(status),
        }
        let realm_id = self.realm_id();
        self.step = PublishStep::Config;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn realm_id(&self) -> RealmId {
        self.ids
            .map_or(self.input.owner.realm_id, |(_, realm_id)| realm_id)
    }

    fn configured(&mut self, config: RealmConfigDocument) -> PublishTurn {
        let eligible = config.sync_eligible_nodes().map_err(unavailable)?;
        let target = self.input.target.node_id;
        if !eligible.contains(&target) {
            return Err(PublishRunError::NotRealmNode(target));
        }
        self.auth = Some(AuthContext {
            user_id: self.input.owner,
            realm_id: self.realm_id(),
            path_restrictions: None,
            session: None,
        });
        self.step = PublishStep::Frozen;
        Ok(Next::Call(read_row(
            RUN_PUBLISH_KEYSPACE,
            self.frozen_key(),
        )))
    }

    fn with_frozen(&mut self, frozen: Option<FrozenPublish>) -> PublishTurn {
        if (frozen.as_ref()).is_some_and(|frozen| frozen.target != self.input.target) {
            return Err(PublishRunError::TargetChanged);
        }
        // A repeated publish answers with the copies already made.
        if let Some(copies) = frozen.and_then(|frozen| frozen.copies) {
            self.copies = copies;
            return self.write_crate();
        }
        let outputs = match &self.record()?.result {
            Some(JobResultPayload::Execution { outputs, .. }) => outputs.clone(),
            _ => Vec::new(),
        };
        let keys = target_keys(&self.input.target, &outputs)?;
        self.pending = outputs.into_iter().zip(keys).collect();
        self.step = PublishStep::Bucket;
        // Asks the realm node whether the owner may write the target, whatever the run produced.
        let target = &self.input.target;
        let message = MetadataTransportMessage::ForwardCheckBucket {
            auth_token: AuthToken::internal(self.auth()?.clone()),
            bucket: target.bucket.clone(),
            prefix: target.prefix.trim_end_matches('/').to_string(),
        };
        let call = peer_call(target.node_id, &message).map_err(PublishRunError::Unreachable)?;
        Ok(Next::Call(call))
    }

    fn bucket_checked(&mut self, result: IoResult) -> PublishTurn {
        if no_peer_transport(&result) {
            return Err(unavailable("metadata handle"));
        }
        let target = &self.input.target;
        match peer_answer(result)?.map_err(PublishRunError::Unreachable)? {
            MetadataTransportMessage::ForwardedBucketChecked { result: Ok(()) } => {}
            MetadataTransportMessage::ForwardedBucketChecked {
                result: Err(refusal),
            } => {
                return Err(PublishRunError::Refused {
                    key: target.bucket.clone(),
                    refusal,
                });
            }
            other => {
                let kind = crate::metadata::transport_message_kind(&other);
                return Err(PublishRunError::Unreachable(kind.to_string()));
            }
        }
        let frozen = FrozenPublish {
            target: target.clone(),
            copies: None,
        };
        self.store_frozen(frozen, PublishStep::Freeze)
    }

    fn store_frozen(&mut self, frozen: FrozenPublish, step: PublishStep) -> PublishTurn {
        let bytes = postcard::to_allocvec(&frozen).map_err(unavailable)?;
        let row = (
            RUN_PUBLISH_KEYSPACE.to_string(),
            self.frozen_key(),
            bytes.into(),
        );
        self.frozen = Some(frozen);
        self.step = step;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    /// Asks the realm node to pull the next output version, or freezes the copies once all came.
    fn next_pull(&mut self) -> PublishTurn {
        let Some((output, key)) = self.pending.front() else {
            let mut frozen = self.frozen.take().ok_or(IoError::Unfinished)?;
            frozen.copies = Some(self.copies.clone());
            return self.store_frozen(frozen, PublishStep::StoreCopies);
        };
        let auth = self.auth()?;
        let source = VersionedObjectArn::new(
            auth.realm_id,
            output.node_id,
            output.bucket.clone(),
            output.key.clone(),
            output.version_id,
        )
        .map_err(unavailable)?;
        let target = &self.input.target;
        let pull = SyncPull {
            source,
            blake3: output.digest.as_deref().and_then(parse_hash),
            size: output.size,
            target_bucket: target.bucket.clone(),
            target_key: key.clone(),
            deleted: false,
        };
        let token = AuthToken::internal(auth.clone());
        let forward = ForwardPullOperation::new(target.node_id, token, pull);
        self.step = PublishStep::Pull;
        Ok(Next::Call(IoCall::child(forward)))
    }

    fn pulled(&mut self, answer: Result<SyncPullAck, SyncRefusal>) -> PublishTurn {
        let (output, key) = self.pending.pop_front().ok_or(IoError::Unfinished)?;
        match answer {
            Ok(ack) => {
                let target = &self.input.target;
                self.copies.push(OutputObject {
                    node_id: target.node_id,
                    bucket: target.bucket.clone(),
                    key,
                    version_id: ack.version_id,
                    ..output
                });
                self.next_pull()
            }
            Err(SyncRefusal::Unavailable) => Err(PublishRunError::Unreachable(
                "the realm node did not finish the copy".to_string(),
            )),
            Err(refusal) => Err(PublishRunError::Refused {
                key: output.key,
                refusal,
            }),
        }
    }

    fn write_crate(&mut self) -> PublishTurn {
        let status = self.status.as_ref().ok_or(IoError::Unfinished)?;
        if matches!(status, RunCrateStatus::Written { .. }) {
            let run_crate = self.status.take().ok_or(IoError::Unfinished)?;
            return self.finish(run_crate);
        }
        let saved_id = match status {
            RunCrateStatus::Minted { document_id } => Some(*document_id),
            _ => None,
        };
        let mut published = self.record()?.clone();
        if let Some(JobResultPayload::Execution { outputs, .. }) = &mut published.result {
            outputs.clone_from(&self.copies);
        }
        let JobPayload::Execution(spec) = &self.record()?.payload else {
            return Err(PublishRunError::NotFound);
        };
        let job_id = self.input.job_id;
        let write = RecordCrateOperation::new(job_id, &published, spec, saved_id, self.ids);
        self.step = PublishStep::Crate;
        Ok(Next::Call(IoCall::child(write)))
    }

    fn finish(&mut self, run_crate: RunCrateStatus) -> PublishTurn {
        Ok(Next::Done(PublishedRun {
            outputs: std::mem::take(&mut self.copies),
            run_crate,
        }))
    }
}

impl Operation for PublishRunOperation {
    type Output = PublishedRun;
    type Error = PublishRunError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::RunPublish)
    }

    fn first(&mut self) -> PublishTurn {
        let read = row_calls::read_job_record(self.input.job_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> PublishTurn {
        match self.step {
            PublishStep::Record => {
                let record = result.child::<Option<JobRecord>, JobMutationError>()?;
                self.checked(record.map_err(unavailable)?)
            }
            PublishStep::Status => {
                let status = result.child::<Option<RunCrateStatus>, JobMutationError>()?;
                self.with_status(status.map_err(unavailable)?)
            }
            PublishStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                self.configured(config.map_err(unavailable)?)
            }
            PublishStep::Frozen => {
                let frozen = value(result)?
                    .map(|bytes| postcard::from_bytes(&bytes))
                    .transpose()
                    .map_err(unavailable)?;
                self.with_frozen(frozen)
            }
            PublishStep::Bucket => self.bucket_checked(result),
            PublishStep::Freeze => {
                done(result)?;
                self.next_pull()
            }
            PublishStep::Pull => self.pulled(result.child::<SyncPullAck, SyncRefusal>()?),
            PublishStep::StoreCopies => {
                done(result)?;
                self.write_crate()
            }
            PublishStep::Crate => {
                let written = result.child::<String, JobError>()?;
                written.map_err(|error| PublishRunError::Crate(error.message))?;
                self.step = PublishStep::Written;
                let status = row_calls::read_crate_status(self.input.job_id);
                Ok(Next::Call(IoCall::child(status)))
            }
            PublishStep::Written => {
                let status = result.child::<Option<RunCrateStatus>, JobMutationError>()?;
                let status = status.map_err(unavailable)?;
                self.finish(status.ok_or(PublishRunError::NotFound)?)
            }
        }
    }
}

/// The crate this device keeps for one of the owner's local runs. The owner is kept with the
/// crate, so it stays readable after the job is pruned.
pub fn local_run_crate(job_id: JobId, owner: UserId) -> LocalRunOperation {
    LocalRunOperation { job_id, owner }
}

#[derive(Debug, PartialEq)]
pub struct LocalRunOperation {
    job_id: JobId,
    owner: UserId,
}

impl Operation for LocalRunOperation {
    type Output = serde_json::Value;
    type Error = PublishRunError;

    fn first(&mut self) -> Result<Next<serde_json::Value>, PublishRunError> {
        let read = row_calls::read_local_crate(self.job_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<serde_json::Value>, PublishRunError> {
        let local = result.child::<Option<LocalCrate>, JobMutationError>()?;
        let local = (local.map_err(unavailable)?)
            .filter(|local| local.owner == self.owner)
            .ok_or(PublishRunError::NotFound)?;
        Ok(Next::Done(
            serde_json::from_str(&local.jsonld).map_err(unavailable)?,
        ))
    }
}

/// The realm key of every output, `<prefix>/<source bucket>/<key>`, so outputs of different
/// buckets never meet. Two outputs on one key are refused before anything is copied.
fn target_keys(
    target: &RemoteBinding,
    outputs: &[OutputObject],
) -> Result<Vec<String>, PublishRunError> {
    let mut seen = HashSet::new();
    outputs
        .iter()
        .map(|output| {
            let key = target.remote_key(&format!("{}/{}", output.bucket, output.key));
            match seen.insert(key.clone()) {
                true => Ok(key),
                false => Err(PublishRunError::Collision(key)),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn output(bucket: &str, key: &str) -> OutputObject {
        OutputObject {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: Ulid::from(2u128),
            execution_id: Ulid::from(3u128),
            container_path: String::new(),
            size: 1,
            digest: None,
        }
    }

    // One file name in two buckets lands on two keys; the same key twice is refused.
    #[test]
    fn keeps_buckets_apart() {
        let target = RemoteBinding {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            bucket: "lab-data".to_string(),
            prefix: "runs/one".to_string(),
        };
        let keys = target_keys(
            &target,
            &[output("a", "report.txt"), output("b", "report.txt")],
        )
        .expect("distinct buckets map apart");
        assert_eq!(keys, ["runs/one/a/report.txt", "runs/one/b/report.txt"]);
        assert!(matches!(
            target_keys(
                &target,
                &[output("a", "report.txt"), output("a", "report.txt")]
            ),
            Err(PublishRunError::Collision(key)) if key == "runs/one/a/report.txt"
        ));
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    fn input() -> PublishRunInput {
        let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([7; 32]);
        PublishRunInput {
            job_id: JobId::from_bytes([4; 16]),
            owner: UserId::local(Ulid::from(9u128), realm_id),
            node_id: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
            target: RemoteBinding {
                node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
                bucket: "lab-data".to_string(),
                prefix: "runs/one".to_string(),
            },
        }
    }

    // Publishes run one at a time, so a repeated request cannot mint a second crate.
    #[test]
    fn holds_publish_scope() {
        assert_eq!(
            publish_run(None, input()).hold_scope(),
            Some(HoldScope::RunPublish)
        );
    }

    // A node that cannot ask the realm node is unavailable; the realm node was not reached.
    #[test]
    fn missing_handle_unavailable() {
        use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
        let mut publish = publish_run(None, input());
        publish.step = PublishStep::Bucket;
        let missing = aruna_core::metadata::MetadataError::HandleMissing;
        let answer = MetadataAnswer {
            peer: input().target.node_id,
            elapsed: std::time::Duration::ZERO,
            result: Err(PeerRequestError::Failed(missing)),
        };
        let event = Event::Net(NetEvent::MetadataAnswer(Box::new(answer)));
        let expected = PublishRunError::Unavailable("metadata handle".to_string());
        let answer = publish.next(IoResult::Event(Box::new(event)));
        assert_eq!(answer, Err(expected));
    }

    // A run this node does not know is not found.
    #[test]
    fn unknown_run_missing() {
        let mut publish = publish_run(None, input());
        publish.first().unwrap();
        let answer = publish.next(child(Ok::<Option<JobRecord>, JobMutationError>(None)));
        assert_eq!(answer, Err(PublishRunError::NotFound));
    }

    // A session without a crate is no run to publish, and a pending crate has to finish first.
    #[test]
    fn status_gates_publish() {
        let mut none = publish_run(None, input());
        none.step = PublishStep::Status;
        let answer = none.next(child(Ok::<Option<RunCrateStatus>, JobMutationError>(None)));
        assert_eq!(answer, Err(PublishRunError::NotFound));
        let mut pending = publish_run(None, input());
        pending.step = PublishStep::Status;
        let status = Some(RunCrateStatus::Pending);
        let answer = pending.next(child(Ok::<_, JobMutationError>(status)));
        assert_eq!(answer, Err(PublishRunError::NotFinished));
    }

    // Another target than the frozen one is refused.
    #[test]
    fn frozen_target_holds() {
        let mut publish = publish_run(None, input());
        publish.step = PublishStep::Frozen;
        let mut other = input().target;
        other.prefix = "runs/other".to_string();
        let frozen = FrozenPublish {
            target: other,
            copies: None,
        };
        let row = postcard::to_allocvec(&frozen).unwrap();
        let answer = publish.next(IoResult::Values(vec![Some(row.into())]));
        assert_eq!(answer, Err(PublishRunError::TargetChanged));
    }

    // A refused pull names the output it was refused for.
    #[test]
    fn refused_pull_named() {
        let mut publish = publish_run(None, input());
        let copy = output("a", "report.txt");
        publish
            .pending
            .push_back((copy, "runs/one/a/report.txt".to_string()));
        publish.step = PublishStep::Pull;
        let refused = child(Err::<SyncPullAck, _>(SyncRefusal::Forbidden));
        let expected = PublishRunError::Refused {
            key: "report.txt".to_string(),
            refusal: SyncRefusal::Forbidden,
        };
        assert_eq!(publish.next(refused), Err(expected));
    }

    // A crate kept for someone else is not found.
    #[test]
    fn foreign_crate_hidden() {
        let mut read = local_run_crate(input().job_id, input().owner);
        read.first().unwrap();
        let stranger = UserId::local(Ulid::from(10u128), input().owner.realm_id);
        let kept = LocalCrate {
            owner: stranger,
            jsonld: "{}".to_string(),
        };
        let answer = read.next(child(Ok::<_, JobMutationError>(Some(kept))));
        assert_eq!(answer, Err(PublishRunError::NotFound));
    }
}
