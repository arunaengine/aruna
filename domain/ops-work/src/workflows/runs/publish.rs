//! Publishes finished local device runs — single execution jobs and workflow runs — to a realm
//! bucket and delegates their RO-Crate write and local crate reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::RUN_PUBLISH_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::metadata::protocol::MetadataTransportMessage;
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::rows::read_row;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::input::JobPayload;
use aruna_core::structs::execution::job::local::{JobRecord, OutputObject, RunCrateStatus};
use aruna_core::structs::execution::workflow_run::WorkflowRunRecord;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::{NodeRealm, RealmConfigDocument, RealmId};
use aruna_core::structs::synced_folder::{RemoteBinding, SyncPullAck, SyncRefusal};
use aruna_core::types::Key;
use aruna_core::user::id::UserId;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_ops_identity::forward::transport::peer_call;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::device_crates::{
    WriteWorkflowCrate, mint_crate_call, minted_doc_id, unavailable, workflow_outputs,
};
use super::provenance::status;
use super::{read_run, run_answer};
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::store::row_calls;
use crate::jobs::workflow::publish_run::{
    PublishRunError, PublishRunInput, PublishedRun, check_bucket, copy_pulled, publish_run,
    pull_call, target_keys,
};

pub use super::device_crates::{DeviceCrateOperation, local_device_crate};

/// Input for publishing either a local execution job or a local workflow run.
#[derive(Clone, Debug, PartialEq)]
pub struct DevicePublishInput {
    pub id: Ulid,
    pub owner: UserId,
    pub node_id: NodeId,
    pub target: RemoteBinding,
}

fn workflow_publish_key(run_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(3 + 16);
    bytes.extend_from_slice(b"wf/");
    bytes.extend_from_slice(&run_id.to_bytes());
    Key::from(bytes)
}

/// Frozen target, copies and minted crate id for one local workflow run publish.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FrozenWorkflow {
    target: RemoteBinding,
    copies: Option<Vec<OutputObject>>,
    document_id: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
enum PublishStep {
    Job(JobId),
    Delegate,
    Workflow(Ulid),
    Config,
    Frozen,
    Bucket,
    Freeze,
    Pull,
    StoreCopies,
    Mint,
    FreezeDoc(Ulid),
    WriteCrate,
}

/// Publishes one finished local execution or workflow run to a realm bucket, then writes its
/// RO-Crate in the realm. Accepts an execution job id, a workflow parent job id, or a run id.
pub fn publish_device_run(ids: NodeRealm, input: DevicePublishInput) -> PublishDeviceOperation {
    PublishDeviceOperation {
        ids,
        input,
        step: PublishStep::Delegate,
        record: None,
        auth: None,
        frozen: None,
        pending: VecDeque::new(),
        copies: Vec::new(),
    }
}

#[derive(Debug, PartialEq)]
pub struct PublishDeviceOperation {
    ids: NodeRealm,
    input: DevicePublishInput,
    step: PublishStep,
    record: Option<WorkflowRunRecord>,
    auth: Option<AuthContext>,
    frozen: Option<FrozenWorkflow>,
    pending: VecDeque<(OutputObject, String)>,
    copies: Vec<OutputObject>,
}

type PublishTurn = Result<Next<PublishedRun>, PublishRunError>;

impl PublishDeviceOperation {
    fn realm_id(&self) -> RealmId {
        self.ids
            .map_or(self.input.owner.realm_id, |(_, realm_id)| realm_id)
    }

    fn record(&self) -> Result<&WorkflowRunRecord, IoError> {
        self.record.as_ref().ok_or(IoError::Unfinished)
    }

    fn auth(&self) -> Result<&AuthContext, IoError> {
        self.auth.as_ref().ok_or(IoError::Unfinished)
    }

    fn frozen_key(&self) -> Result<Key, IoError> {
        Ok(workflow_publish_key(self.record()?.run_id))
    }

    fn actor(&self) -> Actor {
        Actor {
            node_id: self.input.node_id,
            user_id: self.input.owner,
            realm_id: self.realm_id(),
        }
    }

    fn after_job(&mut self, job_id: JobId, job: Option<JobRecord>) -> PublishTurn {
        match job {
            Some(JobRecord {
                payload: JobPayload::Execution(_),
                ..
            }) => {
                self.step = PublishStep::Delegate;
                let delegate = publish_run(
                    self.ids,
                    PublishRunInput {
                        job_id,
                        owner: self.input.owner,
                        node_id: self.input.node_id,
                        target: self.input.target.clone(),
                    },
                );
                Ok(Next::Call(IoCall::child(delegate)))
            }
            Some(JobRecord {
                payload: JobPayload::WorkflowRun(spec),
                ..
            }) => {
                self.step = PublishStep::Workflow(spec.run_id);
                Ok(Next::Call(read_run(spec.run_id)))
            }
            None => {
                self.step = PublishStep::Workflow(self.input.id);
                Ok(Next::Call(read_run(self.input.id)))
            }
            Some(_) => Err(PublishRunError::NotFound),
        }
    }

    fn checked(&mut self, record: Option<WorkflowRunRecord>) -> PublishTurn {
        let record = record.ok_or(PublishRunError::NotFound)?;
        if !record.local_only
            || record.created_by != self.input.owner
            || record.node_id != self.input.node_id
        {
            return Err(PublishRunError::NotFound);
        }
        if !record.state.is_terminal()
            || (record.provenance_crate && record.provenance_status.is_none())
        {
            return Err(PublishRunError::NotFinished);
        }
        if record.protected {
            return Err(PublishRunError::Refused {
                key: record.run_id.to_string(),
                refusal: SyncRefusal::Invalid(
                    "protected runs cannot be published to a realm bucket".to_string(),
                ),
            });
        }
        self.record = Some(record);
        self.step = PublishStep::Config;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id(),
        ))))
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
        let key = self.frozen_key()?;
        self.step = PublishStep::Frozen;
        Ok(Next::Call(read_row(RUN_PUBLISH_KEYSPACE, key)))
    }

    fn with_frozen(&mut self, frozen: Option<FrozenWorkflow>) -> PublishTurn {
        if (frozen.as_ref()).is_some_and(|frozen| frozen.target != self.input.target) {
            return Err(PublishRunError::TargetChanged);
        }
        if let Some(frozen) = frozen
            && let Some(copies) = frozen.copies.clone()
        {
            self.copies = copies;
            self.frozen = Some(frozen);
            return self.write_crate();
        }
        let outputs = workflow_outputs(self.record()?)?;
        let keys = target_keys(&self.input.target, &outputs)?;
        self.pending = outputs.into_iter().zip(keys).collect();
        self.step = PublishStep::Bucket;
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
        check_bucket(&self.input.target, result)?;
        let frozen = FrozenWorkflow {
            target: self.input.target.clone(),
            copies: None,
            document_id: None,
        };
        self.store_frozen(frozen, PublishStep::Freeze)
    }

    fn store_frozen(&mut self, frozen: FrozenWorkflow, step: PublishStep) -> PublishTurn {
        let bytes = postcard::to_allocvec(&frozen).map_err(unavailable)?;
        let row = (
            RUN_PUBLISH_KEYSPACE.to_string(),
            self.frozen_key()?,
            bytes.into(),
        );
        self.frozen = Some(frozen);
        self.step = step;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn next_pull(&mut self) -> PublishTurn {
        match self.pending.front() {
            Some((output, key)) => {
                let call = pull_call(self.auth()?, &self.input.target, output, key)?;
                self.step = PublishStep::Pull;
                Ok(Next::Call(call))
            }
            None => self.freeze_copies(),
        }
    }

    fn freeze_copies(&mut self) -> PublishTurn {
        let mut frozen = self.frozen.take().ok_or(IoError::Unfinished)?;
        frozen.copies = Some(self.copies.clone());
        self.store_frozen(frozen, PublishStep::StoreCopies)
    }

    fn pulled(&mut self, answer: Result<SyncPullAck, SyncRefusal>) -> PublishTurn {
        let (output, key) = self.pending.pop_front().ok_or(IoError::Unfinished)?;
        let copy = copy_pulled(&self.input.target, output, key, answer)?;
        self.copies.push(copy);
        self.next_pull()
    }

    fn write_crate(&mut self) -> PublishTurn {
        let record = self.record()?;
        if record.provenance_status.as_deref() == Some(status::WRITTEN)
            && let Some(doc_id) = record.provenance_document_id
        {
            return self.finish(RunCrateStatus::Written {
                resource: doc_id.to_string(),
            });
        }
        if !record.provenance_crate {
            return self.finish(RunCrateStatus::Local);
        }
        if let Some(doc_id) = self.frozen.as_ref().and_then(|frozen| frozen.document_id) {
            return self.start_write(doc_id);
        }
        let call = mint_crate_call(self.actor(), record);
        self.step = PublishStep::Mint;
        Ok(Next::Call(call))
    }

    fn start_write(&mut self, doc_id: Ulid) -> PublishTurn {
        let write = WriteWorkflowCrate::new(
            self.record()?.clone(),
            self.copies.clone(),
            (self.input.node_id, self.realm_id()),
            self.auth()?.clone(),
            doc_id,
        );
        self.step = PublishStep::WriteCrate;
        Ok(Next::Call(IoCall::child(write)))
    }

    fn finish(&mut self, run_crate: RunCrateStatus) -> PublishTurn {
        Ok(Next::Done(PublishedRun {
            outputs: std::mem::take(&mut self.copies),
            run_crate,
        }))
    }
}

impl Operation for PublishDeviceOperation {
    type Output = PublishedRun;
    type Error = PublishRunError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::RunPublish)
    }

    fn first(&mut self) -> PublishTurn {
        match JobId::try_from_bytes(self.input.id.to_bytes()) {
            Ok(job_id) => {
                self.step = PublishStep::Job(job_id);
                Ok(Next::Call(IoCall::child(row_calls::read_job_record(
                    job_id,
                ))))
            }
            Err(_) => {
                self.step = PublishStep::Workflow(self.input.id);
                Ok(Next::Call(read_run(self.input.id)))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> PublishTurn {
        match std::mem::replace(&mut self.step, PublishStep::Delegate) {
            PublishStep::Job(job_id) => {
                let record = result.child::<Option<JobRecord>, JobMutationError>()?;
                self.after_job(job_id, record.map_err(unavailable)?)
            }
            PublishStep::Delegate => Ok(Next::Done(
                result.child::<PublishedRun, PublishRunError>()??,
            )),
            PublishStep::Workflow(_) => {
                let record = run_answer(result).map_err(unavailable)?;
                self.checked(record)
            }
            PublishStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                self.configured(config.map_err(unavailable)?)
            }
            PublishStep::Frozen => {
                let frozen = result
                    .value()?
                    .map(|bytes| postcard::from_bytes(&bytes))
                    .transpose()
                    .map_err(unavailable)?;
                self.with_frozen(frozen)
            }
            PublishStep::Bucket => self.bucket_checked(result),
            PublishStep::Freeze => {
                result.done()?;
                self.next_pull()
            }
            PublishStep::Pull => self.pulled(result.child::<SyncPullAck, SyncRefusal>()?),
            PublishStep::StoreCopies => {
                result.done()?;
                self.write_crate()
            }
            PublishStep::Mint => {
                let doc_id = minted_doc_id(result)?;
                let mut frozen = self.frozen.take().ok_or(IoError::Unfinished)?;
                frozen.document_id = Some(doc_id);
                self.store_frozen(frozen, PublishStep::FreezeDoc(doc_id))
            }
            PublishStep::FreezeDoc(doc_id) => {
                result.done()?;
                self.start_write(doc_id)
            }
            PublishStep::WriteCrate => {
                let outcome = result.child::<RunCrateStatus, PublishRunError>()??;
                self.finish(outcome)
            }
        }
    }
}

