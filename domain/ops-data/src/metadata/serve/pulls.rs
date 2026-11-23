//! Serves a device's synced-folder requests: admits a pull and runs it in the background while
//! the device polls, answers its polls, and lists heads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::synced_folder::{
    SyncPull, SyncPullAck, SyncPullStatus, SyncRefusal, SyncVersionPage,
};
use aruna_core::task::{PullEffect, PullEvent, PullProgress, TaskEffect, TaskEvent};
use tracing::warn;
use ulid::Ulid;

use super::ServeNode;
use crate::metadata::device_pull::{
    Admission, AdmitPullOperation, CommitPullOperation, ListHeadsOperation, PullCallerOperation,
    PullError, ReceiptOperation, SetListing, receipt_of, refused,
};
use aruna_core::metadata::protocol::MetadataTransportMessage as Message;
use aruna_core::structs::synced_folder::refusal_kind;

/// A forwarded synced-folder request with the owner's token.
#[derive(Debug, PartialEq)]
pub enum PullAsk {
    Pull(Box<SyncPull>),
    Status(Box<SyncPull>),
    List(SetListing),
}

#[derive(Debug, PartialEq)]
enum PullStage {
    Caller,
    Admit(Box<AuthContext>),
    Claim(Box<(AuthContext, BucketInfo)>),
    Started(u64),
    Registry,
    Receipt,
    Listed,
}

/// Answers one forwarded synced-folder request of `peer`.
#[derive(Debug, PartialEq)]
pub struct PullServeOperation {
    peer: NodeId,
    node: ServeNode,
    token: Option<AuthToken>,
    ask: PullAsk,
    stage: PullStage,
}

type PullTurn = Result<Next<Message>, IoError>;

fn status(status: SyncPullStatus) -> PullTurn {
    Ok(Next::Done(Message::ForwardedSyncPull { status }))
}

fn task_call(effect: PullEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Task(TaskEffect::Pull(effect))))
}

impl PullServeOperation {
    pub fn new((peer, node): (NodeId, ServeNode), token: AuthToken, ask: PullAsk) -> Self {
        Self {
            peer,
            node,
            token: Some(token),
            ask,
            stage: PullStage::Caller,
        }
    }

    fn request(&self) -> Option<&SyncPull> {
        match &self.ask {
            PullAsk::Pull(request) | PullAsk::Status(request) => Some(request),
            PullAsk::List(_) => None,
        }
    }

    // A refused pull is logged; a refused poll is only answered.
    fn refused(&self, refusal: SyncRefusal) -> PullTurn {
        match (&self.ask, self.request()) {
            (PullAsk::Pull(_), Some(request)) => status(SyncPullStatus::Refused(refused(
                self.peer, request, refusal,
            ))),
            (PullAsk::List(listing), _) => {
                let bucket = &listing.bucket;
                let kind = refusal_kind(&refusal);
                warn!(peer = %self.peer, bucket = %bucket, refusal = kind, "Refused a synced-folder listing");
                Ok(Next::Done(Message::ForwardedVersions {
                    result: Err(refusal),
                }))
            }
            _ => status(SyncPullStatus::Refused(refusal)),
        }
    }

    fn call(&mut self, stage: PullStage, call: IoCall) -> PullTurn {
        self.stage = stage;
        Ok(Next::Call(call))
    }

    fn claimed(&mut self, event: Event, holder: (AuthContext, BucketInfo)) -> PullTurn {
        let progress = match event {
            Event::Task(TaskEvent::Pull(PullEvent::Claimed(Ok(progress)))) => progress,
            Event::Task(TaskEvent::Pull(PullEvent::Claimed(Err(running)))) => {
                return status(running);
            }
            _ => return self.refused(SyncRefusal::Unavailable),
        };
        let (Some((node, _)), Some(routing), PullAsk::Pull(request)) =
            (self.node.local, self.node.routing.clone(), &self.ask)
        else {
            return self.refused(SyncRefusal::Unavailable);
        };
        let (Ok(key), total) = (receipt_of(request), request.size) else {
            return self.refused(SyncRefusal::Unavailable);
        };
        let commit = CommitPullOperation::new(
            (node, routing),
            holder,
            (**request).clone(),
            progress.clone(),
        );
        let target = (self.peer, request.target_bucket.clone());
        let run = PullRunOperation::new(target, key.to_vec(), commit, progress);
        self.call(PullStage::Started(total), IoCall::background(run))
    }
}

impl Operation for PullServeOperation {
    type Output = Message;
    type Error = IoError;

    fn first(&mut self) -> PullTurn {
        let Some(node) = self.node.local else {
            return self.refused(SyncRefusal::Unavailable);
        };
        let token = self.token.take().ok_or(IoError::Unexpected {
            expected: "the owner's token",
            got: "none",
        })?;
        let call = match &self.ask {
            PullAsk::Pull(request) | PullAsk::Status(request) => {
                let caller = (self.peer, node.1);
                IoCall::child(PullCallerOperation::new(caller, token, (**request).clone()))
            }
            PullAsk::List(listing) => {
                let list = ListHeadsOperation::new(self.peer, node, token, listing.clone());
                return self.call(PullStage::Listed, IoCall::child(list));
            }
        };
        self.call(PullStage::Caller, call)
    }

    fn next(&mut self, result: IoResult) -> PullTurn {
        match std::mem::replace(&mut self.stage, PullStage::Listed) {
            PullStage::Caller => {
                let auth = match result.child::<AuthContext, PullError>()? {
                    Ok(auth) => auth,
                    Err(error) => return self.refused(error.refusal()),
                };
                let (Some((node, _)), Some(request)) = (self.node.local, self.request()) else {
                    return self.refused(SyncRefusal::Unavailable);
                };
                match &self.ask {
                    PullAsk::Pull(_) => {
                        let admit = AdmitPullOperation::new(node, auth.clone(), request.clone());
                        self.call(PullStage::Admit(Box::new(auth)), IoCall::child(admit))
                    }
                    _ => match receipt_of(request) {
                        Ok(key) => {
                            let read = task_call(PullEffect::Status { key: key.to_vec() });
                            self.call(PullStage::Registry, read)
                        }
                        Err(refusal) => self.refused(refusal),
                    },
                }
            }
            PullStage::Admit(auth) => match result.child::<Admission, PullError>()? {
                Ok(Admission::Replayed(ack)) => status(SyncPullStatus::Done(ack)),
                Ok(Admission::Admitted(bucket)) => {
                    let Some(request) = self.request() else {
                        return self.refused(SyncRefusal::Unavailable);
                    };
                    let Ok(key) = receipt_of(request) else {
                        return self.refused(SyncRefusal::Unavailable);
                    };
                    let claim = PullEffect::Claim {
                        key: key.to_vec(),
                        total: request.size,
                    };
                    let holder = Box::new((*auth, bucket));
                    self.call(PullStage::Claim(holder), task_call(claim))
                }
                Err(error) => self.refused(error.refusal()),
            },
            PullStage::Claim(holder) => self.claimed(result.event()?, *holder),
            PullStage::Started(total) => match result.event()? {
                Event::Task(TaskEvent::BackgroundStarted { started: true }) => {
                    status(SyncPullStatus::Pending {
                        bytes_done: 0,
                        total,
                    })
                }
                // The run never started, so its claim was dropped with it.
                _ => self.refused(SyncRefusal::Unavailable),
            },
            PullStage::Registry => match result.event() {
                Ok(Event::Task(TaskEvent::Pull(PullEvent::Status(Some(running))))) => {
                    status(running)
                }
                _ => {
                    let Some(request) = self.request() else {
                        return status(SyncPullStatus::Unknown);
                    };
                    let receipt = ReceiptOperation::new(request);
                    self.call(PullStage::Receipt, IoCall::child(receipt))
                }
            },
            PullStage::Receipt => match result.child::<Option<Ulid>, PullError>()? {
                Ok(Some(version_id)) => status(SyncPullStatus::Done(SyncPullAck {
                    version_id,
                    already_applied: true,
                })),
                _ => status(SyncPullStatus::Unknown),
            },
            PullStage::Listed => match result.child::<SyncVersionPage, PullError>()? {
                Ok(page) => Ok(Next::Done(Message::ForwardedVersions { result: Ok(page) })),
                Err(error) => self.refused(error.refusal()),
            },
        }
    }
}

/// Commits one admitted pull beside the request that started it, then ends its claim with the
/// outcome. It keeps the progress until the claim ended, so polls see the run until then.
#[derive(Debug, PartialEq)]
pub struct PullRunOperation {
    peer: NodeId,
    bucket: String,
    key: Vec<u8>,
    commit: Option<CommitPullOperation>,
    progress: PullProgress,
    finished: bool,
}

impl PullRunOperation {
    pub fn new(
        (peer, bucket): (NodeId, String),
        key: Vec<u8>,
        commit: CommitPullOperation,
        progress: PullProgress,
    ) -> Self {
        Self {
            peer,
            bucket,
            key,
            commit: Some(commit),
            progress,
            finished: false,
        }
    }
}

impl Operation for PullRunOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let commit = self.commit.take().ok_or(IoError::Unexpected {
            expected: "a pull to commit",
            got: "none",
        })?;
        Ok(Next::Call(IoCall::child(commit)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.finished {
            return Ok(Next::Done(()));
        }
        self.finished = true;
        let result = (result.child::<SyncPullAck, PullError>()?).map_err(PullError::refusal);
        if let Err(refusal) = &result {
            let kind = refusal_kind(refusal);
            let bucket = &self.bucket;
            warn!(peer = %self.peer, bucket = %bucket, refusal = kind, "A synced-folder pull failed");
        }
        let key = std::mem::take(&mut self.key);
        Ok(Next::Call(task_call(PullEffect::Finish { key, result })))
    }
}
