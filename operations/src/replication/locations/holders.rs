//! Resolves a distributed version through its holders. Each holder is chosen with the runner's
//! entropy and asked once; one that cannot answer is avoided by the next choices.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::heartbeat::HeartbeatEffect;
use aruna_core::holder_choice::{HolderCandidate, HolderChoiceError, PickHolderOperation};
use aruna_core::id::NodeId;
use aruna_core::node::state::{NodeInfoError, ReadInfoOperation};
use aruna_core::operation::Operation;
use aruna_core::replication_protocol::{LocationSummary, LocationSummaryRequest};
use aruna_core::structs::storage::node_info::NodeInfoDocument;
use thiserror::Error;
use ulid::Ulid;

use super::LocationSummaryError;
use super::remote::RemoteLocationOperation;

/// Holders one resolution asks at most.
pub const MAX_HOLDER_TRIES: usize = 3;

/// What the holders said about a version. A holder may lack the bytes of a version its range
/// holds, so a miss proves no absence: without a holder that has it the version is unavailable.
#[derive(Debug, PartialEq)]
pub enum HolderAnswer {
    /// `node_id` holds the version's bytes and publishes its API at `api_url`.
    Found {
        node_id: NodeId,
        summary: Box<LocationSummary>,
        api_url: String,
    },
    /// A holder refused the caller, and none held the version for it.
    Denied,
    Unavailable,
}

#[derive(Debug, Error, PartialEq)]
pub enum HolderResolveError {
    #[error(transparent)]
    Choice(#[from] HolderChoiceError),
    #[error(transparent)]
    Info(#[from] NodeInfoError),
    #[error(transparent)]
    Io(#[from] IoError),
}

type Turn = Result<Next<HolderAnswer>, HolderResolveError>;
type Documents = BTreeMap<NodeId, NodeInfoDocument>;

#[derive(Debug, PartialEq)]
enum HolderStage {
    Choose,
    Ask(NodeId),
    Origin(NodeId, Box<LocationSummary>),
    Avoid,
}

/// Asks the holders of a version one at a time, at most [`MAX_HOLDER_TRIES`] of them and none
/// after `deadline_ms`, until one holds its bytes. The choices, the holders it skips and the
/// answer all follow from the run's events, so a replay asks the same holders.
#[derive(Debug, PartialEq)]
pub struct HolderResolveOperation {
    remote: Vec<NodeId>,
    request: LocationSummaryRequest,
    version_id: Ulid,
    deadline_ms: u64,
    now_ms: u64,
    tries: usize,
    denied: bool,
    stage: HolderStage,
}

impl HolderResolveOperation {
    pub fn new(
        remote: Vec<NodeId>,
        request: LocationSummaryRequest,
        version_id: Ulid,
        deadline_ms: u64,
    ) -> Self {
        Self {
            remote,
            request,
            version_id,
            deadline_ms,
            now_ms: 0,
            tries: 0,
            denied: false,
            stage: HolderStage::Choose,
        }
    }

    /// Chooses the next holder, or ends once the tries or the time are spent.
    fn choose(&mut self) -> Turn {
        if self.tries >= MAX_HOLDER_TRIES || self.now_ms >= self.deadline_ms {
            let answer = match self.denied {
                true => HolderAnswer::Denied,
                false => HolderAnswer::Unavailable,
            };
            return Ok(Next::Done(answer));
        }
        self.tries += 1;
        self.stage = HolderStage::Choose;
        let pick = PickHolderOperation::new(&self.remote, self.now_ms);
        Ok(Next::Call(IoCall::child(pick)))
    }

    fn ask(&mut self, holder: NodeId) -> Turn {
        self.remote.retain(|node_id| *node_id != holder);
        self.stage = HolderStage::Ask(holder);
        let remote = RemoteLocationOperation::new(holder, self.request.clone());
        Ok(Next::Call(IoCall::child(remote)))
    }

    /// A holder that holds the version with its group and size goes on to its published API; a
    /// miss asks the next one; a refusal is kept; any other answer avoids the holder.
    fn answered(
        &mut self,
        holder: NodeId,
        answer: Result<LocationSummary, LocationSummaryError>,
    ) -> Turn {
        match answer {
            Ok(summary) if summary.version_id != Some(self.version_id) || !summary.materialized => {
                self.choose()
            }
            Ok(summary) if summary.group_id.is_some() && summary.blob_size.is_some() => {
                self.stage = HolderStage::Origin(holder, Box::new(summary));
                Ok(Next::Call(IoCall::child(ReadInfoOperation::new(vec![
                    holder,
                ]))))
            }
            Err(LocationSummaryError::Denied) => {
                self.denied = true;
                self.choose()
            }
            Err(LocationSummaryError::BucketNotFound) => self.choose(),
            Ok(_) | Err(_) => self.avoid(holder),
        }
    }

    fn avoid(&mut self, holder: NodeId) -> Turn {
        self.stage = HolderStage::Avoid;
        let avoid = HeartbeatEffect::Avoid {
            holder,
            now_ms: self.now_ms,
        };
        let effect = Effect::Net(NetEffect::Heartbeat(Box::new(avoid)));
        Ok(Next::Call(IoCall::from(effect)))
    }
}

impl Operation for HolderResolveOperation {
    type Output = HolderAnswer;
    type Error = HolderResolveError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn {
        self.choose()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.stage, HolderStage::Choose) {
            HolderStage::Choose => {
                match result.child::<Option<HolderCandidate>, HolderChoiceError>()?? {
                    Some(holder) => self.ask(holder.node_id),
                    None => {
                        self.tries = MAX_HOLDER_TRIES;
                        self.choose()
                    }
                }
            }
            HolderStage::Ask(holder) => {
                let answer = result.child::<LocationSummary, LocationSummaryError>()?;
                self.answered(holder, answer)
            }
            HolderStage::Origin(holder, summary) => {
                let documents = result.child::<Documents, NodeInfoError>()??;
                match documents
                    .get(&holder)
                    .and_then(|document| document.urls.api.clone())
                {
                    Some(api_url) => Ok(Next::Done(HolderAnswer::Found {
                        node_id: holder,
                        summary,
                        api_url,
                    })),
                    None => self.avoid(holder),
                }
            }
            // A holder skipped or not, the next choice goes on.
            HolderStage::Avoid => {
                result.event()?;
                self.choose()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::events::{Event, NetEvent};
    use aruna_core::heartbeat::HeartbeatEvent;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::node_info::NodeUrls;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn request() -> LocationSummaryRequest {
        let realm_id = RealmId::from_bytes([3; 32]);
        LocationSummaryRequest {
            realm_id,
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Some(Ulid::from_parts(7, 7)),
            auth_context: AuthContext::anonymous(realm_id),
        }
    }

    fn picked(seed: u8) -> IoResult {
        let holder = HolderCandidate {
            node_id: node(seed),
            urls: NodeUrls {
                api: None,
                s3: None,
            },
            free_slots: 1,
            bandwidth: 0,
        };
        IoResult::Child(ChildOutcome::new(Ok::<_, HolderChoiceError>(Some(holder))))
    }

    fn summary(error: LocationSummaryError) -> IoResult {
        IoResult::Child(ChildOutcome::new(Err::<LocationSummary, _>(error)))
    }

    // Each holder is chosen by a pick run of its own, so the choice follows the run's entropy; a
    // holder that cannot answer is avoided and never asked again, and a refusal is the answer once
    // no holder had the version.
    #[test]
    fn holders_asked_once() {
        let version_id = Ulid::from_parts(7, 7);
        let holders = vec![node(1), node(2)];
        let mut routing = HolderResolveOperation::new(holders, request(), version_id, 100);
        routing.stamp(10);
        let pick = |holders: &[NodeId]| IoCall::child(PickHolderOperation::new(holders, 10));
        assert_eq!(routing.first(), Ok(Next::Call(pick(&[node(1), node(2)]))));
        let asked = IoCall::child(RemoteLocationOperation::new(node(1), request()));
        assert_eq!(routing.next(picked(1)), Ok(Next::Call(asked)));
        let avoid = HeartbeatEffect::Avoid {
            holder: node(1),
            now_ms: 10,
        };
        let avoid = IoCall::from(Effect::Net(NetEffect::Heartbeat(Box::new(avoid))));
        let failed = summary(LocationSummaryError::Aborted);
        assert_eq!(routing.next(failed), Ok(Next::Call(avoid)));
        let avoided = Event::Net(NetEvent::Heartbeat(HeartbeatEvent::Avoided));
        let avoided = IoResult::Event(Box::new(avoided));
        assert_eq!(routing.next(avoided), Ok(Next::Call(pick(&[node(2)]))));
        routing.next(picked(2)).unwrap();
        let refused = routing.next(summary(LocationSummaryError::Denied));
        assert_eq!(refused, Ok(Next::Call(pick(&[]))));
        let none: Result<Option<HolderCandidate>, HolderChoiceError> = Ok(None);
        let none = IoResult::Child(ChildOutcome::new(none));
        assert_eq!(routing.next(none), Ok(Next::Done(HolderAnswer::Denied)));
    }
}
