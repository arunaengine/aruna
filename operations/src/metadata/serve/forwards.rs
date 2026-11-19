//! Serves the writes, fetches and audit pages peers forward, each through the operation of its
//! domain, and turns that operation's outcome into the reply.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::audit::{AuditPageRequest, AuditPageResponse};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::heartbeat::Here;
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::group_delete::{GroupDeleteProof, GroupDeletionError};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::vault::VaultRecords;
use tracing::warn;

use crate::forward::realm::ApplyRelayOperation;
use crate::forward::token::ApplyRevokeOperation;
use crate::forward::transport::reject;
use crate::groups::deletion::ApplyRequestOperation;
use crate::groups::forward::ApplyGroupOperation;
use crate::metadata::api::MetadataApiError;
use crate::metadata::audit::AUDIT_DEADLINE_SECS;
use crate::metadata::audit_entry::ServeAuditOperation;
use crate::metadata::device::serve::{
    apply_device_batch, serve_graph_state, serve_realm_documents,
};
use crate::metadata::persistent_id::forward::ServePidOperation;
use crate::metadata::read_peer::ReadPeerOperation;
use crate::metadata::serve::publish::serve_publish;
use crate::placement::policy::transport::PolicyAnswer;
use crate::placement::policy::{ApplyPolicyOperation, ServePolicyOperation};
use crate::s3::bucket::forward::BucketForwardOperation;
use crate::transfer::forward::serve_admission;
use crate::users::vault_route::{ApplyVaultOperation, ServeVaultOperation};
use aruna_core::metadata::protocol::{MetadataReadError, MetadataTransportMessage as Message};

/// How a forwarded request's child outcome becomes the reply.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ForwardReply {
    /// The child answers the reply; its failure is refused with this prefix.
    Refused(&'static str),
    /// The child answers the reply; its failure is logged with this text and answers
    /// that the write is unavailable.
    Logged(&'static str),
    Pid,
    Policy,
    Vault,
    Deletion,
    Audit,
}

impl ForwardReply {
    pub(crate) fn answer(self, result: IoResult) -> Result<Message, IoError> {
        Ok(match self {
            Self::Refused(prefix) => match result.child::<Message, IoError>()? {
                Ok(reply) => reply,
                Err(error) => reject(format!("{prefix}{error}")),
            },
            Self::Logged(text) => match result.child::<Message, IoError>()? {
                Ok(reply) => reply,
                Err(error) => {
                    warn!(%error, "{text}");
                    Message::ForwardedWriteUnavailable
                }
            },
            Self::Pid => match result.child::<Message, MetadataApiError>()? {
                Ok(reply) => reply,
                Err(error) => reject(format!("persistent id request failed: {error}")),
            },
            Self::Policy => Message::ForwardedPlacementPolicy {
                result: (result.child::<PolicyAnswer, MetadataReadError>()?)
                    .and_then(|answer| answer),
            },
            Self::Vault => Message::FetchedVaultRecords {
                result: result.child::<VaultRecords, MetadataReadError>()?,
            },
            Self::Deletion => Message::GroupDeletionResult {
                result: result.child::<Option<GroupDeleteProof>, GroupDeletionError>()?,
            },
            Self::Audit => Message::ForwardedAuditPage {
                result: audit_page(result)?,
            },
        })
    }
}

// The page the audit group answered; a page not served by the deadline is unavailable.
fn audit_page(result: IoResult) -> Result<Result<AuditPageResponse, MetadataReadError>, IoError> {
    let IoResult::Group(GroupEvent::Finished { results }) = result else {
        return Err(IoError::Unexpected {
            expected: "the audit group",
            got: result.label(),
        });
    };
    match results.into_iter().next() {
        Some(GroupResult::Done(event)) => match *event {
            Event::Child(outcome) => IoResult::Child(outcome).child(),
            other => Err(IoError::Unexpected {
                expected: "the audit page",
                got: other.label(),
            }),
        },
        _ => Ok(Err(MetadataReadError::Unavailable)),
    }
}

/// What a forwarded request runs, or its reply when it needs nothing.
#[derive(Debug)]
pub(crate) enum Forward {
    Call(IoCall, ForwardReply),
    Done(Message),
}

/// The child and reply of a forwarded request, or the request back when it is none of these;
/// `fresh` is the id a group deletion request is applied under.
pub(crate) fn forward_call(
    peer: NodeId,
    (local, here): (Option<(NodeId, RealmId)>, Option<&Here>),
    message: Message,
    fresh: ulid::Ulid,
) -> Result<Forward, Message> {
    let refused = ForwardReply::Refused("");
    let (call, reply) = match message {
        Message::ForwardAuditPage { request } => {
            let Some(local) = local else {
                let page = Message::ForwardedAuditPage {
                    result: Err(MetadataReadError::Unavailable),
                };
                return Ok(Forward::Done(page));
            };
            (audit_group(peer, local, request), ForwardReply::Audit)
        }
        forward @ Message::ForwardTokenRevocation { .. } => {
            let apply = ApplyRevokeOperation::new(local, peer, forward);
            let reply = ForwardReply::Refused("token revocation failed: ");
            (IoCall::child(apply), reply)
        }
        forward @ Message::ForwardPersistentId { .. } => {
            let Some((node, realm_id)) = local else {
                return Ok(Forward::Done(Message::ForwardedWriteUnavailable));
            };
            match ServePidOperation::new(peer, (realm_id, node), forward) {
                Some(serve) => (IoCall::child(serve), ForwardReply::Pid),
                None => {
                    let unexpected = reject("unexpected forwarded persistent id message");
                    return Ok(Forward::Done(unexpected));
                }
            }
        }
        Message::ForwardPlacementPolicy { policy_ref } => {
            let Some((_, realm_id)) = local else {
                let policy = Message::ForwardedPlacementPolicy {
                    result: Err(MetadataReadError::Unavailable),
                };
                return Ok(Forward::Done(policy));
            };
            let serve = ServePolicyOperation::new(realm_id, peer, policy_ref);
            (IoCall::child(serve), ForwardReply::Policy)
        }
        forward @ Message::ForwardVaultChange { .. } => {
            let apply = ApplyVaultOperation::new(local, peer, forward);
            (
                IoCall::child(apply),
                ForwardReply::Logged("Forwarded vault change failed"),
            )
        }
        Message::FetchVaultRecords { user_id, query } => {
            let serve = ServeVaultOperation::new(local, peer, user_id, query);
            (IoCall::child(serve), ForwardReply::Vault)
        }
        forward @ Message::ForwardCreatePolicy { .. } => (
            IoCall::child(ApplyPolicyOperation::new(local, peer, forward)),
            refused,
        ),
        bucket @ (Message::ForwardCheckBucket { .. }
        | Message::ForwardCreateBucket { .. }
        | Message::ForwardNewBucket { .. }) => (
            IoCall::child(BucketForwardOperation::new(local, peer, bucket)),
            refused,
        ),
        admit @ Message::ForwardAdmitMember { .. } => {
            let Some(here) = here else {
                return Ok(Forward::Done(Message::ForwardedWriteUnavailable));
            };
            (
                IoCall::child(serve_admission(here.clone(), peer, admit)),
                refused,
            )
        }
        publish @ (Message::ForwardDistributionRequest { .. }
        | Message::SubmitDistributionStatement { .. }) => {
            (IoCall::child(serve_publish(local, peer, publish)), refused)
        }
        fetch @ Message::FetchRealmDocuments { .. } => (
            IoCall::child(serve_realm_documents(local, peer, fetch)),
            refused,
        ),
        fetch @ Message::FetchGraphState { .. } => (
            IoCall::child(serve_graph_state(local, peer, fetch)),
            refused,
        ),
        forward @ Message::ForwardApplyBatch { .. } => (
            IoCall::child(apply_device_batch(local, peer, forward)),
            refused,
        ),
        forward @ Message::ForwardAdminEvent { .. } => {
            let apply = ApplyRelayOperation::new(local, peer, forward);
            let reply = ForwardReply::Logged("Failed to persist a relayed admin event");
            (IoCall::child(apply), reply)
        }
        forward @ Message::ForwardGroupCreate { .. } => {
            let apply = ApplyGroupOperation::new(local, peer, forward);
            (
                IoCall::child(apply),
                ForwardReply::Refused("group create failed: "),
            )
        }
        request @ Message::GroupDeletion { .. } => {
            let apply = ApplyRequestOperation::new(local, peer, request, fresh);
            (IoCall::child(apply), ForwardReply::Deletion)
        }
        other => return Err(other),
    };
    Ok(Forward::Call(call, reply))
}

/// Whether `message` needs this node's view of the realm and its storage routing: a member
/// admission and a synced-folder pull do.
pub fn needs_view(message: &Message) -> bool {
    matches!(
        message,
        Message::ForwardAdmitMember { .. } | Message::ForwardSyncPull { .. }
    )
}

// The audit page served as the only member of a group, so the deadline bounds the caller check
// and the page read together.
fn audit_group(peer: NodeId, local: (NodeId, RealmId), request: AuditPageRequest) -> IoCall {
    let audit = PeerAuditOperation::new(peer, local, request);
    let member = boxed_suboperation(audit, |result| {
        Event::Child(aruna_core::calls::ChildOutcome::new(result))
    });
    IoCall::Group(EffectGroup {
        members: vec![Effect::SubOperation(member)],
        limit: 1,
        form: GroupForm::All {
            deadline: Some(Duration::from_secs(AUDIT_DEADLINE_SECS)),
        },
    })
}

/// Checks the caller of a forwarded audit page, then serves this node's page for it.
#[derive(Debug, PartialEq)]
pub(crate) struct PeerAuditOperation {
    peer: NodeId,
    local: (NodeId, RealmId),
    request: Option<AuditPageRequest>,
    checked: bool,
}

impl PeerAuditOperation {
    pub(crate) fn new(peer: NodeId, local: (NodeId, RealmId), request: AuditPageRequest) -> Self {
        Self {
            peer,
            local,
            request: Some(request),
            checked: false,
        }
    }
}

type AuditTurn = Result<Next<AuditPageResponse>, MetadataReadError>;

impl Operation for PeerAuditOperation {
    type Output = AuditPageResponse;
    type Error = MetadataReadError;

    fn first(&mut self) -> AuditTurn {
        let (_, realm_id) = self.local;
        let token: Option<AuthToken> =
            (self.request.as_ref()).and_then(|request| request.auth_token.clone());
        let check = ReadPeerOperation::new((self.peer, realm_id), token, false);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> AuditTurn {
        if self.checked {
            return Ok(Next::Done(
                result.child::<AuditPageResponse, MetadataReadError>()??,
            ));
        }
        self.checked = true;
        let caller = result.child::<Option<AuthContext>, MetadataReadError>()??;
        let auth = caller.ok_or(MetadataReadError::Unauthorized)?;
        let request = self.request.take().ok_or(MetadataReadError::Unavailable)?;
        let (node, realm_id) = self.local;
        let serve = ServeAuditOperation::new((realm_id, node), auth, request);
        Ok(Next::Call(IoCall::child(serve)))
    }
}
