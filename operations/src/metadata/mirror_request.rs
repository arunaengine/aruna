//! Asks the peer endpoint of a relationship to create or delete its sync mirror.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::metadata::{AuthToken, MetadataError};
use aruna_core::operation::Operation;
use aruna_core::rows::event;
use aruna_core::structs::SyncRelationship;
use aruna_core::types::GroupId;

use crate::auth::request_policy::PolicyRequestExtras;
use crate::metadata::message_kind::transport_message_kind;
use crate::metadata::protocol::{MetadataTransportMessage, decode_message, encode_message};
use crate::sync::mirror_repair::RECONCILE_GRACE;

/// A mirror request ends before repair could treat its create as abandoned.
const MIRROR_REQUEST_TIMEOUT: Duration = RECONCILE_GRACE.saturating_sub(Duration::from_secs(10));

/// One create or delete request to `peer`; a refusal of the peer is `MetadataError::Backend`.
#[derive(Debug, PartialEq)]
pub struct MirrorRequestOperation {
    peer: NodeId,
    frame: Result<Vec<u8>, String>,
    creating: bool,
}

impl MirrorRequestOperation {
    /// Asks `peer` to store the incoming mirror of `relationship`.
    pub fn create(
        peer: NodeId,
        auth_token: AuthToken,
        source_group_id: GroupId,
        relationship: SyncRelationship,
        extras: PolicyRequestExtras,
    ) -> Self {
        let message = MetadataTransportMessage::CreateSyncMirror {
            auth_token: Some(auth_token),
            source_group_id,
            relationship: Box::new(relationship),
            extras,
        };
        Self::with(peer, &message, true)
    }

    /// Asks `peer` to delete its side of `relationship`.
    pub fn delete(
        peer: NodeId,
        auth_token: AuthToken,
        relationship: SyncRelationship,
        extras: PolicyRequestExtras,
        rollback: bool,
    ) -> Self {
        let message = MetadataTransportMessage::DeleteSyncMirror {
            auth_token: Some(auth_token),
            relationship: Box::new(relationship),
            extras,
            rollback,
        };
        Self::with(peer, &message, false)
    }

    fn with(peer: NodeId, message: &MetadataTransportMessage, creating: bool) -> Self {
        Self {
            peer,
            frame: encode_message(message),
            creating,
        }
    }

    fn answered(&self, answer: MetadataTransportMessage) -> Result<Next<()>, MetadataError> {
        match answer {
            MetadataTransportMessage::SyncMirrorCreated if self.creating => Ok(Next::Done(())),
            MetadataTransportMessage::SyncMirrorDeleted if !self.creating => Ok(Next::Done(())),
            MetadataTransportMessage::Reject(error) => Err(MetadataError::Backend(error)),
            other => Err(MetadataError::Backend(format!(
                "unexpected sync mirror response: {}",
                transport_message_kind(&other)
            ))),
        }
    }
}

impl Operation for MirrorRequestOperation {
    type Output = ();
    type Error = MetadataError;

    fn first(&mut self) -> Result<Next<()>, MetadataError> {
        let frame = self.frame.clone().map_err(MetadataError::Backend)?;
        let request = MetadataRequestEffect {
            peer: self.peer,
            frame,
            timeout: MIRROR_REQUEST_TIMEOUT,
        };
        let effect = Effect::Net(NetEffect::MetadataRequest(Box::new(request)));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MetadataError> {
        let answer = match event(result)? {
            Event::Net(NetEvent::MetadataAnswer(answer)) => *answer,
            other => {
                return Err(MetadataError::Backend(format!(
                    "unexpected sync mirror event: {}",
                    other.label()
                )));
            }
        };
        let MetadataAnswer { result, .. } = answer;
        match result {
            Ok(bytes) => {
                let answer = decode_message(&bytes)
                    .map_err(|error| MetadataError::Backend(error.to_string()))?;
                self.answered(answer)
            }
            Err(PeerRequestError::Failed(error) | PeerRequestError::NotSent(error)) => Err(error),
            Err(PeerRequestError::TimedOut) => Err(MetadataError::Backend(
                "sync mirror request timed out".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::replication::ArunaArn;
    use aruna_core::structs::{SyncMode, SyncState, SyncStatusSnapshot};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn peer() -> NodeId {
        node(4)
    }

    fn relationship() -> SyncRelationship {
        let realm_id = RealmId::from_bytes([1; 32]);
        SyncRelationship {
            id: Ulid::from_bytes([2; 16]),
            source: ArunaArn::s3_bucket(realm_id, node(3), "source").unwrap(),
            target: ArunaArn::s3_bucket(realm_id, peer(), "target").unwrap(),
            mode: SyncMode::Continuous,
            reference_handling: Default::default(),
            reference_serving: false,
            replicate_deletes: true,
            created_by: UserId::local(Ulid::from_bytes([5; 16]), realm_id),
            created_at: SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        }
    }

    fn token() -> AuthToken {
        AuthToken::internal(AuthContext {
            user_id: relationship().created_by,
            realm_id: relationship().source.realm_id,
            path_restrictions: None,
            session: None,
        })
    }

    fn create() -> MirrorRequestOperation {
        MirrorRequestOperation::create(
            peer(),
            token(),
            Ulid::from_bytes([6; 16]),
            relationship(),
            PolicyRequestExtras::operation("s3.PutBucketReplication"),
        )
    }

    fn delete() -> MirrorRequestOperation {
        let extras = PolicyRequestExtras::operation("s3.DeleteBucketReplication");
        MirrorRequestOperation::delete(peer(), token(), relationship(), extras, true)
    }

    fn answer(result: Result<Vec<u8>, PeerRequestError>) -> IoResult {
        IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
            MetadataAnswer {
                peer: peer(),
                elapsed: Duration::ZERO,
                result,
            },
        )))))
    }

    fn reply(message: MetadataTransportMessage) -> IoResult {
        answer(Ok(encode_message(&message).unwrap()))
    }

    #[test]
    fn sends_bounded_frame() {
        assert!(MIRROR_REQUEST_TIMEOUT < RECONCILE_GRACE);
        let mut operation = delete();
        let Ok(Next::Call(IoCall::Effect(effect))) = operation.first() else {
            panic!("expected the peer request");
        };
        let Effect::Net(NetEffect::MetadataRequest(request)) = *effect else {
            panic!("expected a metadata request");
        };
        assert_eq!(request.peer, peer());
        assert_eq!(request.timeout, MIRROR_REQUEST_TIMEOUT);
        assert!(matches!(
            decode_message(&request.frame),
            Ok(MetadataTransportMessage::DeleteSyncMirror { rollback: true, .. })
        ));
    }

    #[test]
    fn accepts_matching_answer() {
        let created = reply(MetadataTransportMessage::SyncMirrorCreated);
        assert_eq!(create().next(created), Ok(Next::Done(())));
        let deleted = reply(MetadataTransportMessage::SyncMirrorDeleted);
        assert_eq!(delete().next(deleted), Ok(Next::Done(())));
        assert!(matches!(
            delete().next(reply(MetadataTransportMessage::SyncMirrorCreated)),
            Err(MetadataError::Backend(message))
                if message == "unexpected sync mirror response: sync_mirror_created"
        ));
    }

    #[test]
    fn reports_failures() {
        let refused = reply(MetadataTransportMessage::Reject("not_found".to_string()));
        assert_eq!(
            create().next(refused),
            Err(MetadataError::Backend("not_found".to_string()))
        );
        assert_eq!(
            create().next(answer(Err(PeerRequestError::TimedOut))),
            Err(MetadataError::Backend(
                "sync mirror request timed out".to_string()
            ))
        );
        let missing = PeerRequestError::Failed(MetadataError::HandleMissing);
        assert_eq!(
            create().next(answer(Err(missing))),
            Err(MetadataError::HandleMissing)
        );
        assert!(matches!(
            create().next(IoResult::Done),
            Err(MetadataError::Backend(_))
        ));
    }
}
