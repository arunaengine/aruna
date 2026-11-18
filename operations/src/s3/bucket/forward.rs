//! Handles a bucket creation forwarded by a peer node and answers with the result.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::authorize::{peer_answer, peer_check};
use crate::forward::transport::reject;
use crate::metadata::device_pull::{pull_refusal, refusal_kind};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::realm::load_config::config_unavailable;
use crate::realm::peer_trust::{is_sync_eligible, peer_acts_for};
use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::events::WritePeerRefusal;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{
    BucketInfo, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::format::Compression;
use aruna_core::types::GroupId;
use std::time::{Duration, UNIX_EPOCH};
use tracing::warn;

type ForwardTurn = Result<Next<MetadataTransportMessage>, IoError>;

/// What a peer asks of this node's bucket records.
#[derive(Debug, PartialEq)]
enum BucketAsk {
    /// Creates the bucket, or accepts one of the same group the node already holds.
    Ensure { bucket: String, group_id: GroupId },
    /// Creates the bucket and tells whether this request did.
    Create { bucket: String, group_id: GroupId },
    /// Whether the caller may write under one prefix; nothing is written.
    Check { bucket: String, prefix: String },
    /// A message that is no bucket request.
    Other(&'static str),
}

#[derive(Debug, PartialEq)]
enum ForwardStep {
    Config,
    Peer,
    PeerConfig(Box<AuthContext>),
    Bucket(Box<AuthContext>),
    Authorize(Box<AuthContext>),
    Create,
    Existing,
}

/// Answers a peer's forwarded bucket creation or write check, for a caller the peer's token
/// names, on a node that takes part in the realm's replication.
#[derive(Debug, PartialEq)]
pub(crate) struct BucketForwardOperation {
    local: Option<(NodeId, RealmId)>,
    peer: NodeId,
    token: Option<AuthToken>,
    ask: BucketAsk,
    now_ms: u64,
    step: ForwardStep,
}

impl BucketForwardOperation {
    pub(crate) fn new(
        local: Option<(NodeId, RealmId)>,
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> Self {
        let (token, ask) = match message {
            MetadataTransportMessage::ForwardCreateBucket {
                auth_token,
                bucket,
                group_id,
            } => (Some(auth_token), BucketAsk::Ensure { bucket, group_id }),
            MetadataTransportMessage::ForwardNewBucket {
                auth_token,
                bucket,
                group_id,
            } => (Some(auth_token), BucketAsk::Create { bucket, group_id }),
            MetadataTransportMessage::ForwardCheckBucket {
                auth_token,
                bucket,
                prefix,
            } => (Some(auth_token), BucketAsk::Check { bucket, prefix }),
            _ => (None, BucketAsk::Other("unexpected bucket message")),
        };
        Self {
            local,
            peer,
            token,
            ask,
            now_ms: 0,
            step: ForwardStep::Config,
        }
    }

    fn bucket(&self) -> &str {
        match &self.ask {
            BucketAsk::Ensure { bucket, .. }
            | BucketAsk::Create { bucket, .. }
            | BucketAsk::Check { bucket, .. } => bucket,
            BucketAsk::Other(_) => "",
        }
    }

    fn ids(&self) -> Result<(NodeId, RealmId), IoError> {
        self.local.ok_or(IoError::Unfinished)
    }

    /// The answer of a request that ends in `result`; `created` tells a new bucket apart.
    fn answer(&self, result: Result<bool, SyncRefusal>) -> ForwardTurn {
        if let (Err(refusal), false) = (&result, matches!(self.ask, BucketAsk::Check { .. })) {
            warn!(
                peer = %self.peer,
                bucket = %self.bucket(),
                kind = refusal_kind(refusal),
                "Refused a forwarded bucket creation"
            );
        }
        Ok(Next::Done(match self.ask {
            BucketAsk::Ensure { .. } => MetadataTransportMessage::ForwardedBucketCreated {
                result: result.map(drop),
            },
            BucketAsk::Create { .. } => MetadataTransportMessage::ForwardedNewBucket { result },
            BucketAsk::Check { .. } => MetadataTransportMessage::ForwardedBucketChecked {
                result: result.map(drop),
            },
            BucketAsk::Other(message) => reject(message),
        }))
    }

    fn refuse(&self, refusal: SyncRefusal) -> ForwardTurn {
        self.answer(Err(refusal))
    }

    fn read_config(&mut self, realm_id: RealmId, step: ForwardStep) -> ForwardTurn {
        self.step = step;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn read_bucket(&mut self, step: ForwardStep) -> ForwardTurn {
        let bucket = GetBucketOperation::new(self.bucket().to_string());
        self.step = step;
        Ok(Next::Call(IoCall::child(bucket)))
    }

    fn authorize(
        &mut self,
        auth: Box<AuthContext>,
        path: String,
        operation: &'static str,
    ) -> ForwardTurn {
        let extras = PolicyRequestExtras::operation(operation);
        let check =
            AuthorizeOperation::new(auth.realm_id, &auth, &path, &Permission::WRITE, extras);
        self.step = ForwardStep::Authorize(auth);
        Ok(Next::Call(IoCall::child(check)))
    }

    /// The caller is known; a creation checks the bucket's path, a write check reads it first.
    fn caller_known(&mut self, auth: Box<AuthContext>) -> ForwardTurn {
        let (node_id, realm_id) = self.ids()?;
        if auth.realm_id != realm_id {
            return self.refuse(SyncRefusal::Unauthorized);
        }
        match &self.ask {
            BucketAsk::Ensure { bucket, group_id } | BucketAsk::Create { bucket, group_id } => {
                let path = bucket_permission_path(realm_id, *group_id, node_id, bucket);
                self.authorize(auth, path, "s3.CreateBucket")
            }
            BucketAsk::Check { .. } => self.read_bucket(ForwardStep::Bucket(auth)),
            BucketAsk::Other(_) => Err(IoError::Unfinished),
        }
    }

    fn create(&mut self, auth: &AuthContext) -> ForwardTurn {
        let (BucketAsk::Ensure { bucket, group_id } | BucketAsk::Create { bucket, group_id }) =
            &self.ask
        else {
            return self.answer(Ok(true));
        };
        let create = CreateBucketOperation::new(
            bucket.clone(),
            BucketInfo {
                group_id: *group_id,
                created_at: UNIX_EPOCH + Duration::from_millis(self.now_ms),
                created_by: auth.user_id,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
                distribution: None,
            },
        )
        .mapped();
        self.step = ForwardStep::Create;
        Ok(Next::Call(IoCall::child(create)))
    }

    /// The node holds a bucket of this name: a retried creation accepts its own.
    fn existing(&self, info: &BucketInfo) -> ForwardTurn {
        match &self.ask {
            BucketAsk::Ensure { bucket, group_id } => {
                self.answer(accept_existing(info, *group_id, bucket).map(|()| true))
            }
            _ => self.answer(Ok(false)),
        }
    }
}

impl Operation for BucketForwardOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> ForwardTurn {
        if matches!(self.ask, BucketAsk::Other(_)) {
            return self.answer(Ok(true));
        }
        let Some((_, realm_id)) = self.local else {
            return self.refuse(SyncRefusal::Unavailable);
        };
        self.read_config(realm_id, ForwardStep::Config)
    }

    fn next(&mut self, result: IoResult) -> ForwardTurn {
        match std::mem::replace(&mut self.step, ForwardStep::Create) {
            ForwardStep::Config => {
                let (node_id, _) = self.ids()?;
                let config = match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(config) => config,
                    Err(error) => {
                        config_unavailable(&error);
                        return self.refuse(SyncRefusal::Unavailable);
                    }
                };
                if !is_sync_eligible(&config, node_id) {
                    return self.refuse(SyncRefusal::Unavailable);
                }
                self.step = ForwardStep::Peer;
                Ok(Next::Call(peer_check(self.peer, self.token.take())))
            }
            ForwardStep::Peer => match peer_answer(result)? {
                Ok(auth) => {
                    let realm_id = auth.realm_id;
                    self.read_config(realm_id, ForwardStep::PeerConfig(Box::new(auth)))
                }
                Err(WritePeerRefusal::Unauthorized) => self.refuse(SyncRefusal::Unauthorized),
                Err(WritePeerRefusal::Unavailable(_)) => self.refuse(SyncRefusal::Unavailable),
            },
            ForwardStep::PeerConfig(auth) => {
                let config = match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(config) => config,
                    Err(error) => {
                        config_unavailable(&error);
                        return self.refuse(SyncRefusal::Unavailable);
                    }
                };
                match peer_acts_for(&config, self.peer, auth.user_id) {
                    true => self.caller_known(auth),
                    false => self.refuse(SyncRefusal::Unauthorized),
                }
            }
            ForwardStep::Bucket(auth) => {
                let (node_id, realm_id) = self.ids()?;
                let info = match result.child::<BucketInfo, GetBucketError>()? {
                    Ok(info) => info,
                    Err(GetBucketError::NotFound) => return self.refuse(SyncRefusal::NotFound),
                    Err(_) => return self.refuse(SyncRefusal::Unavailable),
                };
                let BucketAsk::Check { bucket, prefix } = &self.ask else {
                    return Err(IoError::Unfinished);
                };
                let group_id = info.group_id;
                let path = match prefix.is_empty() {
                    true => bucket_permission_path(realm_id, group_id, node_id, bucket),
                    false => object_permission_path(realm_id, group_id, node_id, bucket, prefix),
                };
                self.authorize(auth, path, "s3.PutObject")
            }
            ForwardStep::Authorize(auth) => match result.child::<(), AuthorizeError>()? {
                Ok(()) => self.create(&auth),
                Err(error) => self.refuse(pull_refusal(error)),
            },
            ForwardStep::Create => match result.child::<BucketInfo, CreateBucketError>()? {
                Ok(_) => self.answer(Ok(true)),
                Err(error @ CreateBucketError::ProviderBucket(_)) => {
                    self.refuse(SyncRefusal::Invalid(error.to_string()))
                }
                Err(CreateBucketError::BucketAlreadyExists)
                    if matches!(self.ask, BucketAsk::Create { .. }) =>
                {
                    self.answer(Ok(false))
                }
                // A create that lost the commit race to one of the same name reports existence.
                Err(_) => self.read_bucket(ForwardStep::Existing),
            },
            ForwardStep::Existing => match result.child::<BucketInfo, GetBucketError>()? {
                Ok(info) => self.existing(&info),
                Err(_) => self.refuse(SyncRefusal::Unavailable),
            },
        }
    }
}

/// A retried create succeeds only on an ordinary bucket of the same group; a member of a
/// distributed set is never taken for an ordinary bucket.
fn accept_existing(info: &BucketInfo, group_id: GroupId, bucket: &str) -> Result<(), SyncRefusal> {
    if info.distribution.is_some() {
        return Err(SyncRefusal::Invalid(format!(
            "bucket \"{bucket}\" belongs to a distributed bucket"
        )));
    }
    if info.group_id != group_id {
        return Err(SyncRefusal::Invalid(format!(
            "bucket \"{bucket}\" belongs to another group"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::storage::distribution::DistributionBinding;
    use std::time::SystemTime;
    use ulid::Ulid;

    #[test]
    fn bound_bucket_refused() {
        let group_id = Ulid::from_bytes([1; 16]);
        let mut info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::local(Ulid::from_bytes([2; 16]), RealmId::from_bytes([3; 32])),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        assert!(accept_existing(&info, group_id, "b").is_ok());
        assert!(accept_existing(&info, Ulid::from_bytes([9; 16]), "b").is_err());
        info.distribution = Some(DistributionBinding {
            set_id: Ulid::from_bytes([4; 16]),
            member_id: Ulid::from_bytes([5; 16]),
            incarnation: 6,
            revision: Default::default(),
        });
        assert!(accept_existing(&info, group_id, "b").is_err());
    }

    fn peer() -> NodeId {
        iroh::SecretKey::from_bytes(&[7; 32]).public()
    }

    fn check_message() -> MetadataTransportMessage {
        MetadataTransportMessage::ForwardCheckBucket {
            auth_token: AuthToken::Internal(AuthContext::anonymous(RealmId::from_bytes([3; 32]))),
            bucket: "b".to_string(),
            prefix: String::new(),
        }
    }

    #[test]
    fn missing_network_refused() {
        let mut forward = BucketForwardOperation::new(None, peer(), check_message());
        let expected = MetadataTransportMessage::ForwardedBucketChecked {
            result: Err(SyncRefusal::Unavailable),
        };
        assert_eq!(forward.first(), Ok(Next::Done(expected)));
    }

    #[test]
    fn unreadable_config_refused() {
        let local = Some((peer(), RealmId::from_bytes([3; 32])));
        let mut forward = BucketForwardOperation::new(local, peer(), check_message());
        forward.first().unwrap();
        let failed: Result<RealmConfigDocument, GetConfigError> = Err(IoError::Unfinished.into());
        let outcome = aruna_core::calls::ChildOutcome::new(failed);
        let expected = MetadataTransportMessage::ForwardedBucketChecked {
            result: Err(SyncRefusal::Unavailable),
        };
        assert_eq!(
            forward.next(IoResult::Child(outcome)),
            Ok(Next::Done(expected))
        );
    }

    #[test]
    fn other_message_rejected() {
        let other = reject("anything");
        let mut forward = BucketForwardOperation::new(None, peer(), other);
        assert!(matches!(
            forward.first(),
            Ok(Next::Done(MetadataTransportMessage::Reject(_)))
        ));
    }
}
