//! Serves a distribution request another node forwards to the publisher: checks the caller, a
//! linked dataset with the caller's own access, then publishes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::metadata::protocol::MetadataTransportMessage;
use aruna_core::operation::Operation;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::NodeRealm;
use aruna_core::structs::storage::dataset_description::{DatasetDescription, DescriptionError};
use aruna_core::structs::storage::distribution::request::PublishRefusal;
use aruna_core::structs::storage::distribution::{DistributionRequest, DistributionRevision};
use ulid::Ulid;

use crate::distribution::forward::PeerCallerOperation;
use crate::distribution::submit::PublishLocalOperation;
use crate::document_repository::{config_of, realm_config};
use crate::forward::transport::reject;
use crate::metadata::forward::read::DatasetSummaryOperation;
use crate::realm::peer_trust::is_sync_eligible;

/// The dataset a metadata change links, if any.
fn linked_dataset(request: &DistributionRequest) -> Option<Ulid> {
    match request {
        DistributionRequest::ChangeMetadata {
            metadata: Some(metadata),
            ..
        } => metadata.dataset,
        _ => None,
    }
}

/// Refuses a metadata change that links a dataset the caller cannot read or that is no Dataset,
/// as the metadata route does.
#[derive(Debug, PartialEq)]
pub(crate) struct ReadableLinkOperation {
    auth: AuthContext,
    token: AuthToken,
    dataset: Option<Ulid>,
}

impl ReadableLinkOperation {
    pub(crate) fn new(auth: AuthContext, token: AuthToken, request: &DistributionRequest) -> Self {
        Self {
            auth,
            token,
            dataset: linked_dataset(request),
        }
    }
}

impl Operation for ReadableLinkOperation {
    type Output = ();
    type Error = PublishRefusal;

    fn first(&mut self) -> Result<Next<()>, PublishRefusal> {
        let Some(dataset) = self.dataset else {
            return Ok(Next::Done(()));
        };
        let caller = (Some(&self.auth), Some(self.token.clone()));
        let read = DatasetSummaryOperation::new(self.auth.realm_id, dataset, caller);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PublishRefusal> {
        let dataset = self.dataset.ok_or(IoError::Unfinished)?;
        match result.child::<DatasetDescription, DescriptionError>()? {
            Ok(_) => Ok(Next::Done(())),
            Err(DescriptionError::Unreadable) => Err(PublishRefusal::Rejected(format!(
                "dataset {dataset} does not exist or you may not read it"
            ))),
            Err(DescriptionError::Invalid(reason)) => Err(PublishRefusal::Rejected(reason)),
            Err(DescriptionError::Unavailable) => Err(PublishRefusal::Unavailable(format!(
                "dataset {dataset} could not be read now"
            ))),
        }
    }
}

/// The publisher's side of a forwarded request; see [`ServePublishOperation`].
pub(crate) fn serve_publish(
    ids: NodeRealm,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> ServePublishOperation {
    ServePublishOperation::new(ids, peer, message)
}

/// What the pending call of a served publish answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServeStep {
    Caller,
    Link,
    Peer,
    Publish,
}

/// A request waiting for its checks on the publisher.
#[derive(Debug, PartialEq)]
struct Waiting {
    ids: (Ulid, Ulid),
    request: DistributionRequest,
    auth: Option<AuthContext>,
    token: Option<AuthToken>,
}

/// The publisher's side of a forwarded admin request or member statement.
#[derive(Debug, PartialEq)]
pub(crate) struct ServePublishOperation {
    ids: NodeRealm,
    peer: NodeId,
    message: Option<MetadataTransportMessage>,
    waiting: Option<Waiting>,
    step: ServeStep,
}

type Served = Result<Next<MetadataTransportMessage>, IoError>;

fn accepted(result: Result<DistributionRevision, PublishRefusal>) -> Served {
    let message = MetadataTransportMessage::DistributionAccepted { result };
    Ok(Next::Done(message))
}

impl ServePublishOperation {
    pub(crate) fn new(ids: NodeRealm, peer: NodeId, message: MetadataTransportMessage) -> Self {
        Self {
            ids,
            peer,
            message: Some(message),
            waiting: None,
            step: ServeStep::Caller,
        }
    }

    fn publish(&mut self) -> Served {
        let waiting = self.waiting.take().ok_or(IoError::Unfinished)?;
        let publish =
            PublishLocalOperation::new(self.ids, waiting.ids, waiting.request, waiting.auth);
        self.step = ServeStep::Publish;
        Ok(Next::Call(IoCall::child(publish)))
    }

    fn caller_known(&mut self, caller: Result<AuthContext, SyncRefusal>) -> Served {
        let waiting = self.waiting.as_mut().ok_or(IoError::Unfinished)?;
        let auth = match caller {
            Ok(auth) => auth,
            Err(SyncRefusal::Unavailable) => {
                return accepted(Err(PublishRefusal::Unavailable(
                    "the caller could not be checked".to_string(),
                )));
            }
            Err(_) => return accepted(Err(PublishRefusal::Unauthorized)),
        };
        // Statements carry their signer's authority, never a forwarded caller's.
        if !waiting.request.admin() {
            return accepted(Err(PublishRefusal::Rejected(
                "statements are submitted without a caller".to_string(),
            )));
        }
        let token = waiting.token.take().ok_or(IoError::Unfinished)?;
        let link = ReadableLinkOperation::new(auth.clone(), token, &waiting.request);
        waiting.auth = Some(auth);
        self.step = ServeStep::Link;
        Ok(Next::Call(IoCall::child(link)))
    }
}

impl Operation for ServePublishOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> Served {
        match self.message.take().ok_or(IoError::Unfinished)? {
            MetadataTransportMessage::ForwardDistributionRequest {
                auth_token,
                set_id,
                request_id,
                request,
            } => {
                let caller = PeerCallerOperation::new(self.peer, auth_token.clone());
                self.waiting = Some(Waiting {
                    ids: (set_id, request_id),
                    request: *request,
                    auth: None,
                    token: Some(auth_token),
                });
                Ok(Next::Call(IoCall::child(caller)))
            }
            MetadataTransportMessage::SubmitDistributionStatement { statement } => {
                // Only configured realm infrastructure nodes relay statements.
                let Some((_, realm_id)) = self.ids else {
                    return accepted(Err(PublishRefusal::Unavailable(
                        "net transport unavailable".to_string(),
                    )));
                };
                self.waiting = Some(Waiting {
                    ids: (statement.statement.set_id, statement.statement.statement_id),
                    request: DistributionRequest::Statement(statement),
                    auth: None,
                    token: None,
                });
                self.step = ServeStep::Peer;
                Ok(Next::Call(realm_config(realm_id)))
            }
            _ => Ok(Next::Done(reject("unexpected distribution message"))),
        }
    }

    fn next(&mut self, result: IoResult) -> Served {
        match self.step {
            ServeStep::Caller => self.caller_known(result.child::<AuthContext, SyncRefusal>()?),
            ServeStep::Link => match result.child::<(), PublishRefusal>()? {
                Ok(()) => self.publish(),
                Err(refusal) => accepted(Err(refusal)),
            },
            ServeStep::Peer => match config_of(result)? {
                None => accepted(Err(PublishRefusal::Unavailable(
                    "realm config missing".to_string(),
                ))),
                Some(config) if is_sync_eligible(&config, self.peer) => self.publish(),
                Some(_) => accepted(Err(PublishRefusal::Unauthorized)),
            },
            ServeStep::Publish => accepted(result.child::<DistributionRevision, PublishRefusal>()?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_drivers::driver::drive;

    // The publisher checks a linked dataset with the caller's own access, like the REST route.
    #[tokio::test]
    async fn refuses_unreadable_link() {
        use aruna_core::structs::storage::bucket_metadata::BucketMetadata;
        let (_directory, context) = crate::tests::notifications::context_with_storage();
        let realm = RealmId::from_bytes([9; 32]);
        let auth = AuthContext {
            user_id: UserId::nil(realm),
            realm_id: realm,
            path_restrictions: None,
            session: None,
        };
        let change = |dataset| DistributionRequest::ChangeMetadata {
            metadata: Some(Box::new(BucketMetadata {
                dataset,
                ..BucketMetadata::default()
            })),
            expected: None,
        };
        let token = || AuthToken::bearer("token").unwrap();
        let missing = change(Some(Ulid::from_bytes([7; 16])));
        let link = |request: &DistributionRequest| {
            ReadableLinkOperation::new(auth.clone(), token(), request)
        };
        let refused = drive(link(&missing), &context).await;
        assert!(
            matches!(refused, Err(PublishRefusal::Rejected(_))),
            "{refused:?}"
        );
        for unlinked in [
            change(None),
            DistributionRequest::ChangeMetadata {
                metadata: None,
                expected: None,
            },
        ] {
            let kept = drive(link(&unlinked), &context).await;
            assert_eq!(kept, Ok(()));
        }
    }
}
