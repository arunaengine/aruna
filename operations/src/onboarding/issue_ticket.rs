//! Issues a signed sync ticket listing the documents a joining node is allowed to sync.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{GROUP_DELETE_KEYSPACE, USER_KEYSPACE};
use aruna_core::onboarding::{OnboardingSecretError, OnboardingTicket};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, done};
use aruna_core::structs::identity::group_delete::{GroupDeletePhase, GroupDeleteRecord};
use aruna_core::structs::identity::realm::RealmId;
use ed25519_dalek::SigningKey;
use thiserror::Error;

pub const TICKET_TTL_SECS: u64 = 300;
const TICKET_PAGE_SIZE: usize = 512;

#[derive(Clone, Debug, PartialEq)]
pub struct IssueSyncInput {
    pub realm_signing_key: SigningKey,
    pub realm_id: RealmId,
    /// Node the ticket is issued to.
    pub node_id: NodeId,
    /// Local issuer node that owns the bootstrap node-usage snapshot.
    pub issuer_node_id: NodeId,
    pub now: u64,
    pub ttl_secs: u64,
}

/// Pages through the realm's users, then the deletion records, persists, and signs the ticket.
#[derive(Debug, PartialEq)]
pub struct IssueSyncOperation {
    input: IssueSyncInput,
    documents: Vec<DocumentTarget>,
    deletions: Vec<GroupDeleteRecord>,
    users: PrefixPages,
    deleted: PrefixPages,
}

#[derive(Debug, Error, PartialEq)]
pub enum IssueSyncError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    OnboardingSecretError(#[from] OnboardingSecretError),
    #[error("issuing onboarding sync ticket did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for IssueSyncError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "IssueSync".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl IssueSyncOperation {
    pub fn new(input: IssueSyncInput) -> Self {
        let documents = vec![
            DocumentTarget::RealmAuthorization {
                realm_id: input.realm_id,
            },
            DocumentTarget::RealmConfig {
                realm_id: input.realm_id,
            },
            DocumentTarget::NodeUsage {
                realm_id: input.realm_id,
                node_id: input.issuer_node_id,
                group_id: None,
            },
            DocumentTarget::NodeInfo {
                realm_id: input.realm_id,
                node_id: input.issuer_node_id,
            },
            DocumentTarget::WatchInterest {
                realm_id: input.realm_id,
                node_id: input.issuer_node_id,
            },
        ];
        let users = UserId::storage_prefix(input.realm_id);
        Self {
            input,
            documents,
            deletions: Vec::new(),
            users: PrefixPages::new(USER_KEYSPACE, users, TICKET_PAGE_SIZE),
            deleted: PrefixPages::new(GROUP_DELETE_KEYSPACE, Vec::new(), TICKET_PAGE_SIZE),
        }
    }

    fn take_users(&mut self, result: IoResult) -> Result<IoCall, IssueSyncError> {
        for (key, _) in self.users.take(result)? {
            let user_id = UserId::from_storage_key(&key)?;
            if user_id.realm_id == self.input.realm_id {
                self.documents.push(DocumentTarget::User { user_id });
            }
        }
        Ok(match self.users.done() {
            true => self.deleted.call(),
            false => self.users.call(),
        })
    }

    fn take_deletions(&mut self, result: IoResult) -> Result<IoCall, IssueSyncError> {
        for (key, value) in self.deleted.take(result)? {
            if key.len() != 16 {
                continue;
            }
            let record = GroupDeleteRecord::from_bytes(&value)?;
            if record.plan.realm_id == self.input.realm_id
                && record.phase == GroupDeletePhase::Deleted
            {
                self.deletions.push(record);
            }
        }
        Ok(match self.deleted.done() {
            true => IoCall::Sync,
            false => self.deleted.call(),
        })
    }

    fn issue(&mut self) -> Result<OnboardingTicket, IssueSyncError> {
        Ok(OnboardingTicket::issue_with_deletions(
            &self.input.realm_signing_key,
            &self.input.realm_id,
            self.input.node_id,
            self.input.now.saturating_add(self.input.ttl_secs),
            std::mem::take(&mut self.documents),
            std::mem::take(&mut self.deletions),
        )?)
    }
}

impl Operation for IssueSyncOperation {
    type Output = OnboardingTicket;
    type Error = IssueSyncError;

    fn first(&mut self) -> Result<Next<OnboardingTicket>, IssueSyncError> {
        Ok(Next::Call(self.users.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<OnboardingTicket>, IssueSyncError> {
        if !self.users.done() {
            return self.take_users(result).map(Next::Call);
        }
        if !self.deleted.done() {
            return self.take_deletions(result).map(Next::Call);
        }
        done(result)?;
        self.issue().map(Next::Done)
    }
}

#[cfg(test)]
mod tests {
    use super::{IssueSyncInput, IssueSyncOperation, TICKET_TTL_SECS};
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USER_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_storage::storage;
    use byteview::ByteView;
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[test]
    fn ticket_includes_topics() {
        let realm_signing_key = SigningKey::from_bytes(&[3u8; 32]);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let joiner_node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let issuer_node_id = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let mut operation = IssueSyncOperation::new(IssueSyncInput {
            realm_signing_key,
            realm_id,
            node_id: joiner_node_id,
            issuer_node_id,
            now: 100,
            ttl_secs: TICKET_TTL_SECS,
        });

        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let empty = || IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert!(matches!(
            operation.next(empty()),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        assert_eq!(operation.next(empty()), Ok(Next::Call(IoCall::Sync)));
        let Ok(Next::Done(ticket)) = operation.next(IoResult::Done) else {
            panic!("expected the ticket");
        };

        assert_eq!(ticket.payload.node_id, joiner_node_id.to_string());
        assert_eq!(
            ticket.payload.documents,
            vec![
                DocumentTarget::RealmAuthorization { realm_id },
                DocumentTarget::RealmConfig { realm_id },
                DocumentTarget::NodeUsage {
                    realm_id,
                    node_id: issuer_node_id,
                    group_id: None,
                },
                DocumentTarget::NodeInfo {
                    realm_id,
                    node_id: issuer_node_id,
                },
                DocumentTarget::WatchInterest {
                    realm_id,
                    node_id: issuer_node_id,
                },
            ]
        );
        ticket
            .verify(
                joiner_node_id,
                &DocumentTarget::NodeInfo {
                    realm_id,
                    node_id: issuer_node_id,
                },
                100,
            )
            .unwrap();
    }

    #[tokio::test]
    async fn ticket_discovery_paginates() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let realm_signing_key = SigningKey::from_bytes(&[3u8; 32]);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let issuer_node_id = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let user_count = 10_005usize;
        let writes = (0..user_count)
            .map(|index| {
                let user_id =
                    UserId::local(Ulid::from_parts(1_800_000_000_000, index as u128), realm_id);
                (
                    USER_KEYSPACE.to_string(),
                    ByteView::from(user_id.to_storage_key()),
                    ByteView::from(vec![index as u8]),
                )
            })
            .collect();
        match storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected user seed result: {other:?}"),
        }

        let ticket = drive(
            IssueSyncOperation::new(IssueSyncInput {
                realm_signing_key,
                realm_id,
                node_id,
                issuer_node_id,
                now: 100,
                ttl_secs: TICKET_TTL_SECS,
            }),
            &context,
        )
        .await
        .unwrap();

        let users = ticket
            .payload
            .documents
            .iter()
            .filter(|document| matches!(document, DocumentTarget::User { .. }))
            .count();
        assert_eq!(users, user_count);
        assert!(
            ticket
                .payload
                .documents
                .contains(&DocumentTarget::NodeUsage {
                    realm_id,
                    node_id: issuer_node_id,
                    group_id: None,
                })
        );
        assert!(
            ticket
                .payload
                .documents
                .contains(&DocumentTarget::NodeInfo {
                    realm_id,
                    node_id: issuer_node_id,
                })
        );
        assert!(
            ticket
                .payload
                .documents
                .contains(&DocumentTarget::WatchInterest {
                    realm_id,
                    node_id: issuer_node_id,
                })
        );
        assert_eq!(ticket.payload.documents.len(), user_count + 5);
    }
}
