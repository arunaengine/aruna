use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::onboarding::{OnboardingSecretError, OnboardingTicket};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{Effects, Key};
use ed25519_dalek::SigningKey;
use smallvec::smallvec;
use thiserror::Error;

pub const ONBOARDING_SYNC_TICKET_TTL_SECS: u64 = 300;
const USER_SYNC_TICKET_PAGE_SIZE: usize = 512;

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

#[derive(Debug, PartialEq)]
pub struct IssueSyncOperation {
    input: IssueSyncInput,
    state: IssueSyncState,
    documents: Vec<DocumentTarget>,
    next_start_after: Option<Key>,
    output: Option<Result<OnboardingTicket, IssueSyncError>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum IssueSyncState {
    Init,
    ListUsers,
    Finish,
    Error,
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
        got: String,
    },
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
        Self {
            input,
            state: IssueSyncState::Init,
            documents,
            next_start_after: None,
            output: None,
        }
    }

    fn emit_list_users(&mut self) -> Effects {
        self.state = IssueSyncState::ListUsers;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: USER_KEYSPACE.to_string(),
            prefix: Some(UserId::storage_prefix(self.input.realm_id)),
            start: self.next_start_after.take().map(IterStart::After),
            limit: USER_SYNC_TICKET_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn finish(&mut self) -> Effects {
        match OnboardingTicket::issue(
            &self.input.realm_signing_key,
            &self.input.realm_id,
            self.input.node_id,
            self.input.now.saturating_add(self.input.ttl_secs),
            std::mem::take(&mut self.documents),
        ) {
            Ok(ticket) => {
                self.state = IssueSyncState::Finish;
                self.output = Some(Ok(ticket));
            }
            Err(error) => {
                self.state = IssueSyncState::Error;
                self.output = Some(Err(error.into()));
            }
        }
        smallvec![]
    }

    fn fail(&mut self, error: IssueSyncError) -> Effects {
        self.state = IssueSyncState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for IssueSyncOperation {
    type Output = OnboardingTicket;
    type Error = IssueSyncError;

    fn start(&mut self) -> Effects {
        self.emit_list_users()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            IssueSyncState::ListUsers => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    for (key, _) in values {
                        match UserId::from_storage_key(&key) {
                            Ok(user_id) if user_id.realm_id == self.input.realm_id => {
                                self.documents.push(DocumentTarget::User { user_id });
                            }
                            Ok(_) => {}
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    if let Some(next_start_after) = next_start_after {
                        self.next_start_after = Some(next_start_after);
                        self.emit_list_users()
                    } else {
                        self.finish()
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(IssueSyncError::UnexpectedEvent {
                    state: format!("{:?}", self.state),
                    expected: "storage iter result",
                    got: format!("{other:?}"),
                }),
            },
            IssueSyncState::Init | IssueSyncState::Finish | IssueSyncState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, IssueSyncState::Finish | IssueSyncState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(IssueSyncError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{IssueSyncInput, IssueSyncOperation, ONBOARDING_SYNC_TICKET_TTL_SECS};
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
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
            ttl_secs: ONBOARDING_SYNC_TICKET_TTL_SECS,
        });

        assert_eq!(operation.start().len(), 1);
        assert!(
            operation
                .step(Event::Storage(StorageEvent::IterResult {
                    values: Vec::new(),
                    next_start_after: None,
                }))
                .is_empty()
        );
        let ticket = operation.finalize().unwrap();

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
            task_handle: None,
            compute_handle: None,
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
                ttl_secs: ONBOARDING_SYNC_TICKET_TTL_SECS,
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
