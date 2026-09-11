use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::onboarding::{OnboardingSecretError, OnboardingSyncTicket};
use aruna_core::operation::Operation;
use aruna_core::structs::RealmId;
use aruna_core::types::{Effects, Key, UserId};
use ed25519_dalek::SigningKey;
use smallvec::smallvec;
use thiserror::Error;

pub const ONBOARDING_SYNC_TICKET_TTL_SECS: u64 = 300;
const USER_SYNC_TICKET_PAGE_SIZE: usize = 512;

#[derive(Clone, Debug, PartialEq)]
pub struct IssueOnboardingSyncTicketInput {
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
pub struct IssueOnboardingSyncTicketOperation {
    input: IssueOnboardingSyncTicketInput,
    state: IssueOnboardingSyncTicketState,
    documents: Vec<DocumentSyncTarget>,
    next_start_after: Option<Key>,
    output: Option<Result<OnboardingSyncTicket, IssueOnboardingSyncTicketError>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum IssueOnboardingSyncTicketState {
    Init,
    ListUsers,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum IssueOnboardingSyncTicketError {
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

impl IssueOnboardingSyncTicketOperation {
    pub fn new(input: IssueOnboardingSyncTicketInput) -> Self {
        let documents = vec![
            DocumentSyncTarget::RealmAuthorization {
                realm_id: input.realm_id,
            },
            DocumentSyncTarget::RealmConfig {
                realm_id: input.realm_id,
            },
            DocumentSyncTarget::NodeUsage {
                realm_id: input.realm_id,
                node_id: input.issuer_node_id,
                group_id: None,
            },
            DocumentSyncTarget::NodeInfo {
                realm_id: input.realm_id,
                node_id: input.issuer_node_id,
            },
            DocumentSyncTarget::WatchInterest {
                realm_id: input.realm_id,
                node_id: input.issuer_node_id,
            },
        ];
        Self {
            input,
            state: IssueOnboardingSyncTicketState::Init,
            documents,
            next_start_after: None,
            output: None,
        }
    }

    fn emit_list_users(&mut self) -> Effects {
        self.state = IssueOnboardingSyncTicketState::ListUsers;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: USER_KEYSPACE.to_string(),
            prefix: Some(UserId::storage_prefix(self.input.realm_id)),
            start: self.next_start_after.take().map(IterStart::After),
            limit: USER_SYNC_TICKET_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn finish(&mut self) -> Effects {
        match OnboardingSyncTicket::issue(
            &self.input.realm_signing_key,
            &self.input.realm_id,
            self.input.node_id,
            self.input.now.saturating_add(self.input.ttl_secs),
            std::mem::take(&mut self.documents),
        ) {
            Ok(ticket) => {
                self.state = IssueOnboardingSyncTicketState::Finish;
                self.output = Some(Ok(ticket));
            }
            Err(error) => {
                self.state = IssueOnboardingSyncTicketState::Error;
                self.output = Some(Err(error.into()));
            }
        }
        smallvec![]
    }

    fn fail(&mut self, error: IssueOnboardingSyncTicketError) -> Effects {
        self.state = IssueOnboardingSyncTicketState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for IssueOnboardingSyncTicketOperation {
    type Output = OnboardingSyncTicket;
    type Error = IssueOnboardingSyncTicketError;

    fn start(&mut self) -> Effects {
        self.emit_list_users()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            IssueOnboardingSyncTicketState::ListUsers => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    for (key, _) in values {
                        match UserId::from_storage_key(&key) {
                            Ok(user_id) if user_id.realm_id == self.input.realm_id => {
                                self.documents.push(DocumentSyncTarget::User { user_id });
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
                other => self.fail(IssueOnboardingSyncTicketError::UnexpectedEvent {
                    state: format!("{:?}", self.state),
                    expected: "storage iter result",
                    got: format!("{other:?}"),
                }),
            },
            IssueOnboardingSyncTicketState::Init
            | IssueOnboardingSyncTicketState::Finish
            | IssueOnboardingSyncTicketState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            IssueOnboardingSyncTicketState::Finish | IssueOnboardingSyncTicketState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(IssueOnboardingSyncTicketError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IssueOnboardingSyncTicketInput, IssueOnboardingSyncTicketOperation,
        ONBOARDING_SYNC_TICKET_TTL_SECS,
    };
    use crate::driver::{DriverContext, drive};
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USER_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::structs::RealmId;
    use aruna_core::types::UserId;
    use aruna_storage::storage;
    use byteview::ByteView;
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[test]
    fn ticket_includes_shared_realm_topics_for_issuer_node() {
        let realm_signing_key = SigningKey::from_bytes(&[3u8; 32]);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let joiner_node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let issuer_node_id = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let mut operation =
            IssueOnboardingSyncTicketOperation::new(IssueOnboardingSyncTicketInput {
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
                DocumentSyncTarget::RealmAuthorization { realm_id },
                DocumentSyncTarget::RealmConfig { realm_id },
                DocumentSyncTarget::NodeUsage {
                    realm_id,
                    node_id: issuer_node_id,
                    group_id: None,
                },
                DocumentSyncTarget::NodeInfo {
                    realm_id,
                    node_id: issuer_node_id,
                },
                DocumentSyncTarget::WatchInterest {
                    realm_id,
                    node_id: issuer_node_id,
                },
            ]
        );
        ticket
            .verify(
                joiner_node_id,
                &DocumentSyncTarget::NodeInfo {
                    realm_id,
                    node_id: issuer_node_id,
                },
                100,
            )
            .unwrap();
    }

    #[tokio::test]
    async fn ticket_user_discovery_paginates_beyond_ten_thousand() {
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
            IssueOnboardingSyncTicketOperation::new(IssueOnboardingSyncTicketInput {
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
            .filter(|document| matches!(document, DocumentSyncTarget::User { .. }))
            .count();
        assert_eq!(users, user_count);
        assert!(
            ticket
                .payload
                .documents
                .contains(&DocumentSyncTarget::NodeUsage {
                    realm_id,
                    node_id: issuer_node_id,
                    group_id: None,
                })
        );
        assert!(
            ticket
                .payload
                .documents
                .contains(&DocumentSyncTarget::NodeInfo {
                    realm_id,
                    node_id: issuer_node_id,
                })
        );
        assert!(
            ticket
                .payload
                .documents
                .contains(&DocumentSyncTarget::WatchInterest {
                    realm_id,
                    node_id: issuer_node_id,
                })
        );
        assert_eq!(ticket.payload.documents.len(), user_count + 5);
    }
}
