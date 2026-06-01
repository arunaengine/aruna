use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, RealmAuthorizationDocument};
use aruna_core::types::{Effects, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::announce::AnnounceTopicOperation;

#[derive(Clone, Debug, PartialEq)]
pub struct ClaimInitialRealmAdminInput {
    pub actor: Actor,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClaimInitialRealmAdminResult {
    Claimed(RealmAuthorizationDocument),
    AlreadyClaimed,
}

#[derive(PartialEq)]
pub struct ClaimInitialRealmAdminOperation {
    input: ClaimInitialRealmAdminInput,
    state: ClaimInitialRealmAdminState,
    output: Option<Result<ClaimInitialRealmAdminResult, ClaimInitialRealmAdminError>>,
}

impl std::fmt::Debug for ClaimInitialRealmAdminOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClaimInitialRealmAdminOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClaimInitialRealmAdminState {
    Init,
    StartTransaction,
    GetAuthDoc {
        txn_id: TxnId,
    },
    UpdateAuthDoc {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
    },
    CommitTransaction {
        auth_doc: RealmAuthorizationDocument,
    },
    AbortTransaction,
    AnnounceAuthDoc {
        auth_doc: RealmAuthorizationDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ClaimInitialRealmAdminError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("authorization document not found")]
    AuthDocNotFound,
    #[error("realm_admin role not found")]
    RealmAdminRoleNotFound,
    #[error("claiming initial realm admin did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: ClaimInitialRealmAdminState,
        expected: &'static str,
        got: String,
    },
}

impl ClaimInitialRealmAdminOperation {
    pub fn new(input: ClaimInitialRealmAdminInput) -> Self {
        Self {
            input,
            state: ClaimInitialRealmAdminState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ClaimInitialRealmAdminError) -> Effects {
        self.state = ClaimInitialRealmAdminState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(ClaimInitialRealmAdminError::UnexpectedEvent {
            state: self.state.clone(),
            expected,
            got,
        })
    }

    fn emit_get_auth_doc(&mut self, txn_id: TxnId) -> Effects {
        self.state = ClaimInitialRealmAdminState::GetAuthDoc { txn_id };
        let key = ByteView::from(*self.input.actor.realm_id.as_bytes());
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })]
    }

    fn emit_update_auth_doc(
        &mut self,
        txn_id: TxnId,
        auth_doc: Option<ByteView>,
    ) -> Result<Effects, ClaimInitialRealmAdminError> {
        let mut auth_doc = RealmAuthorizationDocument::from_bytes(
            &auth_doc.ok_or(ClaimInitialRealmAdminError::AuthDocNotFound)?,
        )?;
        let role = auth_doc
            .roles
            .values_mut()
            .find(|role| role.name == "realm_admin")
            .ok_or(ClaimInitialRealmAdminError::RealmAdminRoleNotFound)?;

        if !role.assigned_users.is_empty() {
            self.state = ClaimInitialRealmAdminState::AbortTransaction;
            self.output = Some(Ok(ClaimInitialRealmAdminResult::AlreadyClaimed));
            return Ok(smallvec![Effect::Storage(
                StorageEffect::AbortTransaction { txn_id }
            )]);
        }

        role.assigned_users.insert(self.input.actor.user_id);
        let key = ByteView::from(*auth_doc.realm_id.as_bytes());
        let value = auth_doc.to_bytes(&self.input.actor)?.into();

        self.state = ClaimInitialRealmAdminState::UpdateAuthDoc {
            txn_id,
            auth_doc: auth_doc.clone(),
        };

        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: AUTH_KEYSPACE.to_string(),
            key,
            value,
            txn_id: Some(txn_id),
        })])
    }
}

impl Operation for ClaimInitialRealmAdminOperation {
    type Output = ClaimInitialRealmAdminResult;
    type Error = ClaimInitialRealmAdminError;

    fn start(&mut self) -> Effects {
        self.state = ClaimInitialRealmAdminState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => return self.fail(error.into()),
            other => other,
        };

        match self.state.clone() {
            ClaimInitialRealmAdminState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self
                        .unexpected_event("Event::Storage(StorageEvent::TransactionStarted)", got);
                };
                self.emit_get_auth_doc(txn_id)
            }
            ClaimInitialRealmAdminState::GetAuthDoc { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.unexpected_event("Event::Storage(StorageEvent::ReadResult)", got);
                };

                match self.emit_update_auth_doc(txn_id, value) {
                    Ok(effects) => effects,
                    Err(error) => self.fail(error),
                }
            }
            ClaimInitialRealmAdminState::UpdateAuthDoc { txn_id, auth_doc } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.unexpected_event("Event::Storage(StorageEvent::WriteResult)", got);
                };

                self.state = ClaimInitialRealmAdminState::CommitTransaction {
                    auth_doc: auth_doc.clone(),
                };
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            ClaimInitialRealmAdminState::CommitTransaction { auth_doc } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.unexpected_event(
                        "Event::Storage(StorageEvent::TransactionCommitted)",
                        got,
                    );
                };

                self.state = ClaimInitialRealmAdminState::AnnounceAuthDoc {
                    auth_doc: auth_doc.clone(),
                };
                let document = DocumentSyncTarget::RealmAuthorization {
                    realm_id: auth_doc.realm_id,
                };
                smallvec![Effect::SubOperation(boxed_suboperation(
                    AnnounceTopicOperation::new_for_document(
                        document.topic_id(),
                        self.input.actor.node_id,
                        Some(document),
                    ),
                    |result| Event::SubOperation(SubOperationEvent::DocumentSyncResult {
                        result: result.map_err(|error| error.to_string()),
                    }),
                ))]
            }
            ClaimInitialRealmAdminState::AbortTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionAborted { .. }) = event else {
                    return self
                        .unexpected_event("Event::Storage(StorageEvent::TransactionAborted)", got);
                };

                self.state = ClaimInitialRealmAdminState::Finish;
                smallvec![]
            }
            ClaimInitialRealmAdminState::AnnounceAuthDoc { auth_doc } => {
                let got = format!("{event:?}");
                let Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) = event
                else {
                    return self.unexpected_event(
                        "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
                        got,
                    );
                };

                if let Err(error) = result {
                    return self.fail(ClaimInitialRealmAdminError::TopicAnnouncement(error));
                }

                self.state = ClaimInitialRealmAdminState::Finish;
                self.output = Some(Ok(ClaimInitialRealmAdminResult::Claimed(auth_doc.clone())));
                smallvec![]
            }
            ClaimInitialRealmAdminState::Finish
            | ClaimInitialRealmAdminState::Error
            | ClaimInitialRealmAdminState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ClaimInitialRealmAdminState::Finish | ClaimInitialRealmAdminState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(ClaimInitialRealmAdminError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            ClaimInitialRealmAdminState::GetAuthDoc { txn_id }
            | ClaimInitialRealmAdminState::UpdateAuthDoc { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation, ClaimInitialRealmAdminResult,
    };
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::structs::{Actor, RealmId};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use ed25519_dalek::SigningKey;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    async fn setup_context() -> (DriverContext, NetHandle, RealmId, iroh::PublicKey, TempDir) {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();

        let context = DriverContext {
            storage_handle,
            blob_handle: None,
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            task_handle: Some(task_handle),
        };

        let realm_config = CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            realm_description: "Realm".to_string(),
            oidc_providers: Vec::new(),
        };
        drive(CreateRealmOperation::new(realm_config), &context)
            .await
            .unwrap();

        (context, net_handle, realm_id, node_id, random_path)
    }

    #[tokio::test]
    async fn claims_initial_realm_admin_once() {
        let (context, net_handle, realm_id, node_id, _temp_dir) = setup_context().await;
        let user_id = UserId::local(Ulid::new(), realm_id);
        let result = drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: Actor {
                    node_id,
                    user_id,
                    realm_id,
                },
            }),
            &context,
        )
        .await
        .unwrap();

        match result {
            ClaimInitialRealmAdminResult::Claimed(auth_doc) => {
                assert!(auth_doc.roles.values().any(|role| {
                    role.name == "realm_admin" && role.assigned_users.contains(&user_id)
                }));
            }
            other => panic!("unexpected claim result: {other:?}"),
        }

        let second = drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: Actor {
                    node_id,
                    user_id: UserId::local(Ulid::new(), realm_id),
                    realm_id,
                },
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(second, ClaimInitialRealmAdminResult::AlreadyClaimed);

        net_handle.shutdown().await;
    }
}
