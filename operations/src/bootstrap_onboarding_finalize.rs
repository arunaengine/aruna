use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::errors::StorageError;
use aruna_core::onboarding::{OnboardingMode, OnboardingSecretError};
use aruna_core::structs::{
    Actor, DEFAULT_METADATA_REPLICATION_FACTOR, RealmId, RealmNodeKind, ResourceEvent,
};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_millis;
use ed25519_dalek::SigningKey;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::consume_onboarding_secret::{
    ConsumeOnboardingSecretError, ConsumeOnboardingSecretInput, ConsumeOnboardingSecretOperation,
};
use crate::driver::{DriverContext, drive};
use crate::ensure_realm_config::{
    EnsureRealmConfigConfig, EnsureRealmConfigError, EnsureRealmConfigOperation,
};
use crate::issue_onboarding_sync_ticket::{
    IssueOnboardingSyncTicketError, IssueOnboardingSyncTicketInput,
    IssueOnboardingSyncTicketOperation, ONBOARDING_SYNC_TICKET_TTL_SECS,
};
use crate::notifications::emit::{EmitNotificationsInput, EmitNotificationsOperation};
use crate::notifications::routing::{RoutingContext, route_resource_event};
use crate::process_placements::{PlacementConfig, PlacementError, ProcessPlacementsOperation};
use crate::read_realm_authorization::ReadRealmAuthorizationOperation;
use crate::reserve_onboarding_secret::{
    ReserveOnboardingSecretError, ReserveOnboardingSecretInput, ReserveOnboardingSecretOperation,
};

const ONBOARDING_RESERVATION_TTL_SECS: u64 = 300;
const REALM_NODE_UPDATE_RETRIES: usize = 5;

#[derive(Clone, Debug, PartialEq)]
pub struct BootstrapOnboardingFinalizeInput {
    pub enrollment_id: Ulid,
    pub secret_hash: String,
    pub node_id: NodeId,
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    pub realm_signing_key: SigningKey,
    pub now: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapOnboardingFinalizeOutput {
    pub mode: OnboardingMode,
    pub onboarding_sync_ticket: String,
}

#[derive(Debug, Error, PartialEq)]
pub enum BootstrapOnboardingFinalizeError {
    #[error(transparent)]
    Reserve(#[from] ReserveOnboardingSecretError),
    #[error(transparent)]
    EnsureRealmConfig(#[from] EnsureRealmConfigError),
    #[error(transparent)]
    Placement(#[from] PlacementError),
    #[error(transparent)]
    IssueTicket(#[from] IssueOnboardingSyncTicketError),
    #[error(transparent)]
    Consume(#[from] ConsumeOnboardingSecretError),
    #[error(transparent)]
    OnboardingSecret(#[from] OnboardingSecretError),
    #[error("net handle unavailable")]
    NetHandleUnavailable,
    #[error("document sync peer admission failed: {0}")]
    PeerAdmission(String),
}

pub async fn bootstrap_onboarding_finalize(
    input: BootstrapOnboardingFinalizeInput,
    context: Arc<DriverContext>,
) -> Result<BootstrapOnboardingFinalizeOutput, BootstrapOnboardingFinalizeError> {
    let reserved = drive(
        ReserveOnboardingSecretOperation::new(ReserveOnboardingSecretInput {
            enrollment_id: input.enrollment_id,
            secret_hash: input.secret_hash.clone(),
            node_id: input.node_id.to_string(),
            now: input.now,
            reservation_expires_at: input.now.saturating_add(ONBOARDING_RESERVATION_TTL_SECS),
            finalizing: true,
        }),
        context.as_ref(),
    )
    .await?;

    ensure_realm_node_with_retries(&input, reserved.mode, context.as_ref()).await?;
    process_pending_placements(&input, context.as_ref()).await?;

    let ticket = drive(
        IssueOnboardingSyncTicketOperation::new(IssueOnboardingSyncTicketInput {
            realm_signing_key: input.realm_signing_key,
            realm_id: input.realm_id,
            node_id: input.node_id,
            issuer_node_id: input.local_node_id,
            now: input.now,
            ttl_secs: ONBOARDING_SYNC_TICKET_TTL_SECS,
        }),
        context.as_ref(),
    )
    .await?;
    let encoded_ticket = ticket.encode()?;

    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(BootstrapOnboardingFinalizeError::NetHandleUnavailable)?;
    net_handle
        .ensure_document_sync_topics(&ticket.payload.documents, vec![input.node_id])
        .map_err(|error| BootstrapOnboardingFinalizeError::PeerAdmission(error.to_string()))?;
    net_handle
        .allow_document_sync_peers(&ticket.payload.documents, vec![input.node_id])
        .map_err(|error| BootstrapOnboardingFinalizeError::PeerAdmission(error.to_string()))?;

    drive(
        ConsumeOnboardingSecretOperation::new(ConsumeOnboardingSecretInput {
            enrollment_id: input.enrollment_id,
            secret_hash: input.secret_hash,
            node_id: input.node_id.to_string(),
            now: input.now,
        }),
        context.as_ref(),
    )
    .await?;

    emit_node_onboarded_notification(input.realm_id, input.node_id, context.as_ref()).await;

    Ok(BootstrapOnboardingFinalizeOutput {
        mode: reserved.mode,
        onboarding_sync_ticket: encoded_ticket,
    })
}

async fn emit_node_onboarded_notification(
    realm_id: RealmId,
    node_id: NodeId,
    context: &DriverContext,
) {
    let realm_auth = match drive(ReadRealmAuthorizationOperation::new(realm_id), context).await {
        Ok(Some(doc)) => doc,
        Ok(None) => {
            warn!(realm_id = %realm_id, "Skipping node onboarding notification: no realm authorization document");
            return;
        }
        Err(error) => {
            warn!(error = ?error, "Skipping node onboarding notification: realm authorization read failed");
            return;
        }
    };
    let records = route_resource_event(
        &ResourceEvent::NodeOnboarded { realm_id, node_id },
        RoutingContext {
            group_auth: None,
            realm_auth: Some(&realm_auth),
        },
        unix_timestamp_millis(),
    );
    if records.is_empty() {
        return;
    }
    if let Err(never) = drive(
        EmitNotificationsOperation::new(EmitNotificationsInput { records }),
        context,
    )
    .await
    {
        match never {}
    }
}

async fn ensure_realm_node_with_retries(
    input: &BootstrapOnboardingFinalizeInput,
    mode: OnboardingMode,
    context: &DriverContext,
) -> Result<(), EnsureRealmConfigError> {
    let mut last_conflict = None;
    for _ in 0..REALM_NODE_UPDATE_RETRIES {
        match ensure_realm_node_once(input, mode, context).await {
            Ok(()) => return Ok(()),
            Err(EnsureRealmConfigError::StorageError(StorageError::TransactionConflict)) => {
                last_conflict = Some(StorageError::TransactionConflict);
            }
            Err(error) => return Err(error),
        }
    }

    Err(EnsureRealmConfigError::StorageError(
        last_conflict.unwrap_or(StorageError::TransactionConflict),
    ))
}

async fn ensure_realm_node_once(
    input: &BootstrapOnboardingFinalizeInput,
    mode: OnboardingMode,
    context: &DriverContext,
) -> Result<(), EnsureRealmConfigError> {
    let kind = match mode {
        OnboardingMode::Management => RealmNodeKind::Management,
        OnboardingMode::Server => RealmNodeKind::Server,
        OnboardingMode::Local => RealmNodeKind::Local,
    };

    drive(
        EnsureRealmConfigOperation::new(EnsureRealmConfigConfig {
            actor: Actor {
                node_id: input.local_node_id,
                user_id: UserId::nil(input.realm_id),
                realm_id: input.realm_id,
            },
            target_node_id: input.node_id,
            target_node_kind: kind,
            default_metadata_replication_factor: DEFAULT_METADATA_REPLICATION_FACTOR,
            realm_description: String::new(),
            create_if_missing: false,
            reject_kind_mismatch: true,
        }),
        context,
    )
    .await?;

    Ok(())
}

async fn process_pending_placements(
    input: &BootstrapOnboardingFinalizeInput,
    context: &DriverContext,
) -> Result<(), PlacementError> {
    match drive(
        ProcessPlacementsOperation::new(PlacementConfig {
            realm_id: input.realm_id,
            local_node_id: input.local_node_id,
            retry_after: crate::sync_placement::SYNC_PLACEMENT_RETRY_AFTER,
        }),
        context,
    )
    .await
    {
        Ok(_) => Ok(()),
        Err(error) => {
            warn!(error = %error, "Failed to process pending document-sync placements during onboarding");
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BootstrapOnboardingFinalizeError, BootstrapOnboardingFinalizeInput,
        bootstrap_onboarding_finalize, emit_node_onboarded_notification,
    };
    use crate::create_onboarding_secret::{
        CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
    };
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use crate::get_realm_config::GetRealmConfigOperation;
    use crate::onboarding_secret_state::secret_state_key;
    use crate::reserve_onboarding_secret::ReserveOnboardingSecretError;
    use aruna_core::NodeId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, NOTIFICATION_OUTBOX_KEYSPACE, ONBOARDING_KEYSPACE};
    use aruna_core::onboarding::{
        OnboardingMode, OnboardingSecretRecord, OnboardingSecretState, OnboardingSecretStateRecord,
    };
    use aruna_core::structs::{
        Actor, NotificationKind, NotificationOutboxRecord, RealmAuthorizationDocument, RealmId,
    };
    use aruna_core::types::UserId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use ed25519_dalek::SigningKey;
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    const LOCAL_NODE_SECRET: [u8; 32] = [4u8; 32];
    const ONBOARDING_SECRET_EXPIRES_AT: u64 = 1_000;

    struct FinalizeFixture {
        _tempdir: TempDir,
        storage_handle: storage::StorageHandle,
        context: Arc<DriverContext>,
        realm_signing_key: SigningKey,
        realm_id: RealmId,
        local_node_id: NodeId,
        joiner_node_id: NodeId,
        enrollment_id: Ulid,
    }

    async fn setup_finalize_fixture() -> FinalizeFixture {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_signing_key = SigningKey::from_bytes(&[3u8; 32]);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let local_node_id = iroh::SecretKey::from_bytes(&LOCAL_NODE_SECRET).public();
        let joiner_node_id = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let user_id = UserId::local(Ulid::new(), realm_id);

        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id: local_node_id,
                    user_id,
                    realm_id,
                },
                realm_description: "Realm".to_string(),
                oidc_providers: Vec::new(),
            }),
            context.as_ref(),
        )
        .await
        .unwrap();

        let enrollment_id = Ulid::new();
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id,
                    secret_hash: "abc".to_string(),
                    mode: OnboardingMode::Server,
                    expires_at: ONBOARDING_SECRET_EXPIRES_AT,
                    claimed_node_id: None,
                },
            }),
            context.as_ref(),
        )
        .await
        .unwrap();

        FinalizeFixture {
            _tempdir: tempdir,
            storage_handle,
            context,
            realm_signing_key,
            realm_id,
            local_node_id,
            joiner_node_id,
            enrollment_id,
        }
    }

    fn finalize_input(
        fixture: &FinalizeFixture,
        node_id: NodeId,
        now: u64,
    ) -> BootstrapOnboardingFinalizeInput {
        BootstrapOnboardingFinalizeInput {
            enrollment_id: fixture.enrollment_id,
            secret_hash: "abc".to_string(),
            node_id,
            realm_id: fixture.realm_id,
            local_node_id: fixture.local_node_id,
            realm_signing_key: fixture.realm_signing_key.clone(),
            now,
        }
    }

    async fn read_secret_state(
        storage_handle: &storage::StorageHandle,
        enrollment_id: Ulid,
    ) -> OnboardingSecretState {
        let state_value = match storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: ONBOARDING_KEYSPACE.to_string(),
                key: secret_state_key(enrollment_id),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(value), ..
            }) => value,
            other => panic!("unexpected state read result: {other:?}"),
        };
        let state_record: OnboardingSecretStateRecord = postcard::from_bytes(&state_value).unwrap();
        state_record.state
    }

    async fn assert_realm_has_node(context: &DriverContext, realm_id: RealmId, node_id: NodeId) {
        let document = drive(GetRealmConfigOperation::new(realm_id), context)
            .await
            .unwrap();
        assert!(
            document
                .nodes
                .iter()
                .any(|node| node.node_id == node_id.to_string())
        );
    }

    async fn context_with_net(fixture: &FinalizeFixture) -> (Arc<DriverContext>, NetHandle) {
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                secret_key: Some(iroh::SecretKey::from_bytes(&LOCAL_NODE_SECRET)),
                realm_id: fixture.realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            fixture.storage_handle.clone(),
        )
        .await
        .unwrap();

        (
            Arc::new(DriverContext {
                storage_handle: fixture.storage_handle.clone(),
                net_handle: Some(net_handle.clone()),
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
            }),
            net_handle,
        )
    }

    async fn write_realm_admins(fixture: &FinalizeFixture, admins: &[UserId]) {
        let mut doc = RealmAuthorizationDocument::new_default_realm_doc(fixture.realm_id);
        for role in doc.roles.values_mut() {
            if role.name == "realm_admin" {
                role.assigned_users = admins.iter().copied().collect();
            }
        }
        let actor = Actor {
            node_id: fixture.local_node_id,
            user_id: UserId::nil(fixture.realm_id),
            realm_id: fixture.realm_id,
        };
        let value = doc.to_bytes(&actor).unwrap();
        fixture
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: (*fixture.realm_id.as_bytes()).into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
    }

    async fn read_outbox_rows(
        storage_handle: &storage::StorageHandle,
    ) -> Vec<NotificationOutboxRecord> {
        match storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| postcard::from_bytes(&value).unwrap())
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn failure_after_realm_membership_before_consume_marks_secret_finalizing() {
        let fixture = setup_finalize_fixture().await;

        let result = bootstrap_onboarding_finalize(
            finalize_input(&fixture, fixture.joiner_node_id, 10),
            fixture.context.clone(),
        )
        .await;
        assert_eq!(
            result,
            Err(BootstrapOnboardingFinalizeError::NetHandleUnavailable)
        );
        assert_realm_has_node(
            fixture.context.as_ref(),
            fixture.realm_id,
            fixture.joiner_node_id,
        )
        .await;

        assert_eq!(
            read_secret_state(&fixture.storage_handle, fixture.enrollment_id).await,
            OnboardingSecretState::Finalizing {
                node_id: fixture.joiner_node_id.to_string(),
            }
        );
    }

    #[tokio::test]
    async fn same_node_retry_after_finalizing_can_complete() {
        let fixture = setup_finalize_fixture().await;

        let first = bootstrap_onboarding_finalize(
            finalize_input(&fixture, fixture.joiner_node_id, 10),
            fixture.context.clone(),
        )
        .await;
        assert_eq!(
            first,
            Err(BootstrapOnboardingFinalizeError::NetHandleUnavailable)
        );

        let (retry_context, net_handle) = context_with_net(&fixture).await;
        let retry = bootstrap_onboarding_finalize(
            finalize_input(
                &fixture,
                fixture.joiner_node_id,
                ONBOARDING_SECRET_EXPIRES_AT + 1,
            ),
            retry_context,
        )
        .await
        .unwrap();
        assert_eq!(retry.mode, OnboardingMode::Server);
        assert!(!retry.onboarding_sync_ticket.is_empty());
        assert_eq!(
            read_secret_state(&fixture.storage_handle, fixture.enrollment_id).await,
            OnboardingSecretState::Consumed {
                node_id: fixture.joiner_node_id.to_string(),
            }
        );
        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn different_node_after_finalizing_is_rejected_even_after_record_ttl() {
        let fixture = setup_finalize_fixture().await;

        let first = bootstrap_onboarding_finalize(
            finalize_input(&fixture, fixture.joiner_node_id, 10),
            fixture.context.clone(),
        )
        .await;
        assert_eq!(
            first,
            Err(BootstrapOnboardingFinalizeError::NetHandleUnavailable)
        );

        let other_node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let rejected = bootstrap_onboarding_finalize(
            finalize_input(&fixture, other_node_id, ONBOARDING_SECRET_EXPIRES_AT + 1),
            fixture.context.clone(),
        )
        .await;
        assert_eq!(
            rejected,
            Err(BootstrapOnboardingFinalizeError::Reserve(
                ReserveOnboardingSecretError::AlreadyClaimed
            ))
        );
    }

    #[tokio::test]
    async fn onboarding_emits_to_realm_admins() {
        let fixture = setup_finalize_fixture().await;
        let admin_one = UserId::new(Ulid::from_bytes([40u8; 16]), fixture.realm_id);
        let admin_two = UserId::new(Ulid::from_bytes([41u8; 16]), fixture.realm_id);
        write_realm_admins(&fixture, &[admin_one, admin_two]).await;

        let first = bootstrap_onboarding_finalize(
            finalize_input(&fixture, fixture.joiner_node_id, 10),
            fixture.context.clone(),
        )
        .await;
        assert_eq!(
            first,
            Err(BootstrapOnboardingFinalizeError::NetHandleUnavailable)
        );

        let (retry_context, net_handle) = context_with_net(&fixture).await;
        let retry = bootstrap_onboarding_finalize(
            finalize_input(
                &fixture,
                fixture.joiner_node_id,
                ONBOARDING_SECRET_EXPIRES_AT + 1,
            ),
            retry_context,
        )
        .await
        .unwrap();
        assert_eq!(retry.mode, OnboardingMode::Server);

        let rows = read_outbox_rows(&fixture.storage_handle).await;
        assert_eq!(rows.len(), 2);
        let mut recipients: Vec<UserId> = rows.iter().map(|row| row.record.recipient).collect();
        recipients.sort();
        let mut expected = vec![admin_one, admin_two];
        expected.sort();
        assert_eq!(recipients, expected);
        for row in &rows {
            assert_eq!(
                row.record.kind,
                NotificationKind::NodeOnboarded {
                    realm_id: fixture.realm_id,
                    node_id: fixture.joiner_node_id,
                }
            );
        }
        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn emit_helper_warns_when_realm_auth_document_absent() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[7u8; 32]).public();

        emit_node_onboarded_notification(realm_id, node_id, &context).await;

        assert!(read_outbox_rows(&storage_handle).await.is_empty());
    }

    #[tokio::test]
    async fn onboarding_succeeds_when_admin_role_unassigned() {
        let fixture = setup_finalize_fixture().await;

        let first = bootstrap_onboarding_finalize(
            finalize_input(&fixture, fixture.joiner_node_id, 10),
            fixture.context.clone(),
        )
        .await;
        assert_eq!(
            first,
            Err(BootstrapOnboardingFinalizeError::NetHandleUnavailable)
        );

        let (retry_context, net_handle) = context_with_net(&fixture).await;
        let retry = bootstrap_onboarding_finalize(
            finalize_input(
                &fixture,
                fixture.joiner_node_id,
                ONBOARDING_SECRET_EXPIRES_AT + 1,
            ),
            retry_context,
        )
        .await
        .unwrap();
        assert_eq!(retry.mode, OnboardingMode::Server);

        assert!(read_outbox_rows(&fixture.storage_handle).await.is_empty());
        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn failed_finalize_emits_nothing() {
        let fixture = setup_finalize_fixture().await;

        let result = bootstrap_onboarding_finalize(
            finalize_input(&fixture, fixture.joiner_node_id, 10),
            fixture.context.clone(),
        )
        .await;
        assert_eq!(
            result,
            Err(BootstrapOnboardingFinalizeError::NetHandleUnavailable)
        );

        assert!(read_outbox_rows(&fixture.storage_handle).await.is_empty());
    }
}
