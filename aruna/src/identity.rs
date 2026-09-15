//! Persisted identity: the record, its store, the pure [`plan_enrollment`]
//! decision, and [`IdentityError`]. Enrollment transport and response
//! validation live in the `enrollment` submodule.

#[path = "identity_enrollment.rs"]
mod enrollment;

pub(crate) use enrollment::{
    bootstrap_node_state, onboarding_realm_endpoints, refresh_onboarding_bootstrap,
};

pub use enrollment::{EnrollmentPlan, plan_enrollment};

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keys::generate_signing_key;
use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
use aruna_core::onboarding::{OnboardingMode, OnboardingPhase, OnboardingSecretError};
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::RealmId;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey};
use serde::{Deserialize, Serialize};
use std::array::TryFromSliceError;
use std::string::FromUtf8Error;
use thiserror::Error;

/// A failure while reading, persisting, or enrolling a node identity.
#[derive(Error, Debug)]
pub enum IdentityError {
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    FromSliceError(#[from] TryFromSliceError),
    #[error(transparent)]
    Base64Error(#[from] base64::DecodeError),
    #[error(transparent)]
    SPKIError(#[from] ed25519_dalek::pkcs8::spki::Error),
    #[error(transparent)]
    PKCSError(#[from] ed25519_dalek::pkcs8::Error),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    OnboardingSecretError(#[from] OnboardingSecretError),
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error(transparent)]
    Utf8Error(#[from] FromUtf8Error),
    #[error("onboarding bootstrap failed: {0}")]
    OnboardingBootstrapFailed(String),
    #[error("missing onboarding bootstrap material for {0:?} node")]
    MissingOnboardingMaterial(OnboardingMode),
    #[error("onboarding mode mismatch between secret and bootstrap response")]
    OnboardingModeMismatch,
    #[error("unexpected storage event while loading node state: {0}")]
    UnexpectedStorageEvent(String),
}

const NODE_STATE_RECORD_KEY: &[u8] = b"node_state";

/// The persisted identity boundary.
pub struct IdentityStore {
    storage: StorageHandle,
}

impl IdentityStore {
    pub fn from_storage(storage: StorageHandle) -> Self {
        Self { storage }
    }

    pub async fn load(&self) -> Result<Option<PersistedNodeState>, IdentityError> {
        load_node_state(&self.storage).await
    }

    pub async fn persist(&self, state: &PersistedNodeState) -> Result<(), IdentityError> {
        persist_node_state(&self.storage, state).await
    }

    /// Mints a fresh realm management identity. Only a first boot with no
    /// persisted state calls this, so changing a settings type cannot rotate an
    /// existing identity.
    pub fn generate(&self) -> Result<PersistedNodeState, IdentityError> {
        generate_node_state()
    }

    pub fn capabilities(
        &self,
        state: &PersistedNodeState,
    ) -> Result<(RealmId, NodeCapabilities), IdentityError> {
        node_capabilities(state)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BootOrigin {
    InitializedRealm,
    Onboarded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedNodeStatus {
    PendingInitialization,
    PendingOnboarding,
    Complete,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedNodeIdentity {
    Management {
        realm_private_key_pem: String,
    },
    Server {
        issuer_private_key_pem: String,
        delegation_signature: String,
    },
    /// Owner-bound device. The owner is copied from the enrollment answer: a
    /// device holds no realm state to read it back from before it has joined.
    User {
        owner: UserId,
    },
}

impl std::fmt::Debug for PersistedNodeIdentity {
    /// Redacts private key material: this record may be printed in a diagnostic
    /// path, and keys are not diagnostic output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Management { .. } => f
                .debug_struct("Management")
                .field("realm_private_key_pem", &"<redacted>")
                .finish(),
            Self::Server { .. } => f
                .debug_struct("Server")
                .field("issuer_private_key_pem", &"<redacted>")
                .field("delegation_signature", &"<redacted>")
                .finish(),
            Self::User { owner } => f.debug_struct("User").field("owner", owner).finish(),
        }
    }
}

impl PersistedNodeIdentity {
    /// Owner of a device identity; `None` for infrastructure nodes.
    pub fn owner(&self) -> Option<UserId> {
        match self {
            Self::User { owner } => Some(*owner),
            Self::Management { .. } | Self::Server { .. } => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedNodeState {
    pub boot_origin: BootOrigin,
    pub status: PersistedNodeStatus,
    pub realm_id: RealmId,
    pub net_secret_key: [u8; 32],
    pub onboarding_phase: Option<OnboardingPhase>,
    pub onboarding_sync_ticket: Option<String>,
    pub identity: PersistedNodeIdentity,
}

impl std::fmt::Debug for PersistedNodeState {
    /// Redacts the network secret; the rest of the record is diagnostic.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistedNodeState")
            .field("boot_origin", &self.boot_origin)
            .field("status", &self.status)
            .field("realm_id", &self.realm_id)
            .field("net_secret_key", &"<redacted>")
            .field("onboarding_phase", &self.onboarding_phase)
            .field("onboarding_sync_ticket", &self.onboarding_sync_ticket)
            .field("identity", &self.identity)
            .finish()
    }
}

pub(crate) fn node_capabilities(
    node_state: &PersistedNodeState,
) -> Result<(RealmId, NodeCapabilities), IdentityError> {
    match &node_state.identity {
        PersistedNodeIdentity::Management {
            realm_private_key_pem,
        } => {
            let realm_signing_key = SigningKey::from_pkcs8_pem(realm_private_key_pem)?;
            let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
            let realm_verifying_key = realm_signing_key
                .verifying_key()
                .to_public_key_pem(LineEnding::default())?
                .as_bytes()
                .try_into()?;
            let realm_encoding_key = realm_signing_key
                .to_pkcs8_pem(LineEnding::default())?
                .as_bytes()
                .try_into()?;

            Ok((
                realm_id,
                NodeCapabilities::Management {
                    realm_signing_key,
                    realm_verifying_key,
                    realm_encoding_key,
                },
            ))
        }
        PersistedNodeIdentity::Server {
            issuer_private_key_pem,
            delegation_signature,
        } => Ok((
            node_state.realm_id,
            NodeCapabilities::server_node(
                SigningKey::from_pkcs8_pem(issuer_private_key_pem)?,
                node_state.realm_id,
                delegation_signature.clone(),
            )?,
        )),
        PersistedNodeIdentity::User { .. } => Ok((
            node_state.realm_id,
            NodeCapabilities::user_node(node_state.realm_id)?,
        )),
    }
}

pub(crate) fn generate_node_state() -> Result<PersistedNodeState, IdentityError> {
    let realm_signing_key = generate_signing_key();
    let node_signing_key = generate_signing_key();

    Ok(PersistedNodeState {
        boot_origin: BootOrigin::InitializedRealm,
        status: PersistedNodeStatus::PendingInitialization,
        realm_id: RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes()),
        net_secret_key: node_signing_key.to_bytes(),
        onboarding_phase: None,
        onboarding_sync_ticket: None,
        identity: PersistedNodeIdentity::Management {
            realm_private_key_pem: realm_signing_key
                .to_pkcs8_pem(LineEnding::default())?
                .to_string(),
        },
    })
}

pub(crate) async fn load_node_state(
    storage: &StorageHandle,
) -> Result<Option<PersistedNodeState>, IdentityError> {
    match storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: ByteView::from(NODE_STATE_RECORD_KEY),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => Ok(Some(
            postcard::from_bytes(&bytes).map_err(ConversionError::from)?,
        )),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(IdentityError::UnexpectedStorageEvent(format!("{other:?}"))),
    }
}

pub(crate) async fn persist_node_state(
    storage: &StorageHandle,
    node_state: &PersistedNodeState,
) -> Result<(), IdentityError> {
    let value = postcard::to_allocvec(node_state).map_err(ConversionError::from)?;
    match storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: ByteView::from(NODE_STATE_RECORD_KEY),
            value: ByteView::from(value),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(IdentityError::UnexpectedStorageEvent(format!("{other:?}"))),
    }
}
/// Marks a persisted state complete. The transition lives with the record it
/// writes, not with the configuration that reads it.
pub async fn mark_state_complete(
    storage: &StorageHandle,
    node_state: &PersistedNodeState,
) -> Result<(), IdentityError> {
    if matches!(node_state.status, PersistedNodeStatus::Complete) {
        return Ok(());
    }

    let mut updated_state = node_state.clone();
    updated_state.status = PersistedNodeStatus::Complete;
    updated_state.onboarding_phase = None;
    updated_state.onboarding_sync_ticket = None;
    persist_node_state(storage, &updated_state).await
}

/// Records the onboarding phase a pending joiner reached.
pub async fn mark_onboarding_phase(
    storage: &StorageHandle,
    node_state: &PersistedNodeState,
    phase: OnboardingPhase,
) -> Result<(), IdentityError> {
    let mut updated_state = node_state.clone();
    updated_state.onboarding_phase = Some(phase);
    persist_node_state(storage, &updated_state).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;

    /// The persisted record keeps its byte layout across the ownership move:
    /// the stored encoding is part of the deployed compatibility surface.
    #[test]
    fn persisted_state_stable() {
        let state = PersistedNodeState {
            boot_origin: BootOrigin::InitializedRealm,
            status: PersistedNodeStatus::PendingInitialization,
            realm_id: RealmId::from_bytes([1u8; 32]),
            net_secret_key: [2u8; 32],
            onboarding_phase: None,
            onboarding_sync_ticket: None,
            identity: PersistedNodeIdentity::Management {
                realm_private_key_pem: "pem".to_string(),
            },
        };
        let encoded = postcard::to_allocvec(&state).expect("state encodes");
        let decoded: PersistedNodeState = postcard::from_bytes(&encoded).expect("state decodes");
        assert_eq!(decoded, state);
        // A fixed field order check: the first field is the boot origin tag.
        assert_eq!(encoded[0], 0, "boot origin keeps its encoded variant tag");
    }

    /// Frozen bytes from the prior persisted format, one per identity shape and
    /// onboarding state. Never regenerate them by the current encoder: a layout
    /// change must fail decode, not just a malformed-bytes check.
    #[test]
    fn historical_fixtures_decode() {
        let fixtures: &[(&str, &str, PersistedNodeState)] = &[
            (
                "management pending initialization",
                "0000010101010101010101010101010101010101010101010101010101010101010102020202020202020202020202020202020202020202020202020202020202020000000370656d",
                PersistedNodeState {
                    boot_origin: BootOrigin::InitializedRealm,
                    status: PersistedNodeStatus::PendingInitialization,
                    realm_id: RealmId::from_bytes([1u8; 32]),
                    net_secret_key: [2u8; 32],
                    onboarding_phase: None,
                    onboarding_sync_ticket: None,
                    identity: PersistedNodeIdentity::Management {
                        realm_private_key_pem: "pem".to_string(),
                    },
                },
            ),
            (
                "user complete",
                "0102030303030303030303030303030303030303030303030303030303030303030304040404040404040404040404040404040404040404040404040404040404040000021a30303030303030303030303030303030303030303030303030300303030303030303030303030303030303030303030303030303030303030303",
                PersistedNodeState {
                    boot_origin: BootOrigin::Onboarded,
                    status: PersistedNodeStatus::Complete,
                    realm_id: RealmId::from_bytes([3u8; 32]),
                    net_secret_key: [4u8; 32],
                    onboarding_phase: None,
                    onboarding_sync_ticket: None,
                    identity: PersistedNodeIdentity::User {
                        owner: UserId::nil(RealmId::from_bytes([3u8; 32])),
                    },
                },
            ),
            (
                "user pending onboarding",
                "010105050505050505050505050505050505050505050505050505050505050505050606060606060606060606060606060606060606060606060606060606060606010001067469636b6574021a30303030303030303030303030303030303030303030303030300505050505050505050505050505050505050505050505050505050505050505",
                PersistedNodeState {
                    boot_origin: BootOrigin::Onboarded,
                    status: PersistedNodeStatus::PendingOnboarding,
                    realm_id: RealmId::from_bytes([5u8; 32]),
                    net_secret_key: [6u8; 32],
                    onboarding_phase: Some(OnboardingPhase::Bootstrapped),
                    onboarding_sync_ticket: Some("ticket".to_string()),
                    identity: PersistedNodeIdentity::User {
                        owner: UserId::nil(RealmId::from_bytes([5u8; 32])),
                    },
                },
            ),
        ];

        for (label, encoded_hex, expected) in fixtures {
            let encoded =
                hex::decode(encoded_hex).unwrap_or_else(|error| panic!("{label}: {error}"));
            let decoded: PersistedNodeState = postcard::from_bytes(&encoded)
                .unwrap_or_else(|error| panic!("{label}: historical bytes must decode: {error}"));
            assert_eq!(&decoded, expected, "{label}: decoded value");
            assert_eq!(
                postcard::to_allocvec(&decoded).expect("state encodes"),
                encoded,
                "{label}: the encoding is part of the persisted format"
            );
        }
    }

    #[tokio::test]
    async fn boot_reuses_identity() {
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let store = IdentityStore::from_storage(storage.clone());

        assert!(store.load().await.unwrap().is_none());
        let generated = store.generate().unwrap();
        store.persist(&generated).await.unwrap();

        let (derived_realm, capabilities) = store.capabilities(&generated).unwrap();
        assert_eq!(derived_realm, generated.realm_id);
        assert!(matches!(capabilities, NodeCapabilities::Management { .. }));

        drop(store);
        drop(storage);
        let reopened = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let store = IdentityStore::from_storage(reopened);
        let loaded = store
            .load()
            .await
            .unwrap()
            .expect("a persisted identity must load on the repeat boot");
        assert_eq!(loaded, generated);
        assert_eq!(
            plan_enrollment(Some(&loaded), false).unwrap(),
            EnrollmentPlan::Ready
        );
    }

    #[tokio::test]
    async fn corrupt_identity_rejected() {
        use aruna_core::effects::Effect;
        use aruna_core::handle::Handle;
        use aruna_core::keyspaces::NODE_STATE_KEYSPACE;

        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        storage
            .send_effect(Effect::Storage(aruna_core::effects::StorageEffect::Write {
                key_space: NODE_STATE_KEYSPACE.to_string(),
                key: b"node_state".to_vec().into(),
                value: b"not-postcard".to_vec().into(),
                txn_id: None,
            }))
            .await;

        let store = IdentityStore::from_storage(storage);
        assert!(store.load().await.is_err(), "corrupt bytes must not decode");
    }

    #[test]
    fn realm_mismatch_detected() {
        // Only a management identity derives its realm from key material; a
        // corrupted realm field must be detectable against the derived one.
        let (storage, _receivers) = aruna_storage::StorageHandle::new();
        let store = IdentityStore::from_storage(storage);
        let mut management = store.generate().unwrap();
        let (derived, capabilities) = store.capabilities(&management).unwrap();
        assert_eq!(derived, management.realm_id);
        assert!(matches!(capabilities, NodeCapabilities::Management { .. }));

        management.realm_id = RealmId::from_bytes([0u8; 32]);
        let (derived, _) = store.capabilities(&management).unwrap();
        assert_ne!(
            derived, management.realm_id,
            "a mismatched persisted realm must be detectable"
        );
    }
}
