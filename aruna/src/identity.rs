//! Persisted identity and enrollment decisions.
//!
//! [`IdentityStore`] is the only reader and writer of the persisted identity
//! record; `Config::resolve` opens storage explicitly and wraps it at that call
//! site. Whether a boot must mint, bootstrap, refresh, or reuse an identity is
//! a pure function ([`plan_enrollment`]); the HTTP client lives in
//! `crate::config` and runs only when the plan asks for it.

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
use aruna_core::onboarding::OnboardingPhase;
use aruna_core::structs::{NodeCapabilities, RealmId, StaticRealmEndpoint};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};

use crate::config::{
    SetupError, generate_node_state, node_capabilities, onboarding_realm_endpoints,
};

/// What this boot must do with the persisted identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnrollmentPlan {
    /// No identity exists and no secret is configured: mint a realm node.
    Generate,
    /// No identity exists and a secret is configured: enroll with the seed.
    Bootstrap,
    /// A pending bootstrapped enrollment must refresh its bootstrap material.
    Refresh,
    /// The stored identity is usable as-is.
    Ready,
}

/// The pure enrollment decision, testable without storage or HTTP.
pub fn plan_enrollment(
    state: Option<&PersistedNodeState>,
    has_onboarding_secret: bool,
) -> Result<EnrollmentPlan, SetupError> {
    match state {
        None if has_onboarding_secret => Ok(EnrollmentPlan::Bootstrap),
        None => Ok(EnrollmentPlan::Generate),
        Some(state) if matches!(state.status, PersistedNodeStatus::PendingOnboarding) => {
            let phase = state
                .onboarding_phase
                .unwrap_or(OnboardingPhase::Bootstrapped);
            if matches!(phase, OnboardingPhase::Bootstrapped) {
                if has_onboarding_secret {
                    Ok(EnrollmentPlan::Refresh)
                } else {
                    Err(SetupError::OnboardingBootstrapFailed(
                        "pending bootstrapped onboarding requires ONBOARDING_SECRET to refresh bootstrap material"
                            .to_string(),
                    ))
                }
            } else {
                Ok(EnrollmentPlan::Ready)
            }
        }
        Some(_) => Ok(EnrollmentPlan::Ready),
    }
}

/// The persisted identity boundary.
pub struct IdentityStore {
    storage: StorageHandle,
}

impl IdentityStore {
    pub fn open(storage: StorageHandle) -> Self {
        Self { storage }
    }

    pub async fn load(&self) -> Result<Option<PersistedNodeState>, SetupError> {
        load_node_state(&self.storage).await
    }

    pub async fn persist(&self, state: &PersistedNodeState) -> Result<(), SetupError> {
        persist_node_state(&self.storage, state).await
    }

    /// Mints a fresh realm management identity. Only a first boot with no
    /// persisted state calls this, so changing a settings type cannot rotate an
    /// existing identity.
    pub fn generate(&self) -> Result<PersistedNodeState, SetupError> {
        generate_node_state()
    }

    pub fn capabilities(
        &self,
        state: &PersistedNodeState,
    ) -> Result<(RealmId, NodeCapabilities), SetupError> {
        node_capabilities(state)
    }

    /// Realm endpoints handed over at enrollment. An entry that does not parse,
    /// or whose address names another node, is dropped.
    pub fn onboarding_endpoints(endpoints: &[StaticRealmEndpoint]) -> Vec<EndpointAddr> {
        onboarding_realm_endpoints(endpoints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::UserId;

    fn state(status: PersistedNodeStatus, phase: Option<OnboardingPhase>) -> PersistedNodeState {
        let realm_signing_key = generate_signing_key();
        PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status,
            realm_id: RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes()),
            net_secret_key: generate_signing_key().to_bytes(),
            onboarding_phase: phase,
            onboarding_sync_ticket: None,
            identity: PersistedNodeIdentity::User {
                owner: UserId::nil(RealmId::from_bytes([9u8; 32])),
            },
        }
    }

    #[test]
    fn enrollment_plan_table() {
        // First boot: a secret decides bootstrap versus local generation.
        assert_eq!(
            plan_enrollment(None, false).unwrap(),
            EnrollmentPlan::Generate
        );
        assert_eq!(
            plan_enrollment(None, true).unwrap(),
            EnrollmentPlan::Bootstrap
        );

        // Pending bootstrapped enrollment refreshes with a secret and fails
        // without one.
        let pending = state(
            PersistedNodeStatus::PendingOnboarding,
            Some(OnboardingPhase::Bootstrapped),
        );
        assert_eq!(
            plan_enrollment(Some(&pending), true).unwrap(),
            EnrollmentPlan::Refresh
        );
        assert!(matches!(
            plan_enrollment(Some(&pending), false),
            Err(SetupError::OnboardingBootstrapFailed(_))
        ));

        // A phase past the bootstrap refresh and a completed node are ready.
        let fetched = state(
            PersistedNodeStatus::PendingOnboarding,
            Some(OnboardingPhase::CoreDocumentsFetched),
        );
        assert_eq!(
            plan_enrollment(Some(&fetched), false).unwrap(),
            EnrollmentPlan::Ready
        );
        let complete = state(PersistedNodeStatus::Complete, None);
        assert_eq!(
            plan_enrollment(Some(&complete), false).unwrap(),
            EnrollmentPlan::Ready
        );
    }

    #[tokio::test]
    async fn first_boot_generates_and_repeat_boot_reuses() {
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let store = IdentityStore::open(storage.clone());

        assert!(store.load().await.unwrap().is_none());
        let generated = store.generate().unwrap();
        store.persist(&generated).await.unwrap();

        let (derived_realm, capabilities) = store.capabilities(&generated).unwrap();
        assert_eq!(derived_realm, generated.realm_id);
        assert!(matches!(capabilities, NodeCapabilities::Management { .. }));

        drop(store);
        drop(storage);
        let reopened = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let store = IdentityStore::open(reopened);
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
    async fn corrupt_identity_is_an_error() {
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

        let store = IdentityStore::open(storage);
        assert!(store.load().await.is_err(), "corrupt bytes must not decode");
    }

    #[test]
    fn configured_realm_must_match_the_persisted_realm() {
        // Only a management identity derives its realm from key material; a
        // corrupted realm field must be detectable against the derived one.
        let (storage, _receivers) = aruna_storage::StorageHandle::new();
        let store = IdentityStore::open(storage);
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

const NODE_STATE_RECORD_KEY: &[u8] = b"node_state";
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

pub(crate) async fn load_node_state(
    storage: &StorageHandle,
) -> Result<Option<PersistedNodeState>, SetupError> {
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
        other => Err(SetupError::UnexpectedStorageEvent(format!("{other:?}"))),
    }
}

pub(crate) async fn persist_node_state(
    storage: &StorageHandle,
    node_state: &PersistedNodeState,
) -> Result<(), SetupError> {
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
        other => Err(SetupError::UnexpectedStorageEvent(format!("{other:?}"))),
    }
}
