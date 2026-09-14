//! Persisted identity: schema, store, and enrollment transport.
//!
//! The persisted record and its transitions ([`PersistedNodeState`],
//! [`IdentityStore`], [`mark_state_complete`], [`mark_onboarding_phase`])
//! come first; whether a boot must mint, bootstrap, refresh, or reuse an
//! identity is the pure decision [`plan_enrollment`]; the enrollment HTTP
//! client and its response decode run only when the plan asks for them.
//! `Config::resolve` opens storage explicitly and hands the handle to
//! [`IdentityStore::from_storage`] at that call site.

use std::collections::BTreeMap;
use std::time::Duration;

use aruna_core::UserId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keys::generate_signing_key;
use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
use aruna_core::onboarding::{
    BootstrapOnboardingRequest, BootstrapOnboardingResponse, OnboardingMode, OnboardingPhase,
    OnboardingSecret, OnboardingSyncTicket, issuer_proof_message, node_proof_message,
};
use aruna_core::structs::{NodeCapabilities, RealmId, StaticRealmEndpoint};
use aruna_core::time::unix_timestamp_secs;
use aruna_net::parse_endpoint_config;
use aruna_storage::StorageHandle;
use base64::Engine;
use byteview::ByteView;
use crypto_box::{
    PublicKey as TransportPublicKey, SalsaBox, SecretKey as TransportSecretKey,
    aead::{Aead, OsRng as CryptoOsRng},
};
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey};
use ed25519_dalek::{Signer, SigningKey};
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};

use crate::config::SetupError;

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

const NODE_STATE_RECORD_KEY: &[u8] = b"node_state";

/// The persisted identity boundary.
pub struct IdentityStore {
    storage: StorageHandle,
}

impl IdentityStore {
    pub fn from_storage(storage: StorageHandle) -> Self {
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

const ONBOARDING_BOOTSTRAP_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn node_capabilities(
    node_state: &PersistedNodeState,
) -> Result<(RealmId, NodeCapabilities), SetupError> {
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

pub(crate) fn generate_node_state() -> Result<PersistedNodeState, SetupError> {
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

pub(crate) struct BootstrappedNodeState {
    pub(crate) node_state: PersistedNodeState,
    pub(crate) temporary_bootstrap_endpoint: EndpointAddr,
    pub(crate) realm_endpoints: Vec<EndpointAddr>,
}

/// Realm endpoints handed over at enrollment, so a joiner dials the realm
/// without a discovery read of its own. An entry that does not parse, or whose
/// address names another node, is dropped instead of failing the join.
pub(crate) fn onboarding_realm_endpoints(endpoints: &[StaticRealmEndpoint]) -> Vec<EndpointAddr> {
    endpoints
        .iter()
        .filter_map(
            |endpoint| match parse_endpoint_config(&endpoint.endpoint_addr) {
                Ok(endpoint_addr) if endpoint_addr.id.to_string() == endpoint.node_id => {
                    Some(endpoint_addr)
                }
                Ok(_) => {
                    tracing::warn!(
                        node_id = %endpoint.node_id,
                        "Enrollment endpoint address names a different node; ignoring it"
                    );
                    None
                }
                Err(message) => {
                    tracing::warn!(
                        node_id = %endpoint.node_id,
                        %message,
                        "Enrollment handed over an unusable realm endpoint; ignoring it"
                    );
                    None
                }
            },
        )
        .collect()
}

/// The decode half of the enrollment client, separated so response mapping is
/// testable without an HTTP server.
fn decode_bootstrap_response(body: &[u8]) -> Result<BootstrapOnboardingResponse, SetupError> {
    serde_json::from_slice(body).map_err(|error| {
        SetupError::OnboardingBootstrapFailed(format!("invalid bootstrap response: {error}"))
    })
}

pub(crate) async fn bootstrap_node_state(
    onboarding_secret: &str,
    node_location: Option<String>,
    node_weight: Option<u32>,
    node_labels: BTreeMap<String, String>,
    timeout: Duration,
) -> Result<BootstrappedNodeState, SetupError> {
    let decoded_secret = OnboardingSecret::decode(onboarding_secret)?;
    let node_signing_key = generate_signing_key();
    let net_secret_key = node_signing_key.to_bytes();
    let node_id = iroh::SecretKey::from_bytes(&net_secret_key).public();

    let issuer_signing_key = if matches!(decoded_secret.mode, OnboardingMode::Server) {
        Some(generate_signing_key())
    } else {
        None
    };
    let issuer_public_key = issuer_signing_key.as_ref().map(|issuer_signing_key| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer_signing_key.verifying_key().to_bytes())
    });
    let transport_secret_key = if matches!(decoded_secret.mode, OnboardingMode::Management) {
        Some(TransportSecretKey::generate(&mut CryptoOsRng))
    } else {
        None
    };
    let transport_public_key = transport_secret_key.as_ref().map(|transport_secret_key| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(transport_secret_key.public_key().as_bytes())
    });
    let node_id_string = node_id.to_string();
    let node_proof = node_signing_key
        .sign(&node_proof_message(
            onboarding_secret,
            &node_id_string,
            transport_public_key.as_deref(),
        ))
        .to_string();
    let issuer_proof = issuer_signing_key
        .as_ref()
        .zip(issuer_public_key.as_ref())
        .map(|(issuer_signing_key, issuer_public_key)| {
            issuer_signing_key
                .sign(&issuer_proof_message(
                    onboarding_secret,
                    &node_id_string,
                    issuer_public_key,
                ))
                .to_string()
        });

    let response = onboarding_bootstrap_client(timeout)?
        .post(format!(
            "{}/api/v1/access/onboarding/bootstrap",
            decoded_secret.seed_url.trim_end_matches('/'),
        ))
        .json(&BootstrapOnboardingRequest {
            onboarding_secret: onboarding_secret.to_string(),
            node_id: node_id_string,
            node_proof,
            transport_public_key,
            issuer_public_key,
            issuer_proof,
            node_location,
            node_weight,
            node_labels,
        })
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(SetupError::OnboardingBootstrapFailed(format!(
            "bootstrap endpoint returned {}",
            response.status()
        )));
    }

    let body = response.bytes().await?;
    let response = decode_bootstrap_response(&body)?;
    if response.mode != decoded_secret.mode {
        return Err(SetupError::OnboardingModeMismatch);
    }
    let realm_id = response.realm_id()?;
    validate_bootstrap_response(&response, decoded_secret.mode, realm_id, node_id)?;
    let temporary_bootstrap_endpoint = response.temporary_bootstrap_endpoint.clone();
    let realm_endpoints = onboarding_realm_endpoints(&response.realm_endpoints);
    let identity =
        match response.mode {
            OnboardingMode::Management => {
                let wrapped_key = response.wrapped_realm_private_key.ok_or(
                    SetupError::MissingOnboardingMaterial(OnboardingMode::Management),
                )?;
                let wrapped_nonce = response.wrapped_realm_private_key_nonce.ok_or(
                    SetupError::MissingOnboardingMaterial(OnboardingMode::Management),
                )?;
                let wrapping_public_key =
                    response
                        .wrapping_public_key
                        .ok_or(SetupError::MissingOnboardingMaterial(
                            OnboardingMode::Management,
                        ))?;
                let transport_secret_key = transport_secret_key.ok_or(
                    SetupError::MissingOnboardingMaterial(OnboardingMode::Management),
                )?;
                let wrapping_public_key_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(wrapping_public_key)
                    .map_err(SetupError::Base64Error)?;
                let wrapping_public_key = TransportPublicKey::from(
                    <[u8; 32]>::try_from(wrapping_public_key_bytes.as_slice())
                        .map_err(SetupError::FromSliceError)?,
                );
                let cipher = SalsaBox::new(&wrapping_public_key, &transport_secret_key);
                let nonce_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(wrapped_nonce)
                    .map_err(SetupError::Base64Error)?;
                let nonce = crypto_box::Nonce::from(
                    <[u8; 24]>::try_from(nonce_bytes.as_slice())
                        .map_err(SetupError::FromSliceError)?,
                );
                let ciphertext = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(wrapped_key)
                    .map_err(SetupError::Base64Error)?;
                let realm_private_key_pem =
                    String::from_utf8(cipher.decrypt(&nonce, ciphertext.as_ref()).map_err(
                        |error| SetupError::OnboardingBootstrapFailed(error.to_string()),
                    )?)?;

                PersistedNodeIdentity::Management {
                    realm_private_key_pem,
                }
            }
            OnboardingMode::Server => PersistedNodeIdentity::Server {
                issuer_private_key_pem: issuer_signing_key
                    .ok_or(SetupError::MissingOnboardingMaterial(
                        OnboardingMode::Server,
                    ))?
                    .to_pkcs8_pem(LineEnding::default())?
                    .to_string(),
                delegation_signature: response.delegation_signature.ok_or(
                    SetupError::MissingOnboardingMaterial(OnboardingMode::Server),
                )?,
            },
            // Devices carry no issuer keys. Their finalized membership grants owner authority,
            // and the saved owner identifies that authority before the record arrives.
            OnboardingMode::User { owner } => PersistedNodeIdentity::User { owner },
        };

    Ok(BootstrappedNodeState {
        temporary_bootstrap_endpoint,
        realm_endpoints,
        node_state: PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status: PersistedNodeStatus::PendingOnboarding,
            realm_id,
            net_secret_key,
            onboarding_phase: Some(OnboardingPhase::Bootstrapped),
            onboarding_sync_ticket: Some(response.onboarding_sync_ticket),
            identity,
        },
    })
}

pub(crate) async fn refresh_onboarding_bootstrap(
    onboarding_secret: &str,
    node_state: &PersistedNodeState,
    node_location: Option<String>,
    node_weight: Option<u32>,
    node_labels: BTreeMap<String, String>,
    timeout: Duration,
) -> Result<BootstrapOnboardingResponse, SetupError> {
    let decoded_secret = OnboardingSecret::decode(onboarding_secret)?;
    let node_signing_key = SigningKey::from_bytes(&node_state.net_secret_key);
    let node_id = iroh::SecretKey::from_bytes(&node_state.net_secret_key).public();

    let mut transport_secret_key = None;
    let transport_public_key = if matches!(decoded_secret.mode, OnboardingMode::Management) {
        let secret_key = TransportSecretKey::generate(&mut CryptoOsRng);
        let public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(secret_key.public_key().as_bytes());
        transport_secret_key = Some(secret_key);
        Some(public_key)
    } else {
        None
    };

    let issuer_signing_key = match (&decoded_secret.mode, &node_state.identity) {
        (
            OnboardingMode::Server,
            PersistedNodeIdentity::Server {
                issuer_private_key_pem,
                ..
            },
        ) => Some(SigningKey::from_pkcs8_pem(issuer_private_key_pem)?),
        (OnboardingMode::Server, _) => {
            return Err(SetupError::MissingOnboardingMaterial(
                OnboardingMode::Server,
            ));
        }
        _ => None,
    };
    let issuer_public_key = issuer_signing_key.as_ref().map(|issuer_signing_key| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer_signing_key.verifying_key().to_bytes())
    });
    let node_id_string = node_id.to_string();
    let node_proof = node_signing_key
        .sign(&node_proof_message(
            onboarding_secret,
            &node_id_string,
            transport_public_key.as_deref(),
        ))
        .to_string();
    let issuer_proof = issuer_signing_key
        .as_ref()
        .zip(issuer_public_key.as_ref())
        .map(|(issuer_signing_key, issuer_public_key)| {
            issuer_signing_key
                .sign(&issuer_proof_message(
                    onboarding_secret,
                    &node_id_string,
                    issuer_public_key,
                ))
                .to_string()
        });

    let response = onboarding_bootstrap_client(timeout)?
        .post(format!(
            "{}/api/v1/access/onboarding/bootstrap",
            decoded_secret.seed_url.trim_end_matches('/'),
        ))
        .json(&BootstrapOnboardingRequest {
            onboarding_secret: onboarding_secret.to_string(),
            node_id: node_id_string,
            node_proof,
            transport_public_key,
            issuer_public_key,
            issuer_proof,
            node_location,
            node_weight,
            node_labels,
        })
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(SetupError::OnboardingBootstrapFailed(format!(
            "bootstrap endpoint returned {}",
            response.status()
        )));
    }

    let body = response.bytes().await?;
    let response = decode_bootstrap_response(&body)?;
    if response.mode != decoded_secret.mode {
        return Err(SetupError::OnboardingModeMismatch);
    }
    let response_realm_id = response.realm_id()?;
    if response_realm_id != node_state.realm_id {
        return Err(SetupError::OnboardingBootstrapFailed(
            "bootstrap response realm does not match persisted node state".to_string(),
        ));
    }
    validate_bootstrap_response(&response, decoded_secret.mode, node_state.realm_id, node_id)?;

    drop(transport_secret_key);
    Ok(response)
}

fn onboarding_bootstrap_client(timeout: Duration) -> Result<reqwest::Client, SetupError> {
    Ok(reqwest::Client::builder()
        .connect_timeout(ONBOARDING_BOOTSTRAP_HTTP_CONNECT_TIMEOUT)
        .timeout(timeout)
        .build()?)
}

fn validate_bootstrap_response(
    response: &BootstrapOnboardingResponse,
    expected_mode: OnboardingMode,
    expected_realm_id: RealmId,
    expected_node_id: iroh::PublicKey,
) -> Result<(), SetupError> {
    if response.mode != expected_mode {
        return Err(SetupError::OnboardingModeMismatch);
    }
    if response.realm_id()? != expected_realm_id {
        return Err(SetupError::OnboardingBootstrapFailed(
            "bootstrap response realm does not match expected realm".to_string(),
        ));
    }

    let ticket = OnboardingSyncTicket::decode(&response.onboarding_sync_ticket)?;
    if ticket.payload.realm_id != expected_realm_id.to_string() {
        return Err(SetupError::OnboardingBootstrapFailed(
            "onboarding sync ticket realm does not match bootstrap response".to_string(),
        ));
    }
    if ticket.payload.node_id != expected_node_id.to_string() {
        return Err(SetupError::OnboardingBootstrapFailed(
            "onboarding sync ticket node does not match local node".to_string(),
        ));
    }
    ticket.verify(
        expected_node_id,
        &DocumentSyncTarget::RealmConfig {
            realm_id: expected_realm_id,
        },
        unix_timestamp_secs(),
    )?;

    Ok(())
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
/// Marks a persisted state complete. The transition lives with the record it
/// writes, not with the configuration that reads it.
pub async fn mark_state_complete(
    storage: &StorageHandle,
    node_state: &PersistedNodeState,
) -> Result<(), SetupError> {
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
) -> Result<(), SetupError> {
    let mut updated_state = node_state.clone();
    updated_state.onboarding_phase = Some(phase);
    persist_node_state(storage, &updated_state).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::keys::generate_signing_key;

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
    fn bootstrap_response_decode_is_a_boundary() {
        assert!(matches!(
            decode_bootstrap_response(b"not json"),
            Err(SetupError::OnboardingBootstrapFailed(_))
        ));
    }

    /// The persisted record keeps its byte layout across the ownership move:
    /// the stored encoding is part of the deployed compatibility surface.
    #[test]
    fn persisted_state_bytes_are_stable() {
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
    /// onboarding state. These constants are never regenerated by the current
    /// encoder: an incompatible field-layout change must fail the decode or the
    /// exact re-encode, not merely a malformed-bytes check.
    #[test]
    fn persisted_state_historical_fixtures_decode_exactly() {
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

        let store = IdentityStore::from_storage(storage);
        assert!(store.load().await.is_err(), "corrupt bytes must not decode");
    }

    #[test]
    fn configured_realm_must_match_the_persisted_realm() {
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
