//! Defines onboarding secrets and tickets and the signed proofs that enroll a node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::NodeId;
use crate::UserId;
use crate::auth::credential_hash;
use crate::document::DocumentTarget;
use crate::structs::identity::realm::{RealmId, StaticRealmEndpoint};
use base64::Engine;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnboardingMode {
    Management,
    Server,
    /// Owner-bound device enrollment. The owner is fixed when the secret is
    /// minted and copied into the membership record at finalize, so a device
    /// secret can never enroll a node for anybody else.
    User {
        owner: UserId,
    },
}

impl OnboardingMode {
    /// Owner a device secret is bound to; `None` for infrastructure modes.
    pub fn owner(&self) -> Option<UserId> {
        match self {
            Self::User { owner } => Some(*owner),
            Self::Management | Self::Server => None,
        }
    }
}

/// Distinguishes what a secret may be redeemed for. An initial-administrator
/// token must never be interchangeable with a node-enrollment secret.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnboardingPurpose {
    NodeEnrollment,
    InitialAdministrator,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingSecret {
    pub seed_url: String,
    pub enrollment_id: Ulid,
    pub secret: [u8; 32],
    pub mode: OnboardingMode,
    pub realm_id: RealmId,
    pub purpose: OnboardingPurpose,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingSecretRecord {
    pub enrollment_id: Ulid,
    pub secret_hash: String,
    pub mode: OnboardingMode,
    pub purpose: OnboardingPurpose,
    pub expires_at: u64,
    pub claimed_node_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingStateRecord {
    pub enrollment_id: Ulid,
    pub state: OnboardingSecretState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnboardingSecretState {
    Available,
    Reserved { node_id: String, expires_at: u64 },
    Finalizing { node_id: String },
    Consumed { node_id: String },
}

impl OnboardingSecretState {
    pub fn claimed_node_id(&self) -> Option<&str> {
        match self {
            Self::Available => None,
            Self::Reserved { node_id, .. }
            | Self::Finalizing { node_id }
            | Self::Consumed { node_id } => Some(node_id),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapOnboardingRequest {
    pub onboarding_secret: String,
    pub node_id: String,
    pub node_proof: String,
    pub transport_public_key: Option<String>,
    pub issuer_public_key: Option<String>,
    pub issuer_proof: Option<String>,
    /// Joiner's placement location (`None` ⇒ realm default). Carried so the
    /// finalizing node can seed the joiner's placement map entry.
    pub node_location: Option<String>,
    /// Joiner's placement weight (`None` ⇒ default weight).
    pub node_weight: Option<u32>,
    /// Joiner's placement labels, carried into the seeded placement map entry.
    pub node_labels: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapOnboardingResponse {
    pub realm_id: String,
    pub mode: OnboardingMode,
    pub temporary_bootstrap_endpoint: EndpointAddr,
    #[serde(rename = "wrapped_realm_private_key")]
    pub wrapped_realm_key: Option<String>,
    #[serde(rename = "wrapped_realm_private_key_nonce")]
    pub wrapped_key_nonce: Option<String>,
    pub wrapping_public_key: Option<String>,
    pub delegation_signature: Option<String>,
    pub onboarding_sync_ticket: String,
    /// Sync-eligible realm endpoints the joiner may dial straight away, so a
    /// device with no DHT read of its own still reaches the realm.
    pub realm_endpoints: Vec<StaticRealmEndpoint>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnboardingPhase {
    Bootstrapped,
    CoreDocumentsFetched,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingTicketPayload {
    pub realm_id: String,
    pub node_id: String,
    pub expires_at: u64,
    pub documents: Vec<DocumentTarget>,
    pub group_deletions: Vec<crate::structs::identity::group_delete::GroupDeleteRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingTicket {
    pub payload: OnboardingTicketPayload,
    pub signature: String,
}

/// Mode a mint request asks for. A device secret carries no owner here: the
/// owner is taken from the caller's credential, so the body cannot name one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestedOnboardingMode {
    Management,
    Server,
    User,
}

impl From<OnboardingMode> for RequestedOnboardingMode {
    fn from(mode: OnboardingMode) -> Self {
        match mode {
            OnboardingMode::Management => Self::Management,
            OnboardingMode::Server => Self::Server,
            OnboardingMode::User { .. } => Self::User,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateSecretRequest {
    pub seed_url: String,
    pub mode: RequestedOnboardingMode,
    pub expires_in_seconds: Option<u64>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateSecretResponse {
    pub onboarding_secret: String,
    /// Identifier of the minted enrollment, the handle the status and revoke
    /// routes take. Not secret: the admin listing exposes the same value.
    pub enrollment_id: String,
    pub mode: RequestedOnboardingMode,
    pub expires_at: u64,
    /// `aruna://enroll` deep link a device app opens to claim this secret;
    /// `None` for infrastructure modes, which have no device app.
    pub enroll_url: Option<String>,
}

impl fmt::Debug for OnboardingSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnboardingSecret")
            .field("seed_url", &self.seed_url)
            .field("enrollment_id", &self.enrollment_id)
            .field("mode", &self.mode)
            .field("realm_id", &self.realm_id)
            .field("purpose", &self.purpose)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for BootstrapOnboardingRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BootstrapOnboardingRequest")
            .field("node_id", &self.node_id)
            .field("node_proof", &self.node_proof)
            .field("transport_public_key", &self.transport_public_key)
            .field("issuer_public_key", &self.issuer_public_key)
            .field("issuer_proof", &self.issuer_proof)
            .field("node_location", &self.node_location)
            .field("node_weight", &self.node_weight)
            .field("node_labels", &self.node_labels)
            .finish_non_exhaustive()
    }
}

/// Leaves out the wrapped realm key, its nonce and the sync ticket.
impl fmt::Debug for BootstrapOnboardingResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BootstrapOnboardingResponse")
            .field("realm_id", &self.realm_id)
            .field("mode", &self.mode)
            .field(
                "temporary_bootstrap_endpoint",
                &self.temporary_bootstrap_endpoint,
            )
            .field("wrapping_public_key", &self.wrapping_public_key)
            .field("delegation_signature", &self.delegation_signature)
            .field("realm_endpoints", &self.realm_endpoints)
            .finish_non_exhaustive()
    }
}

/// Leaves out the secret and the deep link that carries it.
impl fmt::Debug for CreateSecretResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateSecretResponse")
            .field("enrollment_id", &self.enrollment_id)
            .field("mode", &self.mode)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OnboardingSecretError {
    #[error("invalid onboarding secret")]
    InvalidSecret,
    #[error("invalid onboarding signature")]
    InvalidSignature,
    #[error(transparent)]
    Base64(#[from] base64::DecodeError),
    #[error(transparent)]
    Postcard(#[from] postcard::Error),
}

impl OnboardingSecret {
    pub fn secret_hash(&self) -> String {
        credential_hash(self.secret)
    }

    pub fn encode(&self) -> Result<String, OnboardingSecretError> {
        let bytes = postcard::to_allocvec(self)?;
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn decode(value: &str) -> Result<Self, OnboardingSecretError> {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(value)?;
        Ok(postcard::from_bytes(&bytes)?)
    }
}

impl BootstrapOnboardingResponse {
    pub fn realm_id(&self) -> Result<RealmId, OnboardingSecretError> {
        RealmId::from_base64(&self.realm_id).map_err(|_| OnboardingSecretError::InvalidSecret)
    }
}

impl OnboardingTicket {
    pub fn issue(
        signing_key: &SigningKey,
        realm_id: &RealmId,
        node_id: NodeId,
        expires_at: u64,
        documents: Vec<DocumentTarget>,
    ) -> Result<Self, OnboardingSecretError> {
        Self::issue_with_deletions(
            signing_key,
            realm_id,
            node_id,
            expires_at,
            documents,
            Vec::new(),
        )
    }

    pub fn issue_with_deletions(
        signing_key: &SigningKey,
        realm_id: &RealmId,
        node_id: NodeId,
        expires_at: u64,
        documents: Vec<DocumentTarget>,
        group_deletions: Vec<crate::structs::identity::group_delete::GroupDeleteRecord>,
    ) -> Result<Self, OnboardingSecretError> {
        let payload = OnboardingTicketPayload {
            realm_id: realm_id.to_string(),
            node_id: node_id.to_string(),
            expires_at,
            documents,
            group_deletions,
        };
        let payload_bytes = postcard::to_allocvec(&payload)?;
        let signature = signing_key.sign(&payload_bytes).to_string();
        Ok(Self { payload, signature })
    }

    pub fn encode(&self) -> Result<String, OnboardingSecretError> {
        let bytes = postcard::to_allocvec(self)?;
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn decode(value: &str) -> Result<Self, OnboardingSecretError> {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(value)?;
        Ok(postcard::from_bytes(&bytes)?)
    }

    pub fn verify(
        &self,
        expected_node_id: NodeId,
        expected_document: &DocumentTarget,
        now: u64,
    ) -> Result<(), OnboardingSecretError> {
        if self.payload.node_id != expected_node_id.to_string() {
            return Err(OnboardingSecretError::InvalidSecret);
        }
        if self.payload.expires_at < now {
            return Err(OnboardingSecretError::InvalidSecret);
        }
        if !self
            .payload
            .documents
            .iter()
            .any(|document| document == expected_document)
        {
            return Err(OnboardingSecretError::InvalidSecret);
        }

        let realm_id = RealmId::from_base64(&self.payload.realm_id)
            .map_err(|_| OnboardingSecretError::InvalidSecret)?;
        let verifying_key = VerifyingKey::from_bytes(realm_id.as_bytes())
            .map_err(|_| OnboardingSecretError::InvalidSignature)?;
        let signature = Signature::from_str(&self.signature)
            .map_err(|_| OnboardingSecretError::InvalidSignature)?;
        let payload_bytes = postcard::to_allocvec(&self.payload)?;
        verifying_key
            .verify(&payload_bytes, &signature)
            .map_err(|_| OnboardingSecretError::InvalidSignature)
    }
}

pub fn node_proof_message(
    onboarding_secret: &str,
    node_id: &str,
    transport_public_key: Option<&str>,
) -> Vec<u8> {
    format!(
        "aruna-bootstrap-node:{onboarding_secret}:{node_id}:{}",
        transport_public_key.unwrap_or_default()
    )
    .into_bytes()
}

pub fn issuer_proof_message(
    onboarding_secret: &str,
    node_id: &str,
    issuer_public_key: &str,
) -> Vec<u8> {
    format!("aruna-bootstrap-issuer:{onboarding_secret}:{node_id}:{issuer_public_key}").into_bytes()
}

#[cfg(test)]
mod tests {
    use super::{
        BootstrapOnboardingRequest, BootstrapOnboardingResponse, CreateSecretResponse,
        OnboardingMode, OnboardingPurpose, OnboardingSecret, OnboardingTicket,
        RequestedOnboardingMode, credential_hash,
    };
    use crate::document::DocumentTarget;
    use crate::structs::identity::realm::RealmId;
    use ed25519_dalek::SigningKey;
    use ulid::Ulid;

    #[test]
    fn secret_debug_hidden() {
        let secret = OnboardingSecret {
            seed_url: "http://127.0.0.1:3000".to_string(),
            enrollment_id: Ulid::from_bytes([1; 16]),
            secret: [201; 32],
            mode: OnboardingMode::Server,
            realm_id: RealmId::from_bytes([5; 32]),
            purpose: OnboardingPurpose::NodeEnrollment,
        };
        assert!(!format!("{secret:?}").contains("201, 201"));
    }

    #[test]
    fn request_debug_hidden() {
        let request = BootstrapOnboardingRequest {
            onboarding_secret: "hidden-value".to_string(),
            node_id: "node".to_string(),
            node_proof: "proof".to_string(),
            transport_public_key: None,
            issuer_public_key: None,
            issuer_proof: None,
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        };
        assert!(!format!("{request:?}").contains("hidden-value"));
    }

    #[test]
    fn response_debug_hidden() {
        let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let response = BootstrapOnboardingResponse {
            realm_id: "realm".to_string(),
            mode: OnboardingMode::Management,
            temporary_bootstrap_endpoint: iroh::EndpointAddr::new(node),
            wrapped_realm_key: Some("hidden-key".to_string()),
            wrapped_key_nonce: Some("hidden-nonce".to_string()),
            wrapping_public_key: None,
            delegation_signature: None,
            onboarding_sync_ticket: "hidden-ticket".to_string(),
            realm_endpoints: Vec::new(),
        };
        let shown = format!("{response:?}");
        for hidden in ["hidden-key", "hidden-nonce", "hidden-ticket"] {
            assert!(!shown.contains(hidden));
        }
    }

    #[test]
    fn created_debug_hidden() {
        let response = CreateSecretResponse {
            onboarding_secret: "hidden-value".to_string(),
            enrollment_id: "enrollment".to_string(),
            mode: RequestedOnboardingMode::User,
            expires_at: 1,
            enroll_url: Some("aruna://enroll?secret=hidden-link".to_string()),
        };
        let shown = format!("{response:?}");
        assert!(!shown.contains("hidden-value"));
        assert!(!shown.contains("hidden-link"));
    }

    #[test]
    fn onboarding_secret_roundtrip() {
        let secret = OnboardingSecret {
            seed_url: "http://127.0.0.1:3000".to_string(),
            enrollment_id: Ulid::generate(),
            secret: [7u8; 32],
            mode: OnboardingMode::Server,
            realm_id: RealmId::from_bytes([5u8; 32]),
            purpose: OnboardingPurpose::NodeEnrollment,
        };

        let encoded = secret.encode().unwrap();
        let decoded = OnboardingSecret::decode(&encoded).unwrap();
        assert_eq!(decoded, secret);
    }

    #[test]
    fn onboarding_secret_hex() {
        let secret = OnboardingSecret {
            seed_url: "http://127.0.0.1:3000".to_string(),
            enrollment_id: Ulid::generate(),
            secret: [7u8; 32],
            mode: OnboardingMode::Server,
            realm_id: RealmId::from_bytes([5u8; 32]),
            purpose: OnboardingPurpose::NodeEnrollment,
        };

        assert_eq!(secret.secret_hash(), credential_hash(secret.secret));
    }

    #[test]
    fn onboarding_sync_verify() {
        let realm_signing_key = SigningKey::from_bytes(&[3u8; 32]);
        let node_signing_key = SigningKey::from_bytes(&[4u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&node_signing_key.to_bytes()).public();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let document = DocumentTarget::RealmAuthorization { realm_id };

        let ticket = OnboardingTicket::issue(
            &realm_signing_key,
            &realm_id,
            node_id,
            u64::MAX,
            vec![document.clone()],
        )
        .unwrap();

        let encoded = ticket.encode().unwrap();
        let decoded = OnboardingTicket::decode(&encoded).unwrap();
        decoded.verify(node_id, &document, 0).unwrap();
    }
}
