use crate::NodeId;
use crate::auth::credential_hash;
use crate::document::DocumentSyncTarget;
use crate::structs::RealmId;
use base64::Engine;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnboardingMode {
    Management,
    Server,
    Local,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingSecret {
    pub seed_url: String,
    pub enrollment_id: Ulid,
    pub secret: [u8; 32],
    pub mode: OnboardingMode,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingSecretRecord {
    pub enrollment_id: Ulid,
    pub secret_hash: String,
    pub mode: OnboardingMode,
    pub expires_at: u64,
    pub claimed_node_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingSecretStateRecord {
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapOnboardingResponse {
    pub realm_id: String,
    pub mode: OnboardingMode,
    pub temporary_bootstrap_endpoint: EndpointAddr,
    pub wrapped_realm_private_key: Option<String>,
    pub wrapped_realm_private_key_nonce: Option<String>,
    pub wrapping_public_key: Option<String>,
    pub delegation_signature: Option<String>,
    pub onboarding_sync_ticket: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnboardingPhase {
    Bootstrapped,
    CoreDocumentsFetched,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingSyncTicketPayload {
    pub realm_id: String,
    pub node_id: String,
    pub expires_at: u64,
    pub documents: Vec<DocumentSyncTarget>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingSyncTicket {
    pub payload: OnboardingSyncTicketPayload,
    pub signature: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateOnboardingSecretRequest {
    pub seed_url: String,
    pub mode: OnboardingMode,
    pub expires_in_seconds: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateOnboardingSecretResponse {
    pub onboarding_secret: String,
    pub mode: OnboardingMode,
    pub expires_at: u64,
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

impl OnboardingSyncTicket {
    pub fn issue(
        signing_key: &SigningKey,
        realm_id: &RealmId,
        node_id: NodeId,
        expires_at: u64,
        documents: Vec<DocumentSyncTarget>,
    ) -> Result<Self, OnboardingSecretError> {
        let payload = OnboardingSyncTicketPayload {
            realm_id: realm_id.to_string(),
            node_id: node_id.to_string(),
            expires_at,
            documents,
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
        expected_document: &DocumentSyncTarget,
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

pub fn bootstrap_node_proof_message(
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

pub fn bootstrap_issuer_proof_message(
    onboarding_secret: &str,
    node_id: &str,
    issuer_public_key: &str,
) -> Vec<u8> {
    format!("aruna-bootstrap-issuer:{onboarding_secret}:{node_id}:{issuer_public_key}").into_bytes()
}

#[cfg(test)]
mod tests {
    use super::{OnboardingMode, OnboardingSecret, OnboardingSyncTicket, credential_hash};
    use crate::document::DocumentSyncTarget;
    use crate::structs::RealmId;
    use ed25519_dalek::SigningKey;
    use ulid::Ulid;

    #[test]
    fn onboarding_secret_roundtrip() {
        let secret = OnboardingSecret {
            seed_url: "http://127.0.0.1:3000".to_string(),
            enrollment_id: Ulid::new(),
            secret: [7u8; 32],
            mode: OnboardingMode::Server,
        };

        let encoded = secret.encode().unwrap();
        let decoded = OnboardingSecret::decode(&encoded).unwrap();
        assert_eq!(decoded, secret);
    }

    #[test]
    fn onboarding_secret_hash_matches_existing_blake3_hex() {
        let secret = OnboardingSecret {
            seed_url: "http://127.0.0.1:3000".to_string(),
            enrollment_id: Ulid::new(),
            secret: [7u8; 32],
            mode: OnboardingMode::Server,
        };

        assert_eq!(secret.secret_hash(), credential_hash(secret.secret));
    }

    #[test]
    fn onboarding_sync_ticket_roundtrip_and_verify() {
        let realm_signing_key = SigningKey::from_bytes(&[3u8; 32]);
        let node_signing_key = SigningKey::from_bytes(&[4u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&node_signing_key.to_bytes()).public();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let document = DocumentSyncTarget::RealmAuthorization { realm_id };

        let ticket = OnboardingSyncTicket::issue(
            &realm_signing_key,
            &realm_id,
            node_id,
            u64::MAX,
            vec![document.clone()],
        )
        .unwrap();

        let encoded = ticket.encode().unwrap();
        let decoded = OnboardingSyncTicket::decode(&encoded).unwrap();
        decoded.verify(node_id, &document, 0).unwrap();
    }
}
