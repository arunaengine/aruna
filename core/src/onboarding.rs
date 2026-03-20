use crate::structs::RealmId;
use base64::Engine;
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};
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
    pub consumed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapOnboardingRequest {
    pub onboarding_secret: String,
    pub node_id: String,
    pub issuer_public_key: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapOnboardingResponse {
    pub realm_id: String,
    pub mode: OnboardingMode,
    pub bootstrap_endpoints: Vec<EndpointAddr>,
    pub realm_private_key_pem: Option<String>,
    pub delegation_signature: Option<String>,
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
    #[error(transparent)]
    Base64(#[from] base64::DecodeError),
    #[error(transparent)]
    Postcard(#[from] postcard::Error),
}

impl OnboardingSecret {
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

#[cfg(test)]
mod tests {
    use super::{OnboardingMode, OnboardingSecret};
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
}
