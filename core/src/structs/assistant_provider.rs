use crate::compute::Secret;
use crate::credential_encryption::{CredentialEncryptionKey, EncryptedS3Secret, EncryptionError};
use crate::errors::ConversionError;
use crate::types::UserId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssistantProviderKind {
    Anthropic,
    Openai,
    Openrouter,
    OpenaiCompatible,
    Chatgpt,
}

impl fmt::Display for AssistantProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Anthropic => "anthropic",
            Self::Openai => "openai",
            Self::Openrouter => "openrouter",
            Self::OpenaiCompatible => "openai_compatible",
            Self::Chatgpt => "chatgpt",
        })
    }
}

impl AssistantProviderKind {
    pub fn default_base_url(self) -> Option<&'static str> {
        match self {
            Self::Anthropic => Some("https://api.anthropic.com"),
            Self::Openai => Some("https://api.openai.com"),
            Self::Openrouter => Some("https://openrouter.ai/api"),
            Self::OpenaiCompatible => None,
            Self::Chatgpt => Some("https://chatgpt.com/backend-api/codex"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssistantProviderStatus {
    #[serde(rename = "pending")]
    PendingLogin,
    Ready,
}

impl fmt::Display for AssistantProviderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PendingLogin => "pending",
            Self::Ready => "ready",
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantProviderSecret {
    pub api_key: Option<Secret>,
    pub access_token: Option<Secret>,
    pub refresh_token: Option<Secret>,
    pub account_id: Option<Secret>,
    pub device_auth_id: Option<Secret>,
    pub user_code: Option<Secret>,
}

impl fmt::Debug for AssistantProviderSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AssistantProviderSecret(***)")
    }
}

impl AssistantProviderSecret {
    pub fn empty() -> Self {
        Self {
            api_key: None,
            access_token: None,
            refresh_token: None,
            account_id: None,
            device_auth_id: None,
            user_code: None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantHeaders(pub BTreeMap<String, Secret>);

impl fmt::Debug for AssistantHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AssistantHeaders(***)")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantProvider {
    pub provider_id: String,
    pub user_id: UserId,
    pub kind: AssistantProviderKind,
    pub label: String,
    pub base_url: String,
    pub headers: EncryptedS3Secret,
    pub secret: EncryptedS3Secret,
    pub models: Vec<String>,
    pub default_model: Option<String>,
    pub created_at: u64,
    pub status: AssistantProviderStatus,
    pub token_obtained_at: Option<u64>,
    pub login_expires_at: Option<u64>,
    pub login_interval_seconds: Option<u64>,
}

#[derive(Debug, Error)]
pub enum AssistantSecretError {
    #[error(transparent)]
    Encryption(#[from] EncryptionError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl AssistantProvider {
    fn field_aad(&self, field: u8) -> Vec<u8> {
        let mut aad = Vec::new();
        aad.extend_from_slice(self.provider_id.as_bytes());
        aad.push(0);
        aad.extend_from_slice(&self.user_id.to_bytes());
        aad.extend_from_slice(self.kind.to_string().as_bytes());
        aad.push(field);
        aad
    }

    pub fn encrypt_secret(
        &mut self,
        key: &CredentialEncryptionKey,
        secret: &AssistantProviderSecret,
    ) -> Result<(), AssistantSecretError> {
        let plaintext = serde_json::to_string(secret)?;
        self.secret = EncryptedS3Secret::encrypt(key, &plaintext, &self.field_aad(1))?;
        Ok(())
    }

    pub fn open_secret(
        &self,
        key: &CredentialEncryptionKey,
    ) -> Result<AssistantProviderSecret, AssistantSecretError> {
        let plaintext = self.secret.open(key, &self.field_aad(1))?;
        Ok(serde_json::from_str(&plaintext)?)
    }

    pub fn encrypt_headers(
        &mut self,
        key: &CredentialEncryptionKey,
        headers: &AssistantHeaders,
    ) -> Result<(), AssistantSecretError> {
        let plaintext = serde_json::to_string(headers)?;
        self.headers = EncryptedS3Secret::encrypt(key, &plaintext, &self.field_aad(2))?;
        Ok(())
    }

    pub fn open_headers(
        &self,
        key: &CredentialEncryptionKey,
    ) -> Result<AssistantHeaders, AssistantSecretError> {
        let plaintext = self.headers.open(key, &self.field_aad(2))?;
        Ok(serde_json::from_str(&plaintext)?)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}
