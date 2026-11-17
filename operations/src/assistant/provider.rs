//! Stores assistant provider records with encrypted secrets and a per-owner index.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{ASSISTANT_PROVIDER_KEYSPACE, PROVIDER_OWNER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    AssistantHeaders, AssistantProvider, AssistantProviderSecret, AssistantSecretError,
};
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use std::collections::BTreeSet;
use thiserror::Error;
use ulid::Ulid;

fn owner_key(user_id: UserId) -> Key {
    ByteView::from(user_id.to_storage_key())
}

fn owner_row(user_id: UserId) -> (String, Key) {
    (PROVIDER_OWNER_KEYSPACE.to_string(), owner_key(user_id))
}

fn provider_row(provider_id: &str) -> (String, Key) {
    (
        ASSISTANT_PROVIDER_KEYSPACE.to_string(),
        provider_id.as_bytes().into(),
    )
}

fn decode_index(value: Option<&ByteView>) -> Result<BTreeSet<String>, ConversionError> {
    let Some(value) = value else {
        return Ok(BTreeSet::new());
    };
    let index: BTreeSet<String> = postcard::from_bytes(value.as_ref())?;
    for provider_id in &index {
        Ulid::from_string(provider_id)?;
    }
    Ok(index)
}

fn encode_index(index: &BTreeSet<String>) -> Result<Value, ConversionError> {
    Ok(ByteView::from(postcard::to_allocvec(index)?))
}

#[derive(Debug, Error, PartialEq)]
pub enum ProviderStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    /// The secret or header encryption failure, as its text.
    #[error("{0}")]
    Secret(String),
    #[error("provider not found")]
    NotFound,
    #[error("provider id collision")]
    IdCollision,
    #[error("provider owner index is inconsistent")]
    IndexInconsistent,
    #[error("provider changed concurrently")]
    Stale,
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for ProviderStoreError {
    fn from(error: IoError) -> Self {
        error.into_storage().map_or_else(Self::Io, Self::Storage)
    }
}

impl From<AssistantSecretError> for ProviderStoreError {
    fn from(error: AssistantSecretError) -> Self {
        Self::Secret(error.to_string())
    }
}

/// What a provider create waits for.
#[derive(Debug, PartialEq)]
enum CreateStage {
    Index,
    Provider(BTreeSet<String>),
    Written,
}

#[derive(Debug, PartialEq)]
pub struct CreateProviderOperation {
    provider: AssistantProvider,
    pending_secret: AssistantProviderSecret,
    pending_headers: AssistantHeaders,
    encryption_key: CredentialEncryptionKey,
    stage: CreateStage,
}

impl CreateProviderOperation {
    pub fn new(
        provider: AssistantProvider,
        secret: AssistantProviderSecret,
        headers: AssistantHeaders,
        encryption_key: CredentialEncryptionKey,
    ) -> Self {
        Self {
            provider,
            pending_secret: secret,
            pending_headers: headers,
            encryption_key,
            stage: CreateStage::Index,
        }
    }

    fn write(&mut self, index: &BTreeSet<String>) -> Result<IoCall, ProviderStoreError> {
        self.provider
            .encrypt_secret(&self.encryption_key, &self.pending_secret)?;
        self.provider
            .encrypt_headers(&self.encryption_key, &self.pending_headers)?;
        let (provider_space, provider_key) = provider_row(&self.provider.provider_id);
        let (owner_space, owner_key) = owner_row(self.provider.user_id);
        Ok(IoCall::Write(vec![
            (provider_space, provider_key, self.provider.to_bytes()?.into()),
            (owner_space, owner_key, encode_index(index)?),
        ]))
    }
}

impl Operation for CreateProviderOperation {
    type Output = AssistantProvider;
    type Error = ProviderStoreError;

    fn first(&mut self) -> Result<Next<AssistantProvider>, ProviderStoreError> {
        Ulid::from_string(&self.provider.provider_id).map_err(ConversionError::from)?;
        Ok(Next::Call(IoCall::Read(vec![owner_row(self.provider.user_id)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AssistantProvider>, ProviderStoreError> {
        match std::mem::replace(&mut self.stage, CreateStage::Written) {
            CreateStage::Index => {
                let mut index = decode_index(result.value()?.as_ref())?;
                if !index.insert(self.provider.provider_id.clone()) {
                    return Err(ProviderStoreError::IdCollision);
                }
                self.stage = CreateStage::Provider(index);
                let read = provider_row(&self.provider.provider_id);
                Ok(Next::Call(IoCall::Read(vec![read])))
            }
            CreateStage::Provider(index) => {
                if result.value()?.is_some() {
                    return Err(ProviderStoreError::IdCollision);
                }
                Ok(Next::Call(self.write(&index)?))
            }
            CreateStage::Written => {
                result.done()?;
                Ok(Next::Done(self.provider.clone()))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

/// Reads one provider of its owner; another owner's provider reads as absent.
#[derive(Debug, PartialEq)]
pub struct GetProviderOperation {
    provider_id: String,
    user_id: UserId,
}

impl GetProviderOperation {
    pub fn new(provider_id: String, user_id: UserId) -> Self {
        Self {
            provider_id,
            user_id,
        }
    }
}

impl Operation for GetProviderOperation {
    type Output = Option<AssistantProvider>;
    type Error = ProviderStoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, ProviderStoreError> {
        Ulid::from_string(&self.provider_id).map_err(ConversionError::from)?;
        Ok(Next::Call(IoCall::Read(vec![provider_row(&self.provider_id)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ProviderStoreError> {
        let provider = (result.value()?)
            .map(|value| AssistantProvider::from_bytes(value.as_ref()))
            .transpose()?;
        let owned = provider.filter(|provider| provider.user_id == self.user_id);
        Ok(Next::Done(owned))
    }
}

/// Lists the providers of one owner through its index, oldest first.
#[derive(Debug, PartialEq)]
pub struct ListProviderOperation {
    user_id: UserId,
    /// The indexed provider ids once the index was read.
    provider_ids: Option<Vec<String>>,
}

impl ListProviderOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            provider_ids: None,
        }
    }

    fn collect(
        &self,
        provider_ids: &[String],
        values: Vec<Option<Value>>,
    ) -> Result<Vec<AssistantProvider>, ProviderStoreError> {
        if values.len() != provider_ids.len() {
            return Err(ProviderStoreError::IndexInconsistent);
        }
        let mut providers = Vec::with_capacity(values.len());
        for (provider_id, value) in provider_ids.iter().zip(values) {
            let value = value.ok_or(ProviderStoreError::IndexInconsistent)?;
            let provider = AssistantProvider::from_bytes(value.as_ref())?;
            if provider.user_id != self.user_id || &provider.provider_id != provider_id {
                return Err(ProviderStoreError::IndexInconsistent);
            }
            providers.push(provider);
        }
        providers.sort_by_key(|provider| provider.created_at);
        Ok(providers)
    }
}

impl Operation for ListProviderOperation {
    type Output = Vec<AssistantProvider>;
    type Error = ProviderStoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, ProviderStoreError> {
        Ok(Next::Call(IoCall::Read(vec![owner_row(self.user_id)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ProviderStoreError> {
        if let Some(provider_ids) = self.provider_ids.take() {
            return Ok(Next::Done(self.collect(&provider_ids, result.values()?)?));
        }
        let index = decode_index(result.value()?.as_ref())?;
        if index.is_empty() {
            return Ok(Next::Done(Vec::new()));
        }
        let provider_ids: Vec<String> = index.into_iter().collect();
        let reads = provider_ids.iter().map(|id| provider_row(id)).collect();
        self.provider_ids = Some(provider_ids);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn transaction(&self) -> bool {
        true
    }
}

/// Replaces one provider record when it still equals `expected`.
#[derive(Debug, PartialEq)]
pub struct UpdateProviderOperation {
    provider: AssistantProvider,
    expected: AssistantProvider,
    user_id: UserId,
    /// Whether the write was sent and its answer ends the operation.
    written: bool,
}

impl UpdateProviderOperation {
    pub fn new(provider: AssistantProvider, expected: AssistantProvider, user_id: UserId) -> Self {
        Self {
            provider,
            expected,
            user_id,
            written: false,
        }
    }

    fn check(&self, value: Option<Value>) -> Result<(), ProviderStoreError> {
        let value = value.ok_or(ProviderStoreError::NotFound)?;
        let current = AssistantProvider::from_bytes(value.as_ref())?;
        if current.user_id != self.user_id
            || self.provider.user_id != self.user_id
            || current.provider_id != self.provider.provider_id
            || current.kind != self.provider.kind
            || current.created_at != self.provider.created_at
        {
            return Err(ProviderStoreError::NotFound);
        }
        if current != self.expected {
            return Err(ProviderStoreError::Stale);
        }
        Ok(())
    }
}

impl Operation for UpdateProviderOperation {
    type Output = AssistantProvider;
    type Error = ProviderStoreError;

    fn first(&mut self) -> Result<Next<AssistantProvider>, ProviderStoreError> {
        let read = provider_row(&self.provider.provider_id);
        Ok(Next::Call(IoCall::Read(vec![read])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AssistantProvider>, ProviderStoreError> {
        if self.written {
            result.done()?;
            return Ok(Next::Done(self.provider.clone()));
        }
        self.check(result.value()?)?;
        let (space, key) = provider_row(&self.provider.provider_id);
        self.written = true;
        Ok(Next::Call(IoCall::Write(vec![(
            space,
            key,
            self.provider.to_bytes()?.into(),
        )])))
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::credential_encryption::EncryptedS3Secret;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::{AssistantProviderKind, AssistantProviderStatus};

    #[test]
    fn rejects_stale_update() {
        let realm_id = RealmId::from_bytes([3; 32]);
        let user_id = UserId::local(Ulid::from_bytes([4; 16]), realm_id);
        let provider_id = Ulid::from_bytes([5; 16]).to_string();
        let expected = AssistantProvider {
            provider_id: provider_id.clone(),
            user_id,
            kind: AssistantProviderKind::Openai,
            label: "Original".to_string(),
            base_url: "https://api.openai.com".to_string(),
            headers: EncryptedS3Secret::empty(),
            secret: EncryptedS3Secret::empty(),
            models: Vec::new(),
            default_model: None,
            created_at: 1,
            status: AssistantProviderStatus::Ready,
            token_obtained_at: None,
            login_expires_at: None,
            login_interval_seconds: None,
        };
        let mut replacement = expected.clone();
        replacement.label = "Replacement".to_string();
        let mut current = expected.clone();
        current.models.push("concurrent-model".to_string());
        let mut operation = UpdateProviderOperation::new(replacement, expected, user_id);
        operation.first().unwrap();
        let read = IoResult::Values(vec![Some(current.to_bytes().unwrap().into())]);
        assert_eq!(operation.next(read), Err(ProviderStoreError::Stale));
    }
}

/// What a provider delete waits for.
#[derive(Debug, PartialEq)]
enum DeleteStage {
    Provider,
    Index,
    Deleted(Value),
    Written,
}

/// Removes one provider of its owner and drops it from the owner index.
#[derive(Debug, PartialEq)]
pub struct DeleteProviderOperation {
    provider_id: String,
    user_id: UserId,
    stage: DeleteStage,
}

impl DeleteProviderOperation {
    pub fn new(provider_id: String, user_id: UserId) -> Self {
        Self {
            provider_id,
            user_id,
            stage: DeleteStage::Provider,
        }
    }
}

impl Operation for DeleteProviderOperation {
    type Output = ();
    type Error = ProviderStoreError;

    fn first(&mut self) -> Result<Next<()>, ProviderStoreError> {
        if Ulid::from_string(&self.provider_id).is_err() {
            return Err(ProviderStoreError::NotFound);
        }
        Ok(Next::Call(IoCall::Read(vec![provider_row(&self.provider_id)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ProviderStoreError> {
        match std::mem::replace(&mut self.stage, DeleteStage::Written) {
            DeleteStage::Provider => {
                let value = result.value()?.ok_or(ProviderStoreError::NotFound)?;
                if AssistantProvider::from_bytes(value.as_ref())?.user_id != self.user_id {
                    return Err(ProviderStoreError::NotFound);
                }
                self.stage = DeleteStage::Index;
                Ok(Next::Call(IoCall::Read(vec![owner_row(self.user_id)])))
            }
            DeleteStage::Index => {
                let mut index = decode_index(result.value()?.as_ref())?;
                if !index.remove(&self.provider_id) {
                    return Err(ProviderStoreError::IndexInconsistent);
                }
                self.stage = DeleteStage::Deleted(encode_index(&index)?);
                Ok(Next::Call(IoCall::Delete(vec![provider_row(&self.provider_id)])))
            }
            DeleteStage::Deleted(index) => {
                result.done()?;
                let (space, key) = owner_row(self.user_id);
                Ok(Next::Call(IoCall::Write(vec![(space, key, index)])))
            }
            DeleteStage::Written => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}
