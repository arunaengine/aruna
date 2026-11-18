//! Destination side decisions of an export from another realm: signing an import intent and
//! admitting an intent with its grant against the realm's current settings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::StorageError;
use aruna_core::federation::{FederationError, Signed};
use aruna_core::handoff::descriptor_digest;
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, values};
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::transfer::{
    ExportGrant, ImportDestination, ImportIntent, MAX_TRANSFER_SECS, TransferError, check_grant,
    check_intent, import_key,
};
use thiserror::Error;
use ulid::Ulid;

use crate::document_repository::GetConfigError;

#[derive(Debug, Error, PartialEq)]
pub enum TransferAdmitError {
    #[error(transparent)]
    Config(#[from] GetConfigError),
    #[error("this realm has no federation settings")]
    Disabled,
    #[error(transparent)]
    Rejected(#[from] TransferError),
    #[error("the intent was issued before a credential cutoff of its principal")]
    CutOff,
    #[error(transparent)]
    Sign(#[from] FederationError),
    #[error("transfer operation did not finish")]
    NotFinished,
    #[error("unexpected event after the transfer decision")]
    UnexpectedEvent,
}

impl From<IoError> for TransferAdmitError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => StorageError::TransactionConflict.into(),
            IoError::Storage(error) => error.into(),
            IoError::Unexpected { .. } | IoError::Codec(_) => Self::UnexpectedEvent,
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished => Self::NotFinished,
        }
    }
}

impl From<StorageError> for TransferAdmitError {
    fn from(error: StorageError) -> Self {
        Self::Config(GetConfigError::StorageError(error))
    }
}

/// The read of the realm config that both decisions start from.
fn read_config(realm_id: RealmId) -> IoCall {
    let target = DocumentTarget::RealmConfig { realm_id };
    read_row(target.storage_keyspace(), target.storage_key())
}

/// The realm config of [`read_config`]; a missing one is not found.
fn realm_config(result: IoResult) -> Result<RealmConfigDocument, TransferAdmitError> {
    let [value] = values::<1>(result)?;
    let value = value.ok_or(GetConfigError::DocumentNotFound)?;
    Ok(RealmConfigDocument::from_bytes(&value).map_err(GetConfigError::from)?)
}

#[derive(Clone, Debug, PartialEq)]
pub struct IssueIntentConfig {
    pub realm_id: RealmId,
    pub principal: UserId,
    pub destination: ImportDestination,
    pub max_bytes: u64,
    /// Lowercase hex SHA-256 of the portal's browser secret.
    pub nonce: String,
    pub node_capabilities: NodeCapabilities,
    pub now: u64,
    /// The id of the intent, minted by the caller.
    pub intent_id: Ulid,
}

/// Signs an import intent for this realm's current descriptor. Authorization comes first.
#[derive(Debug, PartialEq)]
pub struct IssueIntentOperation {
    config: IssueIntentConfig,
}

impl IssueIntentOperation {
    pub fn new(config: IssueIntentConfig) -> Self {
        Self { config }
    }
}

impl Operation for IssueIntentOperation {
    type Output = Signed<ImportIntent>;
    type Error = TransferAdmitError;

    fn first(&mut self) -> Result<Next<Self::Output>, TransferAdmitError> {
        Ok(Next::Call(read_config(self.config.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, TransferAdmitError> {
        let config = &self.config;
        let realm = realm_config(result)?;
        let settings = realm.federation.ok_or(TransferAdmitError::Disabled)?;
        let intent = ImportIntent {
            realm_id: config.realm_id,
            descriptor_digest: descriptor_digest(&settings.descriptor)?,
            principal: config.principal,
            destination: config.destination.clone(),
            max_bytes: config.max_bytes,
            nonce: config.nonce.clone(),
            issued_at: config.now,
            expires_at: config.now.saturating_add(MAX_TRANSFER_SECS),
            intent_id: config.intent_id,
        };
        Ok(Next::Done(Signed::sign(intent, &config.node_capabilities)?))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdmitTransferConfig {
    pub realm_id: RealmId,
    pub intent: Signed<ImportIntent>,
    pub grant: Signed<ExportGrant>,
    /// The browser secret, checked when the portal presents it.
    pub secret: Option<Vec<u8>>,
    pub now: u64,
}

/// Admits an intent with its grant against the current descriptor and the principal's
/// credential cutoff, and returns the import key. Authorization follows.
#[derive(Debug, PartialEq)]
pub struct AdmitTransferOperation {
    config: AdmitTransferConfig,
}

impl AdmitTransferOperation {
    pub fn new(config: AdmitTransferConfig) -> Self {
        Self { config }
    }
}

impl Operation for AdmitTransferOperation {
    type Output = String;
    type Error = TransferAdmitError;

    fn first(&mut self) -> Result<Next<String>, TransferAdmitError> {
        Ok(Next::Call(read_config(self.config.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, TransferAdmitError> {
        let config = &self.config;
        let realm = realm_config(result)?;
        let (intent, grant, now) = (&config.intent, &config.grant, config.now);
        let settings = realm
            .federation
            .as_ref()
            .ok_or(TransferAdmitError::Disabled)?;
        let secret = config.secret.as_deref();
        check_intent(intent, &config.realm_id, &settings.descriptor, secret, now)?;
        check_grant(grant, intent, now)?;
        let payload = &intent.payload;
        if realm
            .user_cutoff(&payload.principal, now)
            .is_some_and(|cutoff| payload.issued_at < cutoff)
        {
            return Err(TransferAdmitError::CutOff);
        }
        Ok(Next::Done(import_key(
            &grant.payload,
            &payload.destination,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::federation::import::tests::{
        context, descriptor, principal, realm, record, seed_cutoff, signer,
    };

    const NOW: u64 = 100;

    #[tokio::test]
    async fn intent_names_descriptor() {
        // An intent is signed only with federation settings, for the current descriptor.
        let (_dir, context) = context();
        let config = IssueIntentConfig {
            realm_id: realm(2),
            principal: principal(),
            destination: record().intent.payload.destination,
            max_bytes: 10,
            nonce: "aa".repeat(32),
            node_capabilities: signer(2),
            now: NOW,
            intent_id: Ulid::from_parts(NOW, 1),
        };
        let issued = drive(IssueIntentOperation::new(config.clone()), &context).await;
        assert!(issued.is_err());
        seed_cutoff(&context, descriptor(), None).await;
        let issued = drive(IssueIntentOperation::new(config), &context)
            .await
            .unwrap();
        assert!(issued.verify(&realm(2)).is_ok());
        let digest = descriptor_digest(&descriptor()).unwrap();
        assert_eq!(issued.payload.descriptor_digest, digest);
        assert_eq!(issued.payload.expires_at, NOW + MAX_TRANSFER_SECS);
    }

    #[tokio::test]
    async fn admission_checks_cutoff() {
        // A valid intent and grant give the import key until the principal is cut off.
        let (_dir, context) = context();
        let record = record();
        let admit = |secret: Option<Vec<u8>>| {
            AdmitTransferOperation::new(AdmitTransferConfig {
                realm_id: realm(2),
                intent: record.intent.clone(),
                grant: record.grant.clone(),
                secret,
                now: NOW,
            })
        };
        seed_cutoff(&context, descriptor(), None).await;
        let key = import_key(&record.grant.payload, &record.intent.payload.destination);
        assert_eq!(drive(admit(None), &context).await, Ok(key.unwrap()));
        let wrong = drive(admit(Some(vec![1; 32])), &context).await;
        assert_eq!(wrong, Err(TransferError::WrongSecret.into()));
        let cutoff = aruna_core::structs::identity::realm::TokenRevocation {
            token_hash: aruna_core::auth::user_cutoff_hash(&principal()),
            expires_at: aruna_core::auth::user_cutoff_expiry(NOW + 1),
        };
        seed_cutoff(&context, descriptor(), Some(cutoff)).await;
        let admitted = drive(admit(None), &context).await;
        assert_eq!(admitted, Err(TransferAdmitError::CutOff));
    }
}
