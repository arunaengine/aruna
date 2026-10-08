//! Destination side decisions of an export from another realm: signing an import intent and
//! admitting an intent with its grant against the realm's current settings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::events::Event;
use aruna_core::federation::{FederationError, Signed};
use aruna_core::handoff::descriptor_digest;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::transfer::{
    ExportGrant, ImportDestination, ImportIntent, MAX_TRANSFER_SECS, TransferError, check_grant,
    check_intent, import_key,
};
use aruna_core::types::Effects;
use thiserror::Error;
use ulid::Ulid;

use crate::realm::get_config::{GetConfigError, GetConfigOperation};

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
}

/// Reads the realm config, then decides once from it.
#[derive(Debug, PartialEq)]
struct ConfigStep<T> {
    read: Option<GetConfigOperation>,
    output: Option<Result<T, TransferAdmitError>>,
}

impl<T> ConfigStep<T> {
    fn new(realm_id: RealmId) -> Self {
        Self {
            read: Some(GetConfigOperation::new(realm_id)),
            output: None,
        }
    }

    fn start(&mut self) -> Effects {
        self.read.as_mut().map(Operation::start).unwrap_or_default()
    }

    fn step(
        &mut self,
        event: Event,
        decide: impl FnOnce(RealmConfigDocument) -> Result<T, TransferAdmitError>,
    ) -> Effects {
        let Some(read) = self.read.as_mut() else {
            return Effects::new();
        };
        let effects = read.step(event);
        if read.is_complete()
            && let Some(read) = self.read.take()
        {
            self.output = Some(read.finalize().map_err(Into::into).and_then(decide));
        }
        effects
    }

    fn abort(&mut self) -> Effects {
        self.read.as_mut().map(Operation::abort).unwrap_or_default()
    }
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
}

/// Signs an import intent for this realm's current descriptor. Authorization comes first.
#[derive(Debug, PartialEq)]
pub struct IssueIntentOperation {
    config: IssueIntentConfig,
    step: ConfigStep<Signed<ImportIntent>>,
}

impl IssueIntentOperation {
    pub fn new(config: IssueIntentConfig) -> Self {
        let step = ConfigStep::new(config.realm_id);
        Self { config, step }
    }
}

impl Operation for IssueIntentOperation {
    type Output = Signed<ImportIntent>;
    type Error = TransferAdmitError;

    fn start(&mut self) -> Effects {
        self.step.start()
    }

    fn step(&mut self, event: Event) -> Effects {
        let config = &self.config;
        self.step.step(event, |realm| {
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
                intent_id: Ulid::generate(),
            };
            Ok(Signed::sign(intent, &config.node_capabilities)?)
        })
    }

    fn is_complete(&self) -> bool {
        self.step.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.step
            .output
            .unwrap_or(Err(TransferAdmitError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.step.abort()
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
    step: ConfigStep<String>,
}

impl AdmitTransferOperation {
    pub fn new(config: AdmitTransferConfig) -> Self {
        let step = ConfigStep::new(config.realm_id);
        Self { config, step }
    }
}

impl Operation for AdmitTransferOperation {
    type Output = String;
    type Error = TransferAdmitError;

    fn start(&mut self) -> Effects {
        self.step.start()
    }

    fn step(&mut self, event: Event) -> Effects {
        let config = &self.config;
        self.step.step(event, |realm| {
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
            Ok(import_key(&grant.payload, &payload.destination)?)
        })
    }

    fn is_complete(&self) -> bool {
        self.step.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.step
            .output
            .unwrap_or(Err(TransferAdmitError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.step.abort()
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
