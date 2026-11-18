//! Reads and turns off a user's email opt-in, and keeps this node's cached mail settings in step
//! with the realm config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::outbound::{OutboundEffect, OutboundEvent, SmtpSettings};
use aruna_core::structs::execution::email::EmailPreference;
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use tracing::warn;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::notifications::opt_in::EmailError;
use crate::notifications::setting_rows::{
    ReadSettingOperation, SettingError, SettingValue, SettingWrite, WriteSettingOperation,
};
use crate::notifications::unexpected_event;
use crate::realm_vault::{PruneCacheOperation, RealmSecretError};

/// Whether a mail sender can be built from `settings`: a valid sender address and host.
pub fn check_smtp(settings: &SmtpSettings) -> Result<(), String> {
    aruna_outbound::SmtpMailer::new(settings).map(|_| ())
}

#[derive(Debug, Default, PartialEq)]
enum RefreshState {
    #[default]
    Config,
    Sender(Box<RealmConfigDocument>),
    Prune,
}

/// Drops the cached mail sender and realm secrets the realm config no longer names. Best effort:
/// a config read error keeps both, and every failure only warns.
#[derive(Debug, PartialEq)]
pub struct RefreshMailOperation {
    realm_id: Option<RealmId>,
    state: RefreshState,
}

impl RefreshMailOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id: Some(realm_id),
            state: RefreshState::Config,
        }
    }

    /// The refresh every sync result handler runs after applying `targets`: inbound, outbound
    /// and restored. It does nothing unless a realm config changed.
    pub fn applied(targets: &[DocumentTarget]) -> Self {
        let realm_id = targets.iter().find_map(|target| match target {
            DocumentTarget::RealmConfig { realm_id } => Some(*realm_id),
            _ => None,
        });
        Self {
            realm_id,
            state: RefreshState::Config,
        }
    }
}

impl Operation for RefreshMailOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        match self.realm_id {
            Some(realm_id) => Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id)))),
            None => Ok(Next::Done(())),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        match std::mem::take(&mut self.state) {
            RefreshState::Config => match result.child::<RealmConfigDocument, GetConfigError>()? {
                Ok(config) => {
                    let revision = config.email.as_ref().map(|profile| profile.revision);
                    self.state = RefreshState::Sender(Box::new(config));
                    let refresh = Effect::Outbound(OutboundEffect::RefreshMail(revision));
                    Ok(Next::Call(IoCall::from(refresh)))
                }
                Err(error) => {
                    warn!(error = %error, "Realm config read failed; cached mail settings stay");
                    Ok(Next::Done(()))
                }
            },
            RefreshState::Sender(config) => match result.event()? {
                Event::Outbound(OutboundEvent::MailRefreshed) => {
                    self.state = RefreshState::Prune;
                    let prune = PruneCacheOperation::new(&config);
                    Ok(Next::Call(IoCall::child(prune)))
                }
                other => Err(unexpected_event("OutboundEvent::MailRefreshed", &other)),
            },
            RefreshState::Prune => {
                if let Err(error) = result.child::<usize, RealmSecretError>()? {
                    warn!(error = %error, "Pruning cached realm secrets failed");
                }
                Ok(Next::Done(()))
            }
        }
    }
}

/// The caller's current opt-in as this node received it; `None` when email is off.
#[derive(Debug, PartialEq)]
pub struct ReadEmailOperation {
    owner: UserId,
}

impl ReadEmailOperation {
    pub fn new(owner: UserId) -> Self {
        Self { owner }
    }
}

impl Operation for ReadEmailOperation {
    type Output = Option<EmailPreference>;
    type Error = EmailError;

    fn first(&mut self) -> Result<Next<Self::Output>, EmailError> {
        let read = ReadSettingOperation::new(SettingChannel::Email, self.owner);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, EmailError> {
        let bytes = result.child::<Option<Vec<u8>>, SettingError>()??;
        let preference = bytes.as_deref().map(EmailPreference::from_bytes);
        Ok(Next::Done(preference.transpose()?))
    }
}

/// Turns email off with a newer register write; queued emails then send nothing.
#[derive(Debug, PartialEq)]
pub struct RemoveEmailOperation {
    write: SettingWrite,
}

impl RemoveEmailOperation {
    pub fn new(owner: UserId, local: NodeId, now_ms: u64) -> Self {
        let off = SettingValue::Email {
            categories: BTreeSet::new(),
            address: None,
        };
        Self {
            write: SettingWrite::new(owner, local, now_ms, off),
        }
    }
}

impl Operation for RemoveEmailOperation {
    type Output = ();
    type Error = EmailError;

    fn first(&mut self) -> Result<Next<()>, EmailError> {
        let write = WriteSettingOperation::new(self.write.clone());
        Ok(Next::Call(IoCall::child(write)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, EmailError> {
        let bytes = result.child::<Vec<u8>, SettingError>()??;
        EmailPreference::from_bytes(&bytes)?;
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
#[path = "email_tests.rs"]
mod tests;
