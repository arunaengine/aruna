//! Creates a service account: a user without login subjects that one group owns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::USER_KEYSPACE;
use aruna_core::UserId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{reducer_state_entry, sync_revision_entry};
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::user::User;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::GroupId;
use aruna_core::user::validation::{INSTALL_ATTRIBUTE, SERVICE_GROUP_ATTRIBUTE};
use byteview::ByteView;
use std::collections::HashMap;
use thiserror::Error;

use super::check::{ServiceCheckConfig, ServiceCheckError, ServiceCheckOperation};
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use crate::users::oidc_user::initial_sync_change;
use aruna_core::placement::target_placement_ref;

const MAX_NAME_LEN: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateServiceConfig {
    pub actor: Actor,
    pub auth_context: AuthContext,
    pub group_id: GroupId,
    pub name: String,
    pub user_id: UserId,
    /// The plugin install the account acts for; it is marked with it for good.
    pub install: Option<ulid::Ulid>,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateServiceError {
    #[error("the plugin install changed meanwhile; this change is stale")]
    Superseded,
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error(transparent)]
    AdminDocument(#[from] AdminDocumentError),
    #[error("caller may not administer this group's service accounts")]
    Unauthorized,
    #[error("service account name must be non-empty and at most {MAX_NAME_LEN} bytes")]
    InvalidName,
    #[error("the account's bucket cut over to a new holder set; retry the creation")]
    PlacementFenced,
    #[error("service account creation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for CreateServiceError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "CreateService".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

type ServiceTurn = Result<Next<User>, CreateServiceError>;

/// Checks that the caller administers the group's service accounts, then writes the account in
/// one write transaction.
#[derive(Debug, PartialEq)]
pub struct CreateServiceOperation {
    config: CreateServiceConfig,
    guard: Option<aruna_core::plugin_installs::InstallGuard>,
    authorized: bool,
}

impl CreateServiceOperation {
    pub fn new(config: CreateServiceConfig) -> Self {
        Self {
            config,
            guard: None,
            authorized: false,
        }
    }

    /// Creates the account only while the install record is still at the guard's revision.
    pub fn guarded(mut self, guard: aruna_core::plugin_installs::InstallGuard) -> Self {
        self.guard = Some(guard);
        self
    }
}

impl Operation for CreateServiceOperation {
    type Output = User;
    type Error = CreateServiceError;

    fn first(&mut self) -> ServiceTurn {
        let name = self.config.name.trim();
        if name.is_empty() || name.len() > MAX_NAME_LEN {
            return Err(CreateServiceError::InvalidName);
        }
        Ok(Next::Call(IoCall::child(ServiceCheckOperation::new(
            ServiceCheckConfig {
                auth_context: self.config.auth_context.clone(),
                group_id: self.config.group_id,
                target: None,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> ServiceTurn {
        if self.authorized {
            return result.child::<User, CreateServiceError>()?.map(Next::Done);
        }
        let checked = result.child::<Option<User>, ServiceCheckError>()?;
        if !ServiceCheckError::allowed(checked)? {
            return Err(CreateServiceError::Unauthorized);
        }
        self.authorized = true;
        let write = ServiceWrite::new(self.config.clone(), self.guard);
        Ok(Next::Call(IoCall::child(write)))
    }
}

/// What the account write transaction waits for.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Stage {
    Config,
    Write,
    Fence,
}

/// Reads the realm config, writes the account, its sync revision and admin events, and checks
/// the bucket fence, all in one write transaction.
#[derive(Debug, PartialEq)]
struct ServiceWrite {
    config: CreateServiceConfig,
    guard: Option<aruna_core::plugin_installs::InstallGuard>,
    fence: aruna_core::placement::fence::WriteFence,
    stage: Stage,
    user: Option<User>,
}

impl ServiceWrite {
    fn new(
        config: CreateServiceConfig,
        guard: Option<aruna_core::plugin_installs::InstallGuard>,
    ) -> Self {
        Self {
            config,
            guard,
            fence: Default::default(),
            stage: Stage::Config,
            user: None,
        }
    }

    fn handle_config(&mut self, value: Option<aruna_core::types::Value>) -> ServiceTurn {
        let config = value
            .as_deref()
            .map(RealmConfigDocument::from_bytes)
            .transpose()?;
        if (self.guard).is_some_and(|guard| !guard.admits(config.as_ref())) {
            return Err(CreateServiceError::Superseded);
        }
        self.write_user(config.as_ref())
    }

    /// Writes the record, its sync revision and the admin events other nodes replay.
    fn write_user(&mut self, config: Option<&RealmConfigDocument>) -> ServiceTurn {
        let group = self.config.group_id.to_string();
        let mut user = User {
            user_id: self.config.user_id,
            name: self.config.name.trim().to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: HashMap::from([(SERVICE_GROUP_ATTRIBUTE.to_string(), group.clone())]),
        };
        let marker = (self.config.install).map(|install| (INSTALL_ATTRIBUTE, install.to_string()));
        user.attributes
            .extend(marker.clone().map(|(key, value)| (key.to_string(), value)));
        let actor = &self.config.actor;
        let document_target = DocumentTarget::User {
            user_id: user.user_id,
        };
        let placement = config
            .map(|config| target_placement_ref(config, &document_target, Default::default()))
            .unwrap_or(PlacementRef::NIL);
        if let Some(config) = config {
            self.fence.add(actor.realm_id, config, [placement]);
        }
        let generation = self.fence.generation(&actor.realm_id, &placement);
        let mut reducer_state = AdminDocumentState::new(AdminDocumentTarget::User {
            user_id: user.user_id,
        });
        let mut writes = vec![
            (
                USER_KEYSPACE.to_string(),
                ByteView::from(user.user_id.to_bytes()),
                ByteView::from(user.to_bytes(actor)?),
            ),
            sync_revision_entry(&document_target, &initial_sync_change(actor, placement))?,
        ];
        // Ownership first, so receivers admit the name change as the owning group's administrator.
        let marker = marker.map(|(key, value)| AdminDocumentOperation::UserAttributeSet {
            key: key.to_string(),
            value,
        });
        let operations = [
            Some(AdminDocumentOperation::UserAttributeSet {
                key: SERVICE_GROUP_ATTRIBUTE.to_string(),
                value: group.clone(),
            }),
            marker,
            Some(AdminDocumentOperation::UserNameSet {
                name: user.name.clone(),
            }),
        ];
        for operation in operations.into_iter().flatten() {
            let event = reducer_state.apply_operation(actor, operation)?;
            let record = new_identified_record(
                event.event_id,
                actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(event),
                placement,
                true,
            )
            .fenced_at(generation);
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.push(reducer_state_entry(&reducer_state)?);
        let mut audit = AdminAuditRecord::new(
            AdminAuditAction::ServiceAccountCreated,
            actor.user_id,
            user.user_id,
        );
        audit.group_id = Some(self.config.group_id);
        writes.extend(audit.entries()?);
        self.user = Some(user);
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// A failed drain timer leaves the durable outbox retryable.
    fn committed(&mut self) -> ServiceTurn {
        let output = self.user.take().ok_or(CreateServiceError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for ServiceWrite {
    type Output = User;
    type Error = CreateServiceError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ServiceTurn {
        let key = ByteView::from(*self.config.actor.realm_id.as_bytes());
        Ok(Next::Call(IoCall::Read(vec![(
            REALM_CONFIG_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn next(&mut self, result: IoResult) -> ServiceTurn {
        match self.stage {
            Stage::Config => {
                let [value] = values::<1>(result)?;
                self.handle_config(value)
            }
            Stage::Write => {
                done(result)?;
                if self.fence.is_empty() {
                    return self.committed();
                }
                self.stage = Stage::Fence;
                Ok(Next::Call(IoCall::Read(self.fence.reads())))
            }
            Stage::Fence => {
                let IoResult::Values(values) = result else {
                    return Err(CreateServiceError::NotFinished);
                };
                let keys = self.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.fence.admits(&rows) {
                    return Err(CreateServiceError::PlacementFenced);
                }
                self.committed()
            }
        }
    }
}
