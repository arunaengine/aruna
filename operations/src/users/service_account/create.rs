//! Creates a service account: a user without login subjects that one group owns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::USER_KEYSPACE;
use aruna_core::UserId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::storage_entries::{reducer_state_entry, sync_revision_entry};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::user::User;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, TxnId};
use aruna_core::user::validation::SERVICE_GROUP_ATTRIBUTE;
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::HashMap;
use thiserror::Error;

use super::check::{ServiceCheckConfig, ServiceCheckError, ServiceCheckOperation};
use crate::placement::target_placement_ref;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use crate::users::oidc_user::initial_sync_change;

const MAX_NAME_LEN: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateServiceConfig {
    pub actor: Actor,
    pub auth_context: AuthContext,
    pub group_id: GroupId,
    pub name: String,
    pub user_id: UserId,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateServiceError {
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
        got: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
enum CreateServiceState {
    Init,
    Authorize,
    StartTransaction,
    ReadConfig { txn_id: TxnId },
    WriteUser { txn_id: TxnId, user: User },
    ReadFence { txn_id: TxnId, user: User },
    Commit { user: User },
    ScheduleDrain { user: User },
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct CreateServiceOperation {
    config: CreateServiceConfig,
    fence: crate::placement::fence::WriteFence,
    state: CreateServiceState,
    output: Option<Result<User, CreateServiceError>>,
}

impl CreateServiceOperation {
    pub fn new(config: CreateServiceConfig) -> Self {
        Self {
            config,
            fence: Default::default(),
            state: CreateServiceState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: CreateServiceError) -> Effects {
        let cleanup = self.abort();
        self.state = CreateServiceState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        self.fail(CreateServiceError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{event:?}"),
        })
    }

    fn handle_authorized(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: Ok(true) }) => {
                self.state = CreateServiceState::StartTransaction;
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false
                })]
            }
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: Ok(false) }) => {
                self.fail(CreateServiceError::Unauthorized)
            }
            Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: Err(error),
            }) => self.fail(error.into()),
            other => self.unexpected("authorization result", other),
        }
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected("transaction started", event);
        };
        self.state = CreateServiceState::ReadConfig { txn_id };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(*self.config.actor.realm_id.as_bytes()),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_config(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let value = match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            Event::Storage(StorageEvent::Error { error }) => return self.fail(error.into()),
            other => return self.unexpected("realm config read result", other),
        };
        let config = match value
            .as_deref()
            .map(RealmConfigDocument::from_bytes)
            .transpose()
        {
            Ok(config) => config,
            Err(error) => return self.fail(error.into()),
        };
        match self.write_user(txn_id, config.as_ref()) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    /// Writes the record, its sync revision and the admin events other nodes replay.
    fn write_user(
        &mut self,
        txn_id: TxnId,
        config: Option<&RealmConfigDocument>,
    ) -> Result<Effects, CreateServiceError> {
        let group = self.config.group_id.to_string();
        let user = User {
            user_id: self.config.user_id,
            name: self.config.name.trim().to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: HashMap::from([(SERVICE_GROUP_ATTRIBUTE.to_string(), group.clone())]),
        };
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
        for operation in [
            AdminDocumentOperation::UserAttributeSet {
                key: SERVICE_GROUP_ATTRIBUTE.to_string(),
                value: group.clone(),
            },
            AdminDocumentOperation::UserNameSet {
                name: user.name.clone(),
            },
        ] {
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
        self.state = CreateServiceState::WriteUser { txn_id, user };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_written(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        if !matches!(event, Event::Storage(StorageEvent::BatchWriteResult { .. })) {
            return self.unexpected("batch write result", event);
        }
        if self.fence.is_empty() {
            return self.commit(txn_id, user);
        }
        self.state = CreateServiceState::ReadFence { txn_id, user };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: self.fence.reads(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_fence(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected("fence read result", event);
        };
        if !self.fence.admits(&values) {
            return self.fail(CreateServiceError::PlacementFenced);
        }
        self.commit(txn_id, user)
    }

    fn commit(&mut self, txn_id: TxnId, user: User) -> Effects {
        self.state = CreateServiceState::Commit { user };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event, user: User) -> Effects {
        if !matches!(
            event,
            Event::Storage(StorageEvent::TransactionCommitted { .. })
        ) {
            return self.unexpected("transaction committed", event);
        }
        self.state = CreateServiceState::ScheduleDrain { user };
        smallvec![schedule_drain_effect()]
    }

    fn handle_drain(&mut self, event: Event, user: User) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. } | TaskEvent::Error { .. }) => {
                self.state = CreateServiceState::Finish;
                self.output = Some(Ok(user));
                smallvec![]
            }
            other => self.unexpected("admin document outbox drain timer schedule", other),
        }
    }
}

impl Operation for CreateServiceOperation {
    type Output = User;
    type Error = CreateServiceError;

    fn start(&mut self) -> Effects {
        let name = self.config.name.trim();
        if name.is_empty() || name.len() > MAX_NAME_LEN {
            return self.fail(CreateServiceError::InvalidName);
        }
        self.state = CreateServiceState::Authorize;
        smallvec![Effect::SubOperation(boxed_suboperation(
            ServiceCheckOperation::new(ServiceCheckConfig {
                auth_context: self.config.auth_context.clone(),
                group_id: self.config.group_id,
                target: None,
            }),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: ServiceCheckError::allowed(result),
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state.clone() {
            CreateServiceState::Authorize => self.handle_authorized(event),
            CreateServiceState::StartTransaction => self.handle_started(event),
            CreateServiceState::ReadConfig { txn_id } => self.handle_config(event, txn_id),
            CreateServiceState::WriteUser { txn_id, user } => {
                self.handle_written(event, txn_id, user)
            }
            CreateServiceState::ReadFence { txn_id, user } => {
                self.handle_fence(event, txn_id, user)
            }
            CreateServiceState::Commit { user } => self.handle_committed(event, user),
            CreateServiceState::ScheduleDrain { user } => self.handle_drain(event, user),
            CreateServiceState::Init => self.unexpected("no event before start", event),
            CreateServiceState::Finish | CreateServiceState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateServiceState::Finish | CreateServiceState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(CreateServiceError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            CreateServiceState::ReadConfig { txn_id }
            | CreateServiceState::WriteUser { txn_id, .. }
            | CreateServiceState::ReadFence { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}
