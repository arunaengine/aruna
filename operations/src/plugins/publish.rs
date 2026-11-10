//! Publishes a plugin version: stores the artifact on a majority of management nodes, then
//! writes its realm config entry. Plain copies with acknowledgements, never a vote.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::plugins::{MAX_ARTIFACT, PublishedPlugin};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::task::TaskEvent;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use aruna_plugins::artifact::{ArtifactDigest, check_artifact};
use smallvec::smallvec;
use thiserror::Error;
use tracing::{info, warn};

use super::artifacts::{ArtifactError, managers};
use super::copies::{chunk_count, chunk_of, store_local};
use super::copies_needed;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::placement::target_placement_ref;
use crate::realm::get_config::GetConfigOperation;
use crate::realm::mutate_placement::is_management;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

#[derive(Debug, Error)]
pub enum PublishError {
    #[error("the artifact exceeds {MAX_ARTIFACT} bytes")]
    TooLarge,
    #[error("the artifact is not a valid plugin: {0}")]
    Invalid(String),
    #[error("caller may not publish plugins")]
    Unauthorized,
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error("version {0} of this plugin is already published with other bytes")]
    VersionTaken(String),
    #[error(
        "{stored} of {needed} needed management nodes stored the artifact; nothing was published"
    )]
    TooFewCopies { stored: usize, needed: usize },
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
    #[error(transparent)]
    Entry(#[from] EntryError),
    #[error("{0}")]
    Unavailable(String),
}

/// Checks and publishes `artifact` as a realm admin on this management node. Answers only
/// after a majority of management nodes stored it durably; otherwise nothing is published.
pub async fn publish_plugin(
    ctx: &DriverContext,
    actor: Actor,
    auth_context: AuthContext,
    artifact: Vec<u8>,
) -> Result<PublishedPlugin, PublishError> {
    if artifact.len() > MAX_ARTIFACT {
        return Err(PublishError::TooLarge);
    }
    let manifest =
        check_artifact(&artifact).map_err(|err| PublishError::Invalid(err.to_string()))?;
    let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
        auth_context: auth_context.clone(),
        path: policy_admin_path(actor.realm_id),
        required_permission: Permission::WRITE,
    });
    if !drive(check, ctx).await.unwrap_or(false) {
        return Err(PublishError::Unauthorized);
    }
    let config = drive(GetConfigOperation::new(actor.realm_id), ctx)
        .await
        .map_err(|err| PublishError::Unavailable(format!("realm config: {err:?}")))?;
    if !is_management(&config, actor.node_id) {
        return Err(PublishError::NotManagementNode);
    }
    let digest = ArtifactDigest::of(&artifact);
    let entry = PublishedPlugin {
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        digest: digest.to_string(),
        size: artifact.len() as u64,
        manifest: serde_json::to_string(&manifest)
            .map_err(|err| PublishError::Invalid(err.to_string()))?,
        published_by: actor.user_id,
        published_at_ms: unix_timestamp_millis(),
    };
    if let Some(existing) = same_version(&config, &entry)? {
        return Ok(existing);
    }
    store_local(ctx, &config, digest.as_str(), &artifact).await?;
    let digest = digest.to_string();
    let stored = 1 + copy_to_managers(ctx, &config, actor.node_id, &digest, &artifact).await;
    let needed = copies_needed(managers(&config).count());
    if stored < needed {
        return Err(PublishError::TooFewCopies { stored, needed });
    }
    let write = PublishEntryOperation::new(PublishEntryConfig {
        actor,
        auth_context,
        plugin: entry,
    });
    let published = drive(write, ctx).await?;
    info!(name = %published.name, version = %published.version, digest = %published.digest, stored, "Published a plugin version");
    Ok(published)
}

/// The already published identical version, or an error when the version has other bytes.
fn same_version(
    config: &RealmConfigDocument,
    entry: &PublishedPlugin,
) -> Result<Option<PublishedPlugin>, PublishError> {
    let existing = (config.plugins.iter())
        .find(|plugin| plugin.name == entry.name && plugin.version == entry.version);
    match existing {
        Some(plugin) if plugin.digest == entry.digest => Ok(Some(plugin.clone())),
        Some(_) => Err(PublishError::VersionTaken(entry.version.clone())),
        None => Ok(None),
    }
}

/// Sends the artifact to every other management node at once; returns how many stored it.
async fn copy_to_managers(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: aruna_core::NodeId,
    digest: &str,
    artifact: &[u8],
) -> usize {
    let Some(metadata) = ctx.metadata_handle.as_ref() else {
        return 0;
    };
    let peers = managers(config).filter(|node| *node != local);
    let copies = peers.map(|node_id| async move {
        // Chunks go one after another; the last answer says whether the copy is complete.
        let mut complete = Ok(false);
        for index in 0..chunk_count(artifact.len() as u64) {
            complete = metadata
                .request_store_artifact(node_id, chunk_of(digest, artifact, index))
                .await;
            if complete.is_err() {
                break;
            }
        }
        if let Err(error) = &complete {
            warn!(%node_id, ?error, "A management node did not store a plugin artifact");
        }
        complete == Ok(true)
    });
    let results = futures_util::future::join_all(copies).await;
    results.into_iter().filter(|stored| *stored).count()
}

#[derive(Debug, PartialEq)]
pub struct PublishEntryConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays restricted.
    pub auth_context: AuthContext,
    pub plugin: PublishedPlugin,
}

/// Writes one published plugin version into the realm config. An identical entry is kept;
/// another digest for the same version is refused.
#[derive(Debug, PartialEq)]
pub struct PublishEntryOperation {
    config: PublishEntryConfig,
    txn_id: Option<TxnId>,
    state: EntryState,
    output: Option<Result<PublishedPlugin, EntryError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum EntryState {
    Init,
    Auth,
    StartTransaction,
    ReadCurrent,
    WriteState { stale: Vec<(KeySpace, Key)> },
    DeleteConflicts,
    CommitTransaction,
    ScheduleSyncDrain,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum EntryError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("realm config document missing")]
    ConfigMissing,
    #[error("caller may not publish plugins")]
    Unauthorized,
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error("version {0} of this plugin is already published with other bytes")]
    VersionTaken(String),
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl PublishEntryOperation {
    pub fn new(config: PublishEntryConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: EntryState::Init,
            output: None,
        }
    }

    fn document(&self) -> DocumentTarget {
        DocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn emit_read(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = EntryState::ReadCurrent;
        let document = self.document();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key()
                ),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&self.admin_target())
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, EntryError> {
        let txn_id = self.txn_id.ok_or(EntryError::MissingTransaction)?;
        let document_value = document_value.ok_or(EntryError::ConfigMissing)?;
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        if !is_management(&document, self.config.actor.node_id) {
            return Err(EntryError::NotManagementNode);
        }
        let target = self.admin_target();
        let previous = (reducer_state_value.as_ref())
            .map(|value| decode_reducer_state(value.as_ref()).map_err(ConversionError::from))
            .transpose()?;
        if previous
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentError::TargetMismatch.into());
        }
        let mut reducer_state = previous
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(target));
        let plugin = self.config.plugin.clone();
        let existing = (reducer_state.materialized_plugins().into_iter())
            .find(|entry| entry.name == plugin.name && entry.version == plugin.version);
        match existing {
            Some(entry) if entry.digest == plugin.digest => {
                // Already published by an earlier attempt: nothing to write.
                self.output = Some(Ok(entry));
                self.state = EntryState::CommitTransaction;
                return Ok(smallvec![Effect::Storage(
                    StorageEffect::CommitTransaction { txn_id }
                )]);
            }
            Some(_) => return Err(EntryError::VersionTaken(plugin.version)),
            None => {}
        }
        let operation = AdminDocumentOperation::ConfigPluginSet {
            name: plugin.name.clone(),
            version: plugin.version.clone(),
            plugin: Some(plugin.clone()),
        };
        let event = reducer_state.apply_operation(&self.config.actor, operation)?;
        document.plugins = reducer_state.materialized_plugins();
        let document_target = self.document();
        let placement = target_placement_ref(&document, &document_target, Default::default());
        let record = new_identified_record(
            event.event_id,
            self.config.actor.node_id,
            document_target.clone(),
            Vec::new(),
            DocumentOutboxEvent::admin(event),
            placement,
            false,
        );
        let mut writes = vec![
            (
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                document.to_bytes(&self.config.actor)?.into(),
            ),
            reducer_state_entry(&reducer_state)?,
            outbox_write_entry(&record).map_err(ConversionError::from)?,
        ];
        writes.extend(conflict_write_entries(&reducer_state)?);
        self.output = Some(Ok(plugin));
        self.state = EntryState::WriteState {
            stale: stale_conflict_deletes(previous.as_ref(), Some(&reducer_state)),
        };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(EntryError::MissingTransaction);
        };
        self.state = EntryState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: EntryError) -> Effects {
        let cleanup = self.abort();
        self.state = EntryState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(EntryError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for PublishEntryOperation {
    type Output = PublishedPlugin;
    type Error = EntryError;

    fn start(&mut self) -> Effects {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return self.fail(EntryError::Unauthorized);
        }
        self.state = EntryState::Auth;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.actor.realm_id),
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            EntryState::Auth => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => {
                            self.state = EntryState::StartTransaction;
                            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                                read: false
                            })]
                        }
                        Ok(false) => self.fail(EntryError::Unauthorized),
                        Err(AuthorizationError::StorageError(error)) => self.fail(error.into()),
                        Err(error) => {
                            warn!(%error, "Plugin publish authorization check failed");
                            self.fail(EntryError::Unauthorized)
                        }
                    }
                }
                other => self.unexpected("authorization result", format!("{other:?}")),
            },
            EntryState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction start result", format!("{other:?}")),
            },
            EntryState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document), (_, state)] = values.as_slice() else {
                        return self
                            .unexpected("realm config and reducer state", format!("{values:?}"));
                    };
                    match self.emit_write(document.clone(), state.clone()) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("storage batch read result", format!("{other:?}")),
            },
            EntryState::WriteState { stale } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) if stale.is_empty() => {
                    self.emit_commit()
                }
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(EntryError::MissingTransaction);
                    };
                    self.state = EntryState::DeleteConflicts;
                    smallvec![Effect::Storage(StorageEffect::BatchDelete {
                        deletes: stale,
                        txn_id: Some(txn_id),
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("storage batch write result", format!("{other:?}")),
            },
            EntryState::DeleteConflicts => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => self.emit_commit(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("storage batch delete result", format!("{other:?}")),
            },
            EntryState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = EntryState::ScheduleSyncDrain;
                    smallvec![schedule_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected("transaction commit result", format!("{other:?}")),
            },
            EntryState::ScheduleSyncDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = EntryState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule the outbox drain; the durable outbox stays retryable");
                    self.state = EntryState::Finish;
                    smallvec![]
                }
                other => self.unexpected("outbox drain timer schedule", format!("{other:?}")),
            },
            EntryState::Finish | EntryState::Error | EntryState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, EntryState::Finish | EntryState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if !self.is_complete() {
            return Err(EntryError::NotFinished);
        }
        self.output.unwrap_or(Err(EntryError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
#[path = "publish_tests.rs"]
mod tests;
