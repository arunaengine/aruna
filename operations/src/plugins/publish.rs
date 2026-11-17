//! Publishes a plugin version: stores the artifact on a majority of management nodes, then
//! writes its realm config entry. Plain copies with acknowledgements, never a vote.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, EffectGroup, GroupForm, NetEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, GroupEvent, GroupResult, NetEvent, SubOperationEvent};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::placement::target_placement_ref;
use aruna_core::plugins::{ArtifactStore, MAX_ARTIFACT, PublishedPlugin};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, KeySpace, Value};
use aruna_plugins::artifact::{ArtifactDigest, check_artifact};
use thiserror::Error;
use tracing::{info, warn};

use super::artifacts::{ArtifactError, managers};
use super::copies::{chunk_count, chunk_of, store_local};
use super::copies_needed;
use super::install::wake_drain;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::realm::mutate_placement::is_management;
use crate::sync::outbox_record::{new_identified_record, outbox_write_entry};

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
    let published_at = unix_timestamp_millis();
    let entry = PublishedPlugin {
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        digest: digest.to_string(),
        size: artifact.len() as u64,
        manifest: serde_json::to_string(&manifest)
            .map_err(|err| PublishError::Invalid(err.to_string()))?,
        published_by: actor.user_id,
        published_at_ms: published_at,
    };
    if let Some(existing) = same_version(&config, &entry)? {
        return Ok(existing);
    }
    store_local(ctx, &config, digest.as_str(), &artifact, published_at).await?;
    let peers = managers(&config)
        .filter(|node| *node != actor.node_id)
        .collect();
    let copy = copy_artifact(peers, digest.to_string(), artifact);
    let mut copies = drive(copy, ctx).await.unwrap_or_default();
    copies.insert(actor.node_id);
    let (stored, needed) = (copies.len(), copies_needed(managers(&config).count()));
    if stored < needed {
        return Err(PublishError::TooFewCopies { stored, needed });
    }
    // The entry transaction counts these copies again against the managers it reads.
    let write = PublishEntryOperation::new(PublishEntryConfig {
        actor,
        auth_context,
        plugin: entry,
        copies,
    });
    let published = drive(write, ctx).await?;
    wake_drain(ctx).await;
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

/// Sends the artifact to every other management node at once, the chunks of one node one after
/// another; answers the nodes that confirmed a complete, synced copy. A node that refuses or
/// fails a chunk gets no more.
pub(crate) fn copy_artifact(peers: Vec<NodeId>, digest: String, artifact: Vec<u8>) -> CopyArtifact {
    CopyArtifact {
        peers,
        digest,
        artifact,
        members: Vec::new(),
        confirmed: BTreeSet::new(),
    }
}

#[derive(Debug, PartialEq)]
pub struct CopyArtifact {
    peers: Vec<NodeId>,
    digest: String,
    artifact: Vec<u8>,
    /// The node and chunk of each window member, by member index.
    members: Vec<(NodeId, u32)>,
    confirmed: BTreeSet<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CopyError {
    #[error("unexpected answer while copying an artifact: {got}")]
    Unexpected { got: &'static str },
    #[error(transparent)]
    Call(#[from] IoError),
}

impl CopyArtifact {
    /// The effect that sends chunk `index` to `node_id`, as window member `members.len()`.
    fn send(&mut self, node_id: NodeId, index: u32) -> Effect {
        self.members.push((node_id, index));
        let chunk = chunk_of(&self.digest, &self.artifact, index);
        let store = ArtifactStore { node_id, chunk };
        Effect::Net(NetEffect::ArtifactStore(Box::new(store)))
    }

    /// The next chunk for the node of window member `index`, or nothing more for that node.
    fn member(
        &mut self,
        index: usize,
        result: GroupResult,
    ) -> Result<Next<BTreeSet<NodeId>>, CopyError> {
        let Some(&(node_id, chunk)) = self.members.get(index) else {
            let got = "GroupEvent::Member";
            return Err(CopyError::Unexpected { got });
        };
        let stored = match result {
            GroupResult::Done(event) => match *event {
                Event::Net(NetEvent::ArtifactStored(stored)) => stored,
                other => return Err(CopyError::Unexpected { got: other.label() }),
            },
            GroupResult::Unfinished | GroupResult::Refused => Err("not run".to_string()),
        };
        match stored {
            Ok(_) if chunk + 1 < chunk_count(self.artifact.len() as u64) => {
                let next = self.send(node_id, chunk + 1);
                return Ok(Next::Call(IoCall::Effect(Box::new(next))));
            }
            Ok(true) => {
                self.confirmed.insert(node_id);
            }
            Ok(false) => {}
            Err(error) => {
                warn!(%node_id, %error, "A management node did not store a plugin artifact");
            }
        }
        Ok(Next::Wait)
    }
}

/// Chunks already sent when the copy stops stay as an unfinished copy, which the copy sweep
/// removes.
impl Operation for CopyArtifact {
    type Output = BTreeSet<NodeId>;
    type Error = CopyError;

    fn first(&mut self) -> Result<Next<Self::Output>, CopyError> {
        let peers = std::mem::take(&mut self.peers);
        if peers.is_empty() || self.artifact.is_empty() {
            return Ok(Next::Done(BTreeSet::new()));
        }
        let limit = peers.len();
        let members = peers.into_iter().map(|node| self.send(node, 0)).collect();
        let form = GroupForm::Window;
        let group = EffectGroup {
            members,
            limit,
            form,
        };
        Ok(Next::Call(IoCall::Group(group)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CopyError> {
        match result {
            IoResult::Group(GroupEvent::Member { index, result }) => self.member(index, result),
            IoResult::Group(GroupEvent::Closed) => {
                Ok(Next::Done(std::mem::take(&mut self.confirmed)))
            }
            other => Err(CopyError::Unexpected { got: other.label() }),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct PublishEntryConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays restricted.
    pub auth_context: AuthContext,
    pub plugin: PublishedPlugin,
    /// Management nodes that confirmed a complete, synced copy, this one included.
    pub copies: BTreeSet<aruna_core::NodeId>,
}

/// Writes one published plugin version into the realm config in one transaction, after the
/// caller's realm admin check. An identical entry is kept; another digest for the same version
/// is refused. [`publish_plugin`] wakes the outbox drain after the commit.
#[derive(Debug, PartialEq)]
pub struct PublishEntryOperation {
    config: PublishEntryConfig,
    phase: EntryPhase,
    output: Option<PublishedPlugin>,
}

#[derive(Debug, PartialEq)]
enum EntryPhase {
    Auth,
    Read,
    Write { stale: Vec<(KeySpace, Key)> },
    DeleteConflicts,
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
    #[error(
        "{stored} of {needed} needed current management nodes stored the artifact; nothing was published"
    )]
    TooFewCopies { stored: usize, needed: usize },
    #[error(transparent)]
    Call(IoError),
}

/// A lost transaction stays a storage conflict, so callers can tell it from other failures.
impl From<IoError> for EntryError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            other => Self::Call(other),
        }
    }
}

impl PublishEntryOperation {
    pub fn new(config: PublishEntryConfig) -> Self {
        Self {
            config,
            phase: EntryPhase::Auth,
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

    fn authorized(&mut self, result: IoResult) -> Result<IoCall, EntryError> {
        let event = match result {
            IoResult::Event(event) => *event,
            other => return Err(unexpected(other.label())),
        };
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return Err(unexpected(event.label()));
        };
        match allowed {
            Ok(true) => {}
            Ok(false) => return Err(EntryError::Unauthorized),
            Err(AuthorizationError::StorageError(error)) => return Err(error.into()),
            Err(error) => {
                warn!(%error, "Plugin publish authorization check failed");
                return Err(EntryError::Unauthorized);
            }
        }
        self.phase = EntryPhase::Read;
        let document = self.document();
        Ok(IoCall::Read(vec![
            (
                document.storage_keyspace().to_string(),
                document.storage_key(),
            ),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&self.admin_target()),
            ),
        ]))
    }

    /// The write of the new entry, or `None` when an earlier attempt already published it.
    fn write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Option<IoCall>, EntryError> {
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
                self.output = Some(entry);
                return Ok(None);
            }
            Some(_) => return Err(EntryError::VersionTaken(plugin.version)),
            None => {}
        }
        let current: BTreeSet<_> = managers(&document).collect();
        let stored = self.config.copies.intersection(&current).count();
        let needed = copies_needed(current.len());
        if stored < needed {
            return Err(EntryError::TooFewCopies { stored, needed });
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
        self.output = Some(plugin);
        self.phase = EntryPhase::Write {
            stale: stale_conflict_deletes(previous.as_ref(), Some(&reducer_state)),
        };
        Ok(Some(IoCall::Write(writes)))
    }

    fn finish(&mut self) -> Result<Next<PublishedPlugin>, EntryError> {
        let output = self.output.take();
        output
            .map(Next::Done)
            .ok_or(EntryError::Call(IoError::Unfinished))
    }
}

impl Operation for PublishEntryOperation {
    type Output = PublishedPlugin;
    type Error = EntryError;

    fn transaction(&self) -> bool {
        true
    }

    fn first(&mut self) -> Result<Next<Self::Output>, EntryError> {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return Err(EntryError::Unauthorized);
        }
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.config.auth_context.clone(),
            path: policy_admin_path(self.config.actor.realm_id),
            required_permission: Permission::WRITE,
        });
        Ok(Next::Call(IoCall::Child(boxed_suboperation(
            check,
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, EntryError> {
        match std::mem::replace(&mut self.phase, EntryPhase::DeleteConflicts) {
            EntryPhase::Auth => Ok(Next::Call(self.authorized(result)?)),
            EntryPhase::Read => {
                let [document, state] = values::<2>(result)?;
                match self.write(document, state)? {
                    Some(write) => Ok(Next::Call(write)),
                    None => self.finish(),
                }
            }
            EntryPhase::Write { stale } => {
                done(result)?;
                match stale.is_empty() {
                    true => self.finish(),
                    false => Ok(Next::Call(IoCall::Delete(stale))),
                }
            }
            EntryPhase::DeleteConflicts => {
                done(result)?;
                self.finish()
            }
        }
    }
}

fn unexpected(got: &'static str) -> EntryError {
    EntryError::Call(IoError::Unexpected {
        expected: "an authorization result",
        got,
    })
}

#[cfg(test)]
#[path = "publish_tests.rs"]
mod tests;
