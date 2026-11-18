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
use aruna_core::events::{Event, GroupEvent, GroupResult, NetEvent};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
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
use aruna_core::types::{Key, KeySpace, Value};
use aruna_plugins::artifact::{ArtifactDigest, check_artifact};
use thiserror::Error;
use tracing::{info, warn};

use super::artifacts::{ArtifactError, managers};
use super::copies_needed;
use super::copy_rows::{StoreCopy, chunk_count, chunk_of};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::realm::mutate_placement::is_management;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

#[derive(Debug, Error, PartialEq)]
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

impl From<IoError> for PublishError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

/// The entry of a checked artifact and the realm config it was checked against.
#[derive(Debug, PartialEq)]
pub struct PublishPlan {
    config: RealmConfigDocument,
    entry: PublishedPlugin,
}

impl PublishPlan {
    /// Stores the artifact on this node, as the copy the publish counts first.
    pub fn store_copy<'a>(&self, artifact: &'a [u8]) -> StoreCopy<'a> {
        let (digest, now_ms) = (&self.entry.digest, self.entry.published_at_ms);
        StoreCopy::new(&self.config, digest, artifact, now_ms)
    }
}

/// What a checked publish does next.
#[derive(Debug, PartialEq)]
pub enum PublishStep {
    /// The identical version is already published.
    Known(PublishedPlugin),
    Store(Box<PublishPlan>),
}

/// Checks `artifact` for a realm admin on this management node and writes nothing.
#[derive(Debug, PartialEq)]
pub struct CheckPublish {
    actor: Actor,
    auth: AuthContext,
    entry: PublishedPlugin,
    allowed: bool,
}

impl CheckPublish {
    pub fn new(
        actor: &Actor,
        auth: &AuthContext,
        artifact: &[u8],
        now_ms: u64,
    ) -> Result<Self, PublishError> {
        if artifact.len() > MAX_ARTIFACT {
            return Err(PublishError::TooLarge);
        }
        let invalid = |error: String| PublishError::Invalid(error);
        let manifest = check_artifact(artifact).map_err(|error| invalid(error.to_string()))?;
        let stored = serde_json::to_string(&manifest);
        let entry = PublishedPlugin {
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            digest: ArtifactDigest::of(artifact).to_string(),
            size: artifact.len() as u64,
            manifest: stored.map_err(|error| invalid(error.to_string()))?,
            published_by: actor.user_id,
            published_at_ms: now_ms,
        };
        Ok(Self {
            actor: actor.clone(),
            auth: auth.clone(),
            entry,
            allowed: false,
        })
    }
}

impl Operation for CheckPublish {
    type Output = PublishStep;
    type Error = PublishError;

    fn first(&mut self) -> Result<Next<PublishStep>, PublishError> {
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.auth.clone(),
            path: policy_admin_path(self.actor.realm_id),
            required_permission: Permission::WRITE,
        });
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PublishStep>, PublishError> {
        if !self.allowed {
            let allowed = result.child::<bool, AuthorizationError>()?;
            if !allowed.unwrap_or(false) {
                return Err(PublishError::Unauthorized);
            }
            self.allowed = true;
            let read = GetConfigOperation::new(self.actor.realm_id);
            return Ok(Next::Call(IoCall::child(read)));
        }
        let config = result.child::<RealmConfigDocument, GetConfigError>()?;
        let config = config
            .map_err(|error| PublishError::Unavailable(format!("realm config: {error:?}")))?;
        if !is_management(&config, self.actor.node_id) {
            return Err(PublishError::NotManagementNode);
        }
        if let Some(existing) = same_version(&config, &self.entry)? {
            return Ok(Next::Done(PublishStep::Known(existing)));
        }
        let entry = self.entry.clone();
        let plan = Box::new(PublishPlan { config, entry });
        Ok(Next::Done(PublishStep::Store(plan)))
    }
}

/// Copies a locally stored artifact to the other management nodes and writes the entry once a
/// majority holds it durably; otherwise nothing is published.
#[derive(Debug, PartialEq)]
pub struct FinishPublish {
    actor: Actor,
    auth: AuthContext,
    plan: PublishPlan,
    artifact: Vec<u8>,
    stored: Option<usize>,
}

impl FinishPublish {
    pub fn new(actor: Actor, auth: AuthContext, plan: PublishPlan, artifact: Vec<u8>) -> Self {
        Self {
            actor,
            auth,
            plan,
            artifact,
            stored: None,
        }
    }

    /// The entry write once enough copies exist; its transaction counts them again.
    fn write(&mut self, result: IoResult) -> Result<Next<PublishedPlugin>, PublishError> {
        let copies = result.child::<BTreeSet<NodeId>, CopyError>()?;
        let mut copies = copies.unwrap_or_default();
        copies.insert(self.actor.node_id);
        let managers = managers(&self.plan.config).count();
        let (stored, needed) = (copies.len(), copies_needed(managers));
        if stored < needed {
            return Err(PublishError::TooFewCopies { stored, needed });
        }
        self.stored = Some(stored);
        let write = PublishEntryOperation::new(PublishEntryConfig {
            actor: self.actor.clone(),
            auth_context: self.auth.clone(),
            plugin: self.plan.entry.clone(),
            copies,
        });
        Ok(Next::Call(IoCall::child(write)))
    }
}

impl Operation for FinishPublish {
    type Output = PublishedPlugin;
    type Error = PublishError;

    fn first(&mut self) -> Result<Next<PublishedPlugin>, PublishError> {
        let local = self.actor.node_id;
        let peers = managers(&self.plan.config).filter(|node| *node != local);
        let digest = self.plan.entry.digest.clone();
        let artifact = std::mem::take(&mut self.artifact);
        let copy = copy_artifact(peers.collect(), digest, artifact);
        Ok(Next::Call(IoCall::child(copy)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PublishedPlugin>, PublishError> {
        let Some(stored) = self.stored else {
            return self.write(result);
        };
        let published = result.child::<PublishedPlugin, EntryError>()??;
        let (name, version) = (&published.name, &published.version);
        let digest = &published.digest;
        info!(%name, %version, %digest, stored, "Published a plugin version");
        Ok(Next::Done(published))
    }
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
pub fn copy_artifact(peers: Vec<NodeId>, digest: String, artifact: Vec<u8>) -> CopyArtifact {
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
/// caller's realm admin check, and wakes the outbox drain after the commit. An identical entry
/// is kept; another digest for the same version is refused.
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
}

/// A lost transaction stays a storage conflict, so callers can tell it from other failures.
impl From<IoError> for EntryError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
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
        match result.child::<bool, AuthorizationError>()? {
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

    /// The entry as written or found; the outbox drain wakes once the write committed.
    fn finish(&mut self) -> Result<Next<PublishedPlugin>, EntryError> {
        let output = self.output.take().ok_or(IoError::Unfinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for PublishEntryOperation {
    type Output = PublishedPlugin;
    type Error = EntryError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
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
        Ok(Next::Call(IoCall::child(check)))
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

#[cfg(test)]
#[path = "publish_tests.rs"]
mod tests;
