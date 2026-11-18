//! Checks the secrets a plugin install binds to its slots, each where it lives: group secrets in
//! this node's view, realm secrets in the config and node secrets on the receiver.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::NODE_SECRETS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{BoundScope, SecretBinding};
use aruna_core::plugins::NodeCheckError;
use aruna_core::secret_registers::{SecretBody, SecretEntry, SecretOwner};
use aruna_core::secrets::{SecretKind, SecretName, SecretRecord, SecretSettings};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_plugins::manifest::Manifest;
use aruna_plugins::trigger::TriggerKind;

use super::artifacts::ServingNode;
use super::entry::{EntryError, ReadEntry};
use super::peer::{ask_peer, peer_answer};
use super::rows::{read_call, values};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::realm::mutate_placement::node_kind;
use crate::secrets::node::record_key;

/// The vault kind a manifest slot kind names.
pub fn vault_kind(kind: aruna_plugins::manifest::SecretKind) -> Option<SecretKind> {
    let name = serde_json::to_value(kind).ok()?;
    SecretKind::parse(name.as_str()?).ok()
}

/// Who installs into which group, on which management node, for which receiver.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BindPlace {
    pub group_id: aruna_core::types::GroupId,
    pub local: Option<NodeId>,
    pub receiver: Option<NodeId>,
}

/// Whether the webhook that `slot` verifies reads its delivery id from the header the secret
/// checks, so the id would carry the credential. Install and receiver both refuse it.
pub(crate) fn delivery_clash(
    manifest: &aruna_plugins::manifest::Manifest,
    slot: &str,
    settings: &SecretSettings,
) -> bool {
    let hook = manifest.triggers.get(&TriggerKind::Webhook);
    let hook = hook.filter(|hook| hook.verify.as_deref() == Some(slot));
    let id = hook.and_then(|hook| hook.delivery_id.as_deref());
    (id.zip(settings.get("header"))).is_some_and(|(id, header)| id.eq_ignore_ascii_case(header))
}

/// Why slots could not be bound.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BindError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Unavailable(String),
}

impl From<IoError> for BindError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

/// A node binding check that did not answer yes.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0:?}")]
pub struct NodeCheckFailure(pub NodeCheckError);

impl From<IoError> for NodeCheckFailure {
    fn from(_: IoError) -> Self {
        Self(NodeCheckError::Unavailable)
    }
}

/// The slot a binding check is waiting for.
#[derive(Debug, PartialEq)]
struct PendingSlot {
    slot: String,
    binding: SecretBinding,
    kind: SecretKind,
}

/// The bindings of every slot the manifest declares, from `requested` (slot to `scope:name`) or
/// the same name in the group, each checked where it lives; and the warnings for slots whose use
/// rule a realm admin must add.
#[derive(Debug, PartialEq)]
pub(crate) struct BindSlots {
    config: Arc<RealmConfigDocument>,
    place: BindPlace,
    manifest: Manifest,
    requested: BTreeMap<String, String>,
    slots: VecDeque<String>,
    pending: Option<PendingSlot>,
    bindings: BTreeMap<String, SecretBinding>,
    warnings: Vec<String>,
}

/// The slots bound and what the admin should know about them.
pub(crate) type BoundSlots = (BTreeMap<String, SecretBinding>, Vec<String>);

impl BindSlots {
    pub(crate) fn new(
        config: Arc<RealmConfigDocument>,
        place: BindPlace,
        manifest: &Manifest,
        requested: &BTreeMap<String, String>,
    ) -> Self {
        Self {
            config,
            place,
            manifest: manifest.clone(),
            requested: requested.clone(),
            slots: manifest.secrets.keys().cloned().collect(),
            pending: None,
            bindings: BTreeMap::new(),
            warnings: Vec::new(),
        }
    }

    fn missing(pending: &PendingSlot) -> BindError {
        let shown = pending.kind.as_str();
        let (binding, slot) = (&pending.binding, &pending.slot);
        BindError::Invalid(format!("no {shown} secret `{binding}` for `{slot}`"))
    }

    fn warn_rule(&mut self, pending: &PendingSlot) {
        let (binding, slot) = (&pending.binding, &pending.slot);
        self.warnings.push(format!(
            "a realm admin adds the use rule of `{binding}` for `{slot}`"
        ));
    }

    /// Records a checked slot; settings that clash with the webhook refuse it.
    fn bound(
        &mut self,
        pending: PendingSlot,
        settings: Option<SecretSettings>,
    ) -> Result<(), BindError> {
        let clash = |settings| delivery_clash(&self.manifest, &pending.slot, settings);
        if settings.as_ref().is_some_and(clash) {
            return Err(BindError::Invalid(format!(
                "the webhook's delivery id is the header `{}` checks",
                pending.binding
            )));
        }
        self.bindings.insert(pending.slot, pending.binding);
        Ok(())
    }

    fn advance(&mut self) -> Result<Next<BoundSlots>, BindError> {
        while let Some(slot) = self.slots.pop_front() {
            let text =
                (self.requested.get(&slot).cloned()).unwrap_or_else(|| format!("group:{slot}"));
            let invalid =
                || BindError::Invalid(format!("`{text}` is not a secret of slot `{slot}`"));
            let declared = self.manifest.secrets.get(&slot).ok_or_else(invalid)?;
            let binding = SecretBinding::parse(&text).ok_or_else(invalid)?;
            let kind = vault_kind(declared.kind).ok_or_else(invalid)?;
            let pending = PendingSlot {
                slot,
                binding,
                kind,
            };
            let name = pending.binding.name.clone();
            // A node secret's settings live on the receiver, which checks them on every delivery.
            let call = match pending.binding.scope {
                BoundScope::Group => {
                    let owner = SecretOwner::Group(self.place.group_id);
                    IoCall::child(ReadEntry::new(self.config.realm_id, &owner, &name))
                }
                BoundScope::Node => {
                    let (Some(local), Some(receiver)) = (self.place.local, self.place.receiver)
                    else {
                        let unknown = "the receiver is not known yet".to_string();
                        return Err(BindError::Unavailable(unknown));
                    };
                    let check = CheckNodeBinding::new((local, receiver), &name, &pending.kind);
                    IoCall::child(check)
                }
                BoundScope::Realm => {
                    let record = (self.config.secrets.iter())
                        .find(|record| record.name == name)
                        .filter(|record| record.kind == pending.kind)
                        .ok_or_else(|| Self::missing(&pending))?;
                    let settings = record.settings.clone();
                    self.warn_rule(&pending);
                    self.bound(pending, Some(settings))?;
                    continue;
                }
            };
            self.pending = Some(pending);
            return Ok(Next::Call(call));
        }
        let bindings = std::mem::take(&mut self.bindings);
        Ok(Next::Done((bindings, std::mem::take(&mut self.warnings))))
    }
}

fn entry_settings(entry: &SecretEntry) -> Option<SecretSettings> {
    match &entry.info.as_ref()?.body {
        SecretBody::Info(info) => Some(info.settings.clone()),
        _ => None,
    }
}

impl Operation for BindSlots {
    type Output = BoundSlots;
    type Error = BindError;

    fn first(&mut self) -> Result<Next<BoundSlots>, BindError> {
        let unknown =
            (self.requested.keys()).find(|slot| !self.manifest.secrets.contains_key(*slot));
        if let Some(slot) = unknown {
            return Err(BindError::Invalid(format!(
                "the manifest has no secret slot `{slot}`"
            )));
        }
        self.advance()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BoundSlots>, BindError> {
        let pending = self.pending.take().ok_or(IoError::Unfinished)?;
        let settings = match pending.binding.scope {
            BoundScope::Node => {
                match result.child::<Option<String>, NodeCheckFailure>()? {
                    Ok(_) => {}
                    Err(NodeCheckFailure(NodeCheckError::Unavailable)) => {
                        let silent = "the receiver did not answer".to_string();
                        return Err(BindError::Unavailable(silent));
                    }
                    Err(_) => return Err(Self::missing(&pending)),
                }
                self.warn_rule(&pending);
                None
            }
            _ => {
                let entry = result.child::<Option<SecretEntry>, EntryError>()?;
                let entry = entry.map_err(|error| BindError::Unavailable(error.to_string()))?;
                let entry = entry.filter(|entry| entry.kind() == Some(&pending.kind));
                if entry.as_ref().is_none_or(|entry| entry.info.is_none()) {
                    return Err(Self::missing(&pending));
                }
                entry.as_ref().and_then(entry_settings)
            }
        };
        self.bound(pending, settings)?;
        self.advance()
    }
}

/// Whether this node holds the node secret `name` with `kind`; answers the header its `header`
/// setting names, in lowercase. Never the value.
#[derive(Debug, PartialEq)]
pub(crate) struct NodeSecretFits {
    name: SecretName,
    kind: SecretKind,
}

impl NodeSecretFits {
    pub(crate) fn new(name: &SecretName, kind: &SecretKind) -> Self {
        Self {
            name: name.clone(),
            kind: kind.clone(),
        }
    }
}

impl Operation for NodeSecretFits {
    type Output = Option<String>;
    type Error = NodeCheckFailure;

    fn first(&mut self) -> Result<Next<Option<String>>, NodeCheckFailure> {
        let row = (NODE_SECRETS_KEYSPACE, record_key(&self.name));
        Ok(Next::Call(read_call(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<String>>, NodeCheckFailure> {
        let unavailable = NodeCheckFailure(NodeCheckError::Unavailable);
        let [row] = values::<1>(result).map_err(|_| unavailable)?;
        let record = row.and_then(|bytes| postcard::from_bytes::<SecretRecord>(&bytes).ok());
        match record {
            None => Err(NodeCheckFailure(NodeCheckError::Missing)),
            Some(record) if record.kind != self.kind => {
                Err(NodeCheckFailure(NodeCheckError::OtherKind))
            }
            Some(record) => Ok(Next::Done(
                record
                    .settings
                    .get("header")
                    .map(|h| h.to_ascii_lowercase()),
            )),
        }
    }
}

/// Checks a node binding where it lives: on `receiver`, here or over the metadata transport;
/// answers the secret's checked header as [`NodeSecretFits`] does.
#[derive(Debug, PartialEq)]
pub(crate) struct CheckNodeBinding {
    local: NodeId,
    receiver: NodeId,
    name: SecretName,
    kind: SecretKind,
}

impl CheckNodeBinding {
    pub(crate) fn new(
        (local, receiver): (NodeId, NodeId),
        name: &SecretName,
        kind: &SecretKind,
    ) -> Self {
        Self {
            local,
            receiver,
            name: name.clone(),
            kind: kind.clone(),
        }
    }
}

impl Operation for CheckNodeBinding {
    type Output = Option<String>;
    type Error = NodeCheckFailure;

    fn first(&mut self) -> Result<Next<Option<String>>, NodeCheckFailure> {
        if self.receiver == self.local {
            let fits = NodeSecretFits::new(&self.name, &self.kind);
            return Ok(Next::Call(IoCall::child(fits)));
        }
        let message = MetadataTransportMessage::CheckNodeSecret {
            name: self.name.clone(),
            kind: self.kind.clone(),
        };
        let call = ask_peer(self.receiver, &message);
        Ok(Next::Call(
            call.ok_or(NodeCheckFailure(NodeCheckError::Unavailable))?,
        ))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<String>>, NodeCheckFailure> {
        if self.receiver == self.local {
            return Ok(Next::Done(
                result.child::<Option<String>, NodeCheckFailure>()??,
            ));
        }
        match peer_answer(self.receiver, result)? {
            Some(MetadataTransportMessage::NodeSecretChecked { result }) => {
                Ok(Next::Done(result.map_err(NodeCheckFailure)?))
            }
            _ => Err(NodeCheckFailure(NodeCheckError::Unavailable)),
        }
    }
}

/// The receiver side of a node binding check, for a management node that installs or a server
/// node that relays a webhook.
#[derive(Debug, PartialEq)]
pub(crate) struct ServeNodeCheck {
    node: ServingNode,
    peer: NodeId,
    name: SecretName,
    kind: SecretKind,
    checked: bool,
}

impl ServeNodeCheck {
    pub(crate) fn new(node: ServingNode, peer: NodeId, name: SecretName, kind: SecretKind) -> Self {
        Self {
            node,
            peer,
            name,
            kind,
            checked: false,
        }
    }
}

impl Operation for ServeNodeCheck {
    type Output = Option<String>;
    type Error = NodeCheckFailure;

    fn first(&mut self) -> Result<Next<Option<String>>, NodeCheckFailure> {
        let read = GetConfigOperation::new(self.node.0);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<String>>, NodeCheckFailure> {
        if self.checked {
            return Ok(Next::Done(
                result.child::<Option<String>, NodeCheckFailure>()??,
            ));
        }
        let config = result.child::<RealmConfigDocument, GetConfigError>()?;
        let config = config.map_err(|_| NodeCheckFailure(NodeCheckError::Unavailable))?;
        let server = node_kind(&config, self.peer)
            .is_some_and(|kind| !matches!(kind, RealmNodeKind::User { .. }));
        if !server {
            return Err(NodeCheckFailure(NodeCheckError::Forbidden));
        }
        self.checked = true;
        let fits = NodeSecretFits::new(&self.name, &self.kind);
        Ok(Next::Call(IoCall::child(fits)))
    }
}

#[cfg(test)]
#[path = "bound_tests.rs"]
pub(crate) mod tests;
