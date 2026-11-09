//! Replicated user and group secrets: their lifecycle, info and value writes, which writers
//! count, and how the current entry of one name is selected.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::UserId;
use crate::admin_documents::{AdminDocumentClock, NodeInterval};
use crate::errors::ConversionError;
use crate::register::{RegisterStamp, current_write};
use crate::secrets::{
    MAX_DESCRIPTION, MAX_VALUE_BYTES, SecretKind, SecretName, SecretScope, SecretSettings, UseRule,
};
use crate::types::GroupId;

/// Sealed bytes a value may take: the encoded fields plus the HPKE key and tag.
pub const MAX_SEALED: usize = MAX_VALUE_BYTES + 64;

/// Who owns a replicated secret.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecretOwner {
    User(UserId),
    Group(GroupId),
}

impl SecretOwner {
    /// Tagged bytes, so a user and a group with the same ULID never share a key or seal.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::User(user) => [&[1u8][..], &user.to_bytes()].concat(),
            Self::Group(group) => [&[2u8][..], &group.to_bytes()].concat(),
        }
    }

    pub fn scope(self) -> SecretScope {
        match self {
            Self::User(user) => SecretScope::User(user),
            Self::Group(group) => SecretScope::Group(group),
        }
    }
}

/// The three registers of one secret.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecretPart {
    Lifecycle,
    Info,
    Value,
}

impl SecretPart {
    pub const fn tag(self) -> u8 {
        match self {
            Self::Lifecycle => 1,
            Self::Info => 2,
            Self::Value => 3,
        }
    }
}

/// A create, which fixes the kind and whether the owner may read the value back, or a delete.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecretLifecycle {
    Created { kind: SecretKind, readable: bool },
    Deleted,
}

/// The description, settings and use rules of one incarnation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretInfo {
    pub description: String,
    pub settings: SecretSettings,
    pub uses: Vec<UseRule>,
}

/// The sealed fields of one incarnation and their names.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretValue {
    pub fields: Vec<String>,
    pub sealed: Vec<u8>,
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretValue")
            .field("fields", &self.fields)
            .field("sealed_len", &self.sealed.len())
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecretBody {
    Lifecycle(SecretLifecycle),
    Info(SecretInfo),
    Value(SecretValue),
}

/// One immutable register write. A create's incarnation is its own write id; every info and
/// value write names the create it belongs to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretDocument {
    pub owner: SecretOwner,
    pub name: SecretName,
    pub stamp: RegisterStamp,
    pub incarnation: Ulid,
    /// The writer's realm config clock, which decides whether its writer counts.
    pub config_clock: AdminDocumentClock,
    pub author: UserId,
    pub written_at_ms: u64,
    pub body: SecretBody,
}

impl SecretDocument {
    pub fn part(&self) -> SecretPart {
        match self.body {
            SecretBody::Lifecycle(_) => SecretPart::Lifecycle,
            SecretBody::Info(_) => SecretPart::Info,
            SecretBody::Value(_) => SecretPart::Value,
        }
    }

    pub fn key(&self) -> Vec<u8> {
        document_key(&self.owner, &self.name, self.part(), self.stamp.write_id)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// The structural checks sync admission and every reader share; rules are checked
    /// against the kind when a write is made.
    pub fn check(&self) -> Result<(), &'static str> {
        if self.stamp.counter == 0 {
            return Err("a register counter starts at 1");
        }
        let creates = matches!(
            self.body,
            SecretBody::Lifecycle(SecretLifecycle::Created { .. })
        );
        if creates != (self.incarnation == self.stamp.write_id) {
            return Err("only a create names itself as its incarnation");
        }
        match &self.body {
            SecretBody::Lifecycle(SecretLifecycle::Created { readable: true, .. })
                if !matches!(self.owner, SecretOwner::User(_)) =>
            {
                Err("only user secrets can be readable")
            }
            SecretBody::Info(info) if info.description.chars().count() > MAX_DESCRIPTION => {
                Err("the description is too long")
            }
            SecretBody::Value(value) if value.sealed.len() > MAX_SEALED => {
                Err("the sealed value is too large")
            }
            _ => Ok(()),
        }
    }
}

/// Row key: owner, name, part and write id, so a prefix lists an owner or one name.
pub fn document_key(owner: &SecretOwner, name: &SecretName, part: SecretPart, id: Ulid) -> Vec<u8> {
    [
        entry_prefix(owner, name).as_slice(),
        &[part.tag()],
        &id.to_bytes(),
    ]
    .concat()
}

pub fn owner_prefix(owner: &SecretOwner) -> Vec<u8> {
    owner.to_bytes()
}

pub fn entry_prefix(owner: &SecretOwner, name: &SecretName) -> Vec<u8> {
    let name = name.as_str().as_bytes();
    // Names are at most 64 bytes, so one length byte keeps two names from sharing a prefix.
    let length = u8::try_from(name.len()).unwrap_or(u8::MAX);
    [owner.to_bytes().as_slice(), &[length], name].concat()
}

/// Whether the writer of a document counts on this node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Eligibility {
    Counts,
    /// The cited clock shows no infrastructure membership of the writer; never counts.
    Ignored,
    /// The local realm config does not cover the cited clock yet; counts may change later.
    Deferred,
}

/// Decides from the writer's cited realm config clock and the retained node history.
pub fn writer_eligibility(
    document: &SecretDocument,
    intervals: &[NodeInterval],
    local: &AdminDocumentClock,
) -> Eligibility {
    if !local.covers(&document.config_clock) {
        return Eligibility::Deferred;
    }
    let member = (intervals.iter()).any(|interval| {
        interval.node_id == document.stamp.node_id && interval.open_at(&document.config_clock)
    });
    match member {
        true => Eligibility::Counts,
        false => Eligibility::Ignored,
    }
}

/// The current state of one name: the winning create and, within its incarnation, the
/// winning info and value writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretEntry {
    pub lifecycle: SecretDocument,
    pub info: Option<SecretDocument>,
    pub value: Option<SecretDocument>,
}

impl SecretEntry {
    pub fn kind(&self) -> Option<&SecretKind> {
        match &self.lifecycle.body {
            SecretBody::Lifecycle(SecretLifecycle::Created { kind, .. }) => Some(kind),
            _ => None,
        }
    }

    pub fn readable(&self) -> bool {
        matches!(
            self.lifecycle.body,
            SecretBody::Lifecycle(SecretLifecycle::Created { readable: true, .. })
        )
    }

    /// A create whose info or value has not arrived yet; it refuses every use.
    pub fn complete(&self) -> bool {
        self.info.is_some() && self.value.is_some()
    }

    /// The opaque token `If-Match` compares: the three selected write ids.
    pub fn revision(&self) -> String {
        let id = |document: &Option<SecretDocument>| {
            (document.as_ref()).map_or("-".to_string(), |document| {
                document.stamp.write_id.to_string()
            })
        };
        format!(
            "{}.{}.{}",
            self.lifecycle.stamp.write_id,
            id(&self.info),
            id(&self.value)
        )
    }
}

/// Selects the lifecycle first, then info and value only within its incarnation, so a stale
/// edit of a deleted incarnation never brings it back. `counts` filters the writers.
pub fn current_entry<'a>(
    documents: impl IntoIterator<Item = &'a SecretDocument>,
    counts: impl Fn(&SecretDocument) -> bool,
) -> Option<SecretEntry> {
    let documents: Vec<&SecretDocument> = (documents.into_iter())
        .filter(|document| document.check().is_ok() && counts(document))
        .collect();
    let pick = |part: SecretPart, incarnation: Option<Ulid>| {
        let writes = (documents.iter().copied()).filter(|document| {
            document.part() == part && incarnation.is_none_or(|id| document.incarnation == id)
        });
        current_write(writes, |document| document.stamp).cloned()
    };
    let lifecycle = pick(SecretPart::Lifecycle, None)?;
    if matches!(
        lifecycle.body,
        SecretBody::Lifecycle(SecretLifecycle::Deleted)
    ) {
        return None;
    }
    let incarnation = Some(lifecycle.stamp.write_id);
    Some(SecretEntry {
        info: pick(SecretPart::Info, incarnation),
        value: pick(SecretPart::Value, incarnation),
        lifecycle,
    })
}

#[cfg(test)]
#[path = "registers_tests.rs"]
mod tests;
