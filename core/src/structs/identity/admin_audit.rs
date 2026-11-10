//! Defines the node-local administrative audit rows and their realm, group and owner index keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::UserId;
use crate::errors::ConversionError;
use crate::keyspaces::ADMIN_AUDIT_KEYSPACE;
use crate::secrets::{SecretName, SecretScope};
use crate::types::GroupId;

const REALM_SCOPE: u8 = b'r';
const GROUP_SCOPE: u8 = b'g';
const USER_SCOPE: u8 = b'u';

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminAuditAction {
    AccountDeactivated,
    AccountReactivated,
    ServiceAccountCreated,
    RestrictedTokenIssued,
    SecretCreated,
    SecretReplaced,
    SecretRotated,
    SecretDeleted,
    /// The owner read back a readable user secret.
    SecretRead,
}

/// The secret a row is about; never its value or field values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretAudit {
    pub scope: SecretScope,
    pub name: SecretName,
    pub incarnation: Option<Ulid>,
    /// The write this change made, or the value write a read-back opened.
    pub write_id: Option<Ulid>,
}

/// One completed administrative change, as the node that made it recorded it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminAuditRecord {
    /// Time-ordered id; its timestamp is the moment the change was recorded.
    pub audit_id: Ulid,
    pub action: AdminAuditAction,
    pub actor: UserId,
    pub target: UserId,
    /// The group that owns the target, which lets that group's administrators read the row.
    pub group_id: Option<GroupId>,
    /// The session a restricted token belongs to; never the token itself.
    pub session_id: Option<String>,
    pub secret: Option<SecretAudit>,
}

impl AdminAuditRecord {
    pub fn new(action: AdminAuditAction, actor: UserId, target: UserId) -> Self {
        Self {
            audit_id: Ulid::generate(),
            action,
            actor,
            target,
            group_id: None,
            session_id: None,
            secret: None,
        }
    }

    /// A row about a secret: the owner is the target of a user secret, else the actor.
    pub fn for_secret(action: AdminAuditAction, actor: UserId, secret: SecretAudit) -> Self {
        let (target, group_id) = match secret.scope {
            SecretScope::User(owner) => (owner, None),
            SecretScope::Group(group_id) => (actor, Some(group_id)),
            SecretScope::Node | SecretScope::Realm => (actor, None),
        };
        Self {
            target,
            group_id,
            secret: Some(secret),
            ..Self::new(action, actor, target)
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// The realm row, a group row when a group owns the target, and an owner row for a user
    /// secret, written together.
    pub fn entries(&self) -> Result<Vec<(String, ByteView, ByteView)>, ConversionError> {
        let value = ByteView::from(self.to_bytes()?);
        let mut keys = vec![audit_key(AuditIndex::Realm, self.audit_id)];
        if let Some(group_id) = self.group_id {
            keys.push(audit_key(AuditIndex::Group(group_id), self.audit_id));
        }
        if let Some(SecretScope::User(owner)) = self.secret.as_ref().map(|secret| secret.scope) {
            keys.push(audit_key(AuditIndex::User(owner), self.audit_id));
        }
        Ok(keys
            .into_iter()
            .map(|key| (ADMIN_AUDIT_KEYSPACE.to_string(), key.into(), value.clone()))
            .collect())
    }
}

/// Which rows a page reads: every row, one group's rows, or the rows about one owner's secrets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditIndex {
    Realm,
    Group(GroupId),
    User(UserId),
}

pub fn audit_prefix(index: AuditIndex) -> Vec<u8> {
    match index {
        AuditIndex::Realm => vec![REALM_SCOPE],
        AuditIndex::Group(group_id) => [&[GROUP_SCOPE][..], &group_id.to_bytes()].concat(),
        AuditIndex::User(owner) => [&[USER_SCOPE][..], &owner.to_bytes()].concat(),
    }
}

pub fn audit_key(index: AuditIndex, audit_id: Ulid) -> Vec<u8> {
    let mut key = audit_prefix(index);
    key.extend_from_slice(&audit_id.to_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::realm::RealmId;

    #[test]
    fn group_rows_indexed() {
        let realm = RealmId::from_bytes([1; 32]);
        let actor = UserId::local(Ulid::from_parts(1, 1), realm);
        let mut record =
            AdminAuditRecord::new(AdminAuditAction::ServiceAccountCreated, actor, actor);
        assert_eq!(record.entries().unwrap().len(), 1);
        let group = Ulid::from_parts(2, 2);
        record.group_id = Some(group);
        let entries = record.entries().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries[0].1.starts_with(audit_prefix(AuditIndex::Realm)));
        assert!(
            entries[1]
                .1
                .starts_with(audit_prefix(AuditIndex::Group(group)))
        );
        let other = audit_prefix(AuditIndex::Group(Ulid::from_parts(3, 3)));
        assert!(!entries[1].1.starts_with(&other));
        assert_eq!(AdminAuditRecord::from_bytes(&entries[1].2).unwrap(), record);
    }

    #[test]
    fn owner_rows_indexed() {
        let realm = RealmId::from_bytes([1; 32]);
        let owner = UserId::local(Ulid::from_parts(1, 1), realm);
        let name = SecretName::try_from("catalog-key".to_string()).unwrap();
        let secret = |scope| SecretAudit {
            scope,
            name: name.clone(),
            incarnation: Some(Ulid::from_parts(4, 4)),
            write_id: None,
        };
        let record = AdminAuditRecord::for_secret(
            AdminAuditAction::SecretRead,
            owner,
            secret(SecretScope::User(owner)),
        );
        assert_eq!(record.target, owner);
        let entries = record.entries().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(
            entries[1]
                .1
                .starts_with(audit_prefix(AuditIndex::User(owner)))
        );
        // A user and a group with the same ULID never share an index.
        let group = AuditIndex::Group(owner.user_ulid);
        assert!(!entries[1].1.starts_with(audit_prefix(group)));
        let admin = UserId::local(Ulid::from_parts(2, 2), realm);
        let scope = SecretScope::Group(Ulid::from_parts(3, 3));
        let grouped =
            AdminAuditRecord::for_secret(AdminAuditAction::SecretCreated, admin, secret(scope));
        assert_eq!(
            (grouped.target, grouped.group_id),
            (admin, Some(Ulid::from_parts(3, 3)))
        );
        assert_eq!(grouped.entries().unwrap().len(), 2);
    }
}
