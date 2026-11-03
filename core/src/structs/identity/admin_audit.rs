//! Defines the node-local administrative audit rows and their realm and group index keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::UserId;
use crate::errors::ConversionError;
use crate::keyspaces::ADMIN_AUDIT_KEYSPACE;
use crate::types::GroupId;

const REALM_SCOPE: u8 = b'r';
const GROUP_SCOPE: u8 = b'g';

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminAuditAction {
    AccountDeactivated,
    AccountReactivated,
    ServiceAccountCreated,
    RestrictedTokenIssued,
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
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// The realm row, and a group row when a group owns the target, written together.
    pub fn entries(&self) -> Result<Vec<(String, ByteView, ByteView)>, ConversionError> {
        let value = ByteView::from(self.to_bytes()?);
        let mut keys = vec![audit_key(None, self.audit_id)];
        if let Some(group_id) = self.group_id {
            keys.push(audit_key(Some(group_id), self.audit_id));
        }
        Ok(keys
            .into_iter()
            .map(|key| (ADMIN_AUDIT_KEYSPACE.to_string(), key.into(), value.clone()))
            .collect())
    }
}

/// The key prefix of the realm rows, or of one group's rows.
pub fn audit_prefix(group_id: Option<GroupId>) -> Vec<u8> {
    match group_id {
        None => vec![REALM_SCOPE],
        Some(group_id) => {
            let mut prefix = vec![GROUP_SCOPE];
            prefix.extend_from_slice(&group_id.to_bytes());
            prefix
        }
    }
}

pub fn audit_key(group_id: Option<GroupId>, audit_id: Ulid) -> Vec<u8> {
    let mut key = audit_prefix(group_id);
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
        assert!(entries[0].1.starts_with(&audit_prefix(None)));
        assert!(entries[1].1.starts_with(&audit_prefix(Some(group))));
        let other = audit_prefix(Some(Ulid::from_parts(3, 3)));
        assert!(!entries[1].1.starts_with(&other));
        assert_eq!(AdminAuditRecord::from_bytes(&entries[1].2).unwrap(), record);
    }
}
