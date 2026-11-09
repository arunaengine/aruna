//! Changes named secrets: the rules for create, replace, rotate and delete that every scope shares.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::secrets::{
    SecretError, SecretFields, SecretKind, SecretName, SecretRecord, SecretScope, SecretSettings,
    UseRule,
};
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord, SecretAudit};
use thiserror::Error;
use ulid::Ulid;

/// One change a manager asks for. `expected` is an `If-Match` revision token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretChange {
    Create {
        name: SecretName,
        kind: SecretKind,
        description: String,
        settings: SecretSettings,
        uses: Vec<UseRule>,
        fields: SecretFields,
    },
    /// Replaces description, settings and rules; the value stays.
    Replace {
        name: SecretName,
        description: String,
        settings: SecretSettings,
        uses: Vec<UseRule>,
        expected: Option<String>,
    },
    /// Sets a new value; the record stays.
    Rotate {
        name: SecretName,
        fields: SecretFields,
        expected: Option<String>,
    },
    Delete {
        name: SecretName,
        expected: Option<String>,
    },
}

impl SecretChange {
    pub fn name(&self) -> &SecretName {
        match self {
            Self::Create { name, .. }
            | Self::Replace { name, .. }
            | Self::Rotate { name, .. }
            | Self::Delete { name, .. } => name,
        }
    }

    fn expected(&self) -> Option<&str> {
        match self {
            Self::Create { .. } => None,
            Self::Replace { expected, .. }
            | Self::Rotate { expected, .. }
            | Self::Delete { expected, .. } => expected.as_deref(),
        }
    }
}

/// What a change writes: the new record (none after a delete), a value to seal under its value
/// id, the value it retires, and the audit row that commits with it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Applied {
    pub info: Option<SecretRecord>,
    pub value: Option<SecretFields>,
    pub retired: Option<Ulid>,
    pub audit: AdminAuditRecord,
}

/// Why a change was refused. Never carries a value.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ChangeError {
    #[error("a secret with this name exists")]
    Exists,
    #[error("no secret with this name exists")]
    NotFound,
    #[error("the secret changed since the given revision")]
    Stale,
    #[error(transparent)]
    Invalid(#[from] SecretError),
}

/// Applies `change` to the current record, as this node sees it. `write_id` names the write:
/// the new record revision and, when the value changes, the new value id.
pub fn apply_change(
    scope: SecretScope,
    current: Option<&SecretRecord>,
    change: SecretChange,
    actor: UserId,
    now_ms: u64,
    write_id: Ulid,
) -> Result<Applied, ChangeError> {
    if let (Some(current), Some(expected)) = (current, change.expected())
        && current.revision_token() != expected
    {
        return Err(ChangeError::Stale);
    }
    let name = change.name().clone();
    let (action, info, value, retired) = match (change, current) {
        (SecretChange::Create { .. }, Some(_)) => return Err(ChangeError::Exists),
        (
            SecretChange::Create {
                name,
                kind,
                description,
                settings,
                uses,
                fields,
            },
            None,
        ) => {
            fields.check(&kind)?;
            let info = SecretRecord {
                name,
                kind,
                description,
                settings,
                uses,
                fields: fields.names().map(str::to_string).collect(),
                revision: write_id,
                value_id: write_id,
                created_by: actor,
                created_at_ms: now_ms,
                updated_by: actor,
                updated_at_ms: now_ms,
            };
            (
                AdminAuditAction::SecretCreated,
                Some(info),
                Some(fields),
                None,
            )
        }
        (_, None) => return Err(ChangeError::NotFound),
        (
            SecretChange::Replace {
                description,
                settings,
                uses,
                ..
            },
            Some(current),
        ) => {
            let info = SecretRecord {
                description,
                settings,
                uses,
                ..touched(current, actor, now_ms, write_id)
            };
            (AdminAuditAction::SecretReplaced, Some(info), None, None)
        }
        (SecretChange::Rotate { fields, .. }, Some(current)) => {
            fields.check(&current.kind)?;
            let info = SecretRecord {
                fields: fields.names().map(str::to_string).collect(),
                value_id: write_id,
                ..touched(current, actor, now_ms, write_id)
            };
            let retired = Some(current.value_id);
            (
                AdminAuditAction::SecretRotated,
                Some(info),
                Some(fields),
                retired,
            )
        }
        (SecretChange::Delete { .. }, Some(current)) => (
            AdminAuditAction::SecretDeleted,
            None,
            None,
            Some(current.value_id),
        ),
    };
    if let Some(info) = &info {
        info.check(scope)?;
    }
    let audit = AdminAuditRecord::for_secret(
        action,
        actor,
        SecretAudit {
            scope,
            name,
            incarnation: None,
            write_id: Some(write_id),
        },
    );
    Ok(Applied {
        info,
        value,
        retired,
        audit,
    })
}

fn touched(current: &SecretRecord, actor: UserId, now_ms: u64, write_id: Ulid) -> SecretRecord {
    SecretRecord {
        revision: write_id,
        updated_by: actor,
        updated_at_ms: now_ms,
        ..current.clone()
    }
}

#[cfg(test)]
#[path = "change_tests.rs"]
mod tests;
