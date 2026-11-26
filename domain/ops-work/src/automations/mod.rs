//! Workflow automations on one node: records, access checks, limits, identity and runtime.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0


pub mod change;
pub mod check;
pub mod diff;
pub mod firing_reads;
pub mod identity;
pub mod lifecycle;
pub mod limits;
pub mod reads;
pub mod upkeep;

use aruna_core::automation::keys::{active_prefix, firing_key, record_key, record_prefix};
use aruna_core::automation::record::{Automation, Lifecycle};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::request_policy::PolicyRequestExtras;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::types::{Key, KeySpace, Value};
use aruna_ops_identity::auth::error::AuthorizeError;
use aruna_ops_identity::auth::request_authorization::AuthorizeOperation;
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Error, PartialEq)]
pub enum AutoError {
    #[error("automation not found")]
    NotFound,
    #[error("permission denied")]
    Forbidden,
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Limited(String),
    #[error("storage failed: {0}")]
    Storage(String),
}

impl From<IoError> for AutoError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.to_string())
    }
}

impl From<ConversionError> for AutoError {
    fn from(error: ConversionError) -> Self {
        Self::Storage(error.to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Need {
    Read,
    Write,
}

pub(crate) fn authorize_path(auth: &AuthContext, path: &str, permission: Permission) -> IoCall {
    let check = AuthorizeOperation::new(
        auth.realm_id,
        auth,
        path,
        &permission,
        PolicyRequestExtras::rest(),
    );
    IoCall::child(check)
}

pub(crate) fn allowed(result: IoResult) -> Result<bool, AutoError> {
    match result.child::<(), AuthorizeError>()? {
        Ok(()) => Ok(true),
        Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => Ok(false),
        Err(error) => Err(AutoError::Storage(error.to_string())),
    }
}

pub(crate) fn read_auto(id: Ulid) -> IoCall {
    IoCall::Read(vec![(AUTOMATIONS_KEYSPACE.to_string(), record_key(id))])
}

pub(crate) fn autos_page(after: Option<Key>) -> IoCall {
    IoCall::Page {
        space: AUTOMATIONS_KEYSPACE.to_string(),
        prefix: record_prefix(),
        after,
        limit: 256,
    }
}

pub(crate) fn active_page(after: Option<Key>) -> IoCall {
    IoCall::Page {
        space: AUTOMATION_RUNS_KEYSPACE.to_string(),
        prefix: active_prefix(),
        after,
        limit: 256,
    }
}

pub(crate) fn group_read(
    auth: &AuthContext,
    group_id: aruna_core::types::GroupId,
    local: NodeId,
) -> IoCall {
    let path = group_permission_path(auth.realm_id, group_id, local);
    authorize_path(auth, &path, Permission::READ)
}

pub(crate) fn auto_answer(result: IoResult) -> Result<Option<Automation>, AutoError> {
    Ok(result
        .value()?
        .map(|bytes| Automation::from_bytes(&bytes))
        .transpose()?)
}

pub(crate) fn auto_row(auto: &Automation) -> Result<(KeySpace, Key, Value), AutoError> {
    Ok((
        AUTOMATIONS_KEYSPACE.to_string(),
        record_key(auto.automation_id),
        ByteView::from(auto.to_bytes()?),
    ))
}

pub(crate) fn firing_row(firing: &Firing) -> Result<(KeySpace, Key, Value), AutoError> {
    Ok((
        AUTOMATION_RUNS_KEYSPACE.to_string(),
        firing_key(firing.automation_id, firing.firing_id),
        ByteView::from(firing.to_bytes()?),
    ))
}

/// Auth context for an automation's service account restricted strictly to its role paths.
pub fn automation_auth(auto: &Automation, realm_id: RealmId) -> Option<AuthContext> {
    let account = auto.account.filter(|_| auto.role_id.is_some())?;
    let restrictions = auto
        .role_paths(realm_id, auto.host_node)
        .into_iter()
        .map(|(pattern, permission)| PathRestriction {
            pattern,
            permission,
        })
        .collect();
    Some(AuthContext {
        user_id: account,
        realm_id,
        path_restrictions: Some(restrictions),
        session: None,
    })
}

/// Reads an automation and checks the caller's group access (`Read` or `Write`).
#[derive(Debug, PartialEq)]
pub struct AutoAccessOperation {
    auth: AuthContext,
    id: Ulid,
    local: NodeId,
    need: Need,
    record: Option<Automation>,
}

impl AutoAccessOperation {
    pub fn new(auth: &AuthContext, id: Ulid, local: NodeId, need: Need) -> Self {
        Self {
            auth: auth.clone(),
            id,
            local,
            need,
            record: None,
        }
    }
}

impl Operation for AutoAccessOperation {
    type Output = Automation;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Automation>, AutoError> {
        Ok(Next::Call(read_auto(self.id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Automation>, AutoError> {
        let Some(record) = self.record.take() else {
            let record = auto_answer(result)?
                .filter(|item| item.lifecycle != Lifecycle::Removed)
                .ok_or(AutoError::NotFound)?;
            let perm = match self.need {
                Need::Read => Permission::READ,
                Need::Write => Permission::WRITE,
            };
            let path = group_permission_path(self.auth.realm_id, record.group_id, self.local);
            self.record = Some(record);
            return Ok(Next::Call(authorize_path(&self.auth, &path, perm)));
        };
        if !allowed(result)? {
            return Err(match self.need {
                Need::Read => AutoError::NotFound,
                Need::Write => AutoError::Forbidden,
            });
        }
        Ok(Next::Done(record))
    }

    fn expected_error(error: &AutoError) -> bool {
        matches!(error, AutoError::NotFound | AutoError::Forbidden)
    }
}
