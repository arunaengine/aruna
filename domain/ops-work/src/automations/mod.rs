//! Workflow automations on one node: records, access checks, limits, identity and runtime.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod admit;
pub mod applet_link;
pub mod change;
pub mod check;
mod commit_bundle;
pub mod control;
pub mod copy_stage;
#[cfg(test)]
mod copy_tests;
pub mod dataset_stage;
pub mod diff;
pub mod firing_reads;
pub mod firing_tx;
#[cfg(test)]
mod flow_tests;
#[cfg(test)]
mod guard_tests;
pub mod identity;
pub mod lifecycle;
pub mod limits;
#[cfg(test)]
mod match_tests;
pub mod matcher;
pub mod notify_stage;
pub mod reads;
pub mod run_stage;
pub mod runner;
#[cfg(test)]
mod runner_tests;
pub mod save;
mod schedule;
pub mod start;
pub mod timer;
pub mod upkeep;

use aruna_core::automation::firing::Firing;
use aruna_core::automation::keys::{
    active_prefix, firing_key, record_key, record_prefix, schedule_key, schedule_prefix,
};
use aruna_core::automation::record::{Automation, Lifecycle, Spec};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::request_policy::PolicyRequestExtras;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission};
use aruna_core::structs::identity::features::{Feature, Resolved, resolve};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::types::{GroupId, Key, KeySpace, Value};
use aruna_ops_identity::auth::error::AuthorizeError;
use aruna_ops_identity::auth::request_authorization::AuthorizeOperation;
use byteview::ByteView;
use std::collections::BTreeSet;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Error, PartialEq)]
pub enum AutoError {
    #[error("automation not found")]
    NotFound,
    #[error("permission denied")]
    Forbidden,
    #[error("feature `{}` is disabled", .0.name())]
    FeatureOff(Feature, Resolved),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Capacity(String),
    #[error("{0}")]
    Limited(String),
    #[error("{}", .0.reason.as_deref().unwrap_or("conditions were not met"))]
    ConditionsNotMet(Box<Firing>),
    #[error("{0}")]
    Unavailable(String),
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

pub(crate) fn schedule_page(after: Option<Key>) -> IoCall {
    IoCall::Page {
        space: AUTOMATIONS_KEYSPACE.to_string(),
        prefix: schedule_prefix(),
        after,
        limit: 256,
    }
}

pub(crate) fn check_feature(
    config: &RealmConfigDocument,
    disabled: &BTreeSet<Feature>,
    feature: Feature,
    group_id: GroupId,
) -> Result<(), AutoError> {
    let resolved = resolve(config, disabled, feature, Some(&group_id));
    match resolved.on {
        true => Ok(()),
        false => Err(AutoError::FeatureOff(feature, resolved)),
    }
}

pub(crate) fn schedule_row(automation_id: Ulid, ms: u64) -> (KeySpace, Key, Value) {
    (
        AUTOMATIONS_KEYSPACE.to_string(),
        schedule_key(automation_id),
        ByteView::from(ms.to_be_bytes().to_vec()),
    )
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
    automation_auth_for(auto, &auto.spec, realm_id)
}

/// Auth context for an automation's service account bounded to a pinned execution `spec`.
pub fn automation_auth_for(
    auto: &Automation,
    spec: &Spec,
    realm_id: RealmId,
) -> Option<AuthContext> {
    let account = auto.account.filter(|_| auto.role_id.is_some())?;
    let restrictions = auto
        .role_paths_for(spec, realm_id, auto.host_node)
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
