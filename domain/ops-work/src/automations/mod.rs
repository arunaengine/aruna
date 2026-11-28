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
    active_prefix, firing_key, group_auto_key, group_prefix, record_key, record_prefix,
    schedule_cursor_key, schedule_due_key, schedule_due_prefix,
};
use aruna_core::automation::record::{Automation, Lifecycle, Spec};
use aruna_core::automation::schedule::{CursorMode, ScheduleCursor};
use aruna_core::automation::usage::Window;
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
use aruna_ops_identity::document_repository::GetConfigError;
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
    #[error("{reason}")]
    Limited { reason: String, retry_after: u64 },
    #[error("{0}")]
    Unavailable(String),
    #[error("storage failed: {0}")]
    Storage(String),
}

impl AutoError {
    pub fn limited(reason: impl Into<String>, retry_after: u64) -> Self {
        Self::Limited {
            reason: reason.into(),
            retry_after: retry_after.max(1),
        }
    }
}

pub(crate) const ACTIVE_RETRY_SECONDS: u64 = 30;
pub(crate) const APPLET_RETRY_SECONDS: u64 = 60;

pub(crate) fn pinned_retry_after(window: Window, pinned_ms: u64, now_ms: u64) -> u64 {
    let span = window.span_ms();
    let expiry_ms = u64::from(window.start(pinned_ms))
        .saturating_add(1)
        .saturating_mul(span);
    expiry_ms.saturating_sub(now_ms).div_ceil(1_000).max(1)
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

pub(crate) fn group_auto_row(auto: &Automation) -> (KeySpace, Key, Value) {
    (
        AUTOMATIONS_KEYSPACE.to_string(),
        group_auto_key(auto.group_id, auto.automation_id),
        ByteView::from(Vec::new()),
    )
}

pub(crate) fn auto_rows(auto: &Automation) -> Result<Vec<(KeySpace, Key, Value)>, AutoError> {
    let mut rows = vec![crate::automation_row(auto)?];
    if auto.lifecycle != Lifecycle::Removed {
        rows.push(group_auto_row(auto));
    }
    Ok(rows)
}

pub(crate) fn group_autos_page(group_id: GroupId, after: Option<Key>) -> IoCall {
    IoCall::Page {
        space: AUTOMATIONS_KEYSPACE.to_string(),
        prefix: group_prefix(group_id),
        after,
        limit: 256,
    }
}

pub(crate) fn schedule_due_page(after: Option<Key>) -> IoCall {
    IoCall::Page {
        space: AUTOMATIONS_KEYSPACE.to_string(),
        prefix: schedule_due_prefix(),
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

/// Decodes a `GetConfigOperation` result and verifies that every listed feature is enabled.
pub(crate) fn check_features(
    result: IoResult,
    disabled: &BTreeSet<Feature>,
    group_id: GroupId,
    features: &[Feature],
) -> Result<(), AutoError> {
    let config = result
        .child::<RealmConfigDocument, GetConfigError>()?
        .map_err(|_| AutoError::Unavailable("the realm feature policy cannot be read".into()))?;
    for &feature in features {
        check_feature(&config, disabled, feature, group_id)?;
    }
    Ok(())
}

pub(crate) fn schedule_due_row(
    due_ms: u64,
    automation_id: Ulid,
    group_id: GroupId,
) -> (KeySpace, Key, Value) {
    (
        AUTOMATIONS_KEYSPACE.to_string(),
        schedule_due_key(due_ms, automation_id),
        ByteView::from(group_id.to_bytes().to_vec()),
    )
}

pub(crate) fn schedule_cursor_row(
    automation_id: Ulid,
    cursor: &ScheduleCursor,
) -> Result<(KeySpace, Key, Value), AutoError> {
    Ok((
        AUTOMATIONS_KEYSPACE.to_string(),
        schedule_cursor_key(automation_id),
        ByteView::from(cursor.to_bytes()?),
    ))
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ScheduleSync {
    pub writes: Vec<(KeySpace, Key, Value)>,
    pub deletes: Vec<(KeySpace, Key)>,
}

/// Computes the due-index and cursor row writes/deletes needed after mutating `auto`.
pub(crate) fn plan_schedule_sync(
    auto: &Automation,
    previous: Option<&ScheduleCursor>,
    mode: CursorMode,
    now_ms: u64,
) -> Result<ScheduleSync, AutoError> {
    let next = if auto.lifecycle == Lifecycle::Active {
        ScheduleCursor::for_spec(&auto.spec, previous, mode, now_ms)
    } else {
        None
    };
    plan_cursor_transition(auto.automation_id, auto.group_id, previous, next.as_ref())
}

/// Deletes the due-index and cursor rows for a schedule cursor when no next slot exists.
pub(crate) fn clear_schedule_rows(automation_id: Ulid, previous: &ScheduleCursor) -> ScheduleSync {
    ScheduleSync {
        writes: Vec::new(),
        deletes: vec![
            (
                AUTOMATIONS_KEYSPACE.to_string(),
                schedule_due_key(previous.due_ms, automation_id),
            ),
            (
                AUTOMATIONS_KEYSPACE.to_string(),
                schedule_cursor_key(automation_id),
            ),
        ],
    }
}

/// Computes the due-index and cursor row writes/deletes to move from `previous` to `next`.
pub(crate) fn plan_cursor_transition(
    automation_id: Ulid,
    group_id: GroupId,
    previous: Option<&ScheduleCursor>,
    next: Option<&ScheduleCursor>,
) -> Result<ScheduleSync, AutoError> {
    let mut sync = ScheduleSync::default();
    if let Some(old) = previous
        && next.map(|cursor| cursor.due_ms) != Some(old.due_ms)
    {
        sync.deletes.push((
            AUTOMATIONS_KEYSPACE.to_string(),
            schedule_due_key(old.due_ms, automation_id),
        ));
    }
    match next {
        Some(cursor) => {
            if previous != Some(cursor) {
                sync.writes
                    .push(schedule_cursor_row(automation_id, cursor)?);
            }
            if previous.map(|old| old.due_ms) != Some(cursor.due_ms) {
                sync.writes
                    .push(schedule_due_row(cursor.due_ms, automation_id, group_id));
            }
        }
        None => {
            if previous.is_some() {
                sync.deletes.push((
                    AUTOMATIONS_KEYSPACE.to_string(),
                    schedule_cursor_key(automation_id),
                ));
            }
        }
    }
    Ok(sync)
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
