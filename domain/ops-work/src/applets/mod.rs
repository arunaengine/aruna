//! Applet records on their host node: creation, reads, approved changes and their storage rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod backends;
pub mod build;
pub mod change;
pub mod create;
pub mod domains;
pub mod draft;
pub mod feed;
pub mod identity;
pub mod intake;
pub mod reads;
pub mod release;
pub mod replica;
pub mod runs;
pub mod upkeep;
pub mod visits;

use aruna_core::applet::hosts::{HostRow, host_key};
use aruna_core::applet::record::{Applet, applet_key, group_key, held_bucket};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{APPLET_HOSTS_KEYSPACE, APPLETS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::request_policy::PolicyRequestExtras;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{bucket_permission_path, group_permission_path};
use aruna_core::types::{Key, KeySpace, Value};
use aruna_ops_identity::auth::error::AuthorizeError;
use aruna_ops_identity::auth::request_authorization::AuthorizeOperation;
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Error, PartialEq)]
pub enum AppletError {
    /// Unknown, or hidden from the caller.
    #[error("applet not found")]
    NotFound,
    #[error("permission denied")]
    Forbidden,
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    /// A visitor, a guest or the whole applet used up a cap.
    #[error("{0}")]
    Limited(String),
    /// Another node decides about this applet; writes go there.
    #[error("the applet is hosted on node {0}")]
    Elsewhere(NodeId),
    #[error("storage failed: {0}")]
    Storage(String),
}

impl From<IoError> for AppletError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.to_string())
    }
}

impl From<ConversionError> for AppletError {
    fn from(error: ConversionError) -> Self {
        Self::Storage(error.to_string())
    }
}

/// What a caller needs on an applet's group to act on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Need {
    /// READ on the group's data on the host node.
    Read,
    /// WRITE on the group's data on the host node.
    Write,
    /// WRITE on the held bucket, the right to publish every file of a version.
    Publish,
}

/// The permission path and access `need` asks for on `applet`.
pub fn need_path(applet: &Applet, realm_id: RealmId, need: Need) -> (String, Permission) {
    let (group, node) = (applet.group_id, applet.host_node);
    match need {
        Need::Read => (
            group_permission_path(realm_id, group, node),
            Permission::READ,
        ),
        Need::Write => (
            group_permission_path(realm_id, group, node),
            Permission::WRITE,
        ),
        Need::Publish => {
            let bucket = held_bucket(group, node);
            let path = bucket_permission_path(realm_id, group, node, &bucket);
            (path, Permission::WRITE)
        }
    }
}

/// The child that checks `need` for `auth` on `applet`.
pub(crate) fn authorize(auth: &AuthContext, applet: &Applet, need: Need) -> IoCall {
    let (path, permission) = need_path(applet, auth.realm_id, need);
    authorize_path(auth, &path, permission)
}

/// The child that checks `permission` for `auth` on `path`.
pub(crate) fn authorize_path(auth: &AuthContext, path: &str, permission: Permission) -> IoCall {
    let extras = PolicyRequestExtras::rest();
    let check = AuthorizeOperation::new(auth.realm_id, auth, path, &permission, extras);
    IoCall::child(check)
}

/// Whether the [`authorize`] child allowed the call; a fault inside the check is not a denial.
pub(crate) fn allowed(result: IoResult) -> Result<bool, AppletError> {
    match result.child::<(), AuthorizeError>()? {
        Ok(()) => Ok(true),
        Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => Ok(false),
        Err(error) => Err(AppletError::Storage(error.to_string())),
    }
}

pub(crate) fn read_applet(applet_id: Ulid) -> IoCall {
    IoCall::Read(vec![(APPLETS_KEYSPACE.to_string(), applet_key(applet_id))])
}

pub(crate) fn applet_answer(result: IoResult) -> Result<Option<Applet>, AppletError> {
    let value = result.value()?;
    Ok(value.as_deref().map(Applet::from_bytes).transpose()?)
}

/// The record row and its group index row.
pub(crate) fn record_rows(applet: &Applet) -> Result<Vec<(KeySpace, Key, Value)>, AppletError> {
    let space = APPLETS_KEYSPACE.to_string();
    Ok(vec![
        (
            space.clone(),
            applet_key(applet.applet_id),
            ByteView::from(applet.to_bytes()?),
        ),
        (
            space,
            group_key(applet.group_id, applet.applet_id),
            ByteView::from(Vec::new()),
        ),
    ])
}

/// The host index row of `host` naming `row`.
pub(crate) fn host_row(host: &str, row: HostRow) -> Result<(KeySpace, Key, Value), AppletError> {
    let value = postcard::to_allocvec(&row).map_err(ConversionError::from)?;
    Ok((
        APPLET_HOSTS_KEYSPACE.to_string(),
        host_key(host),
        ByteView::from(value),
    ))
}

pub(crate) fn host_answer(value: Option<Value>) -> Result<Option<HostRow>, AppletError> {
    let row = value
        .as_deref()
        .map(postcard::from_bytes::<HostRow>)
        .transpose();
    Ok(row.map_err(ConversionError::from)?)
}

/// What [`AppletAccessOperation`] waits for.
#[derive(Debug, PartialEq)]
enum AccessStep {
    Record,
    Need(Box<Applet>),
    Visible,
}

/// Reads `applet_id` and checks `need`. A caller that may not read the applet does not see it;
/// one that may read it but lacks `need` is forbidden.
#[derive(Debug, PartialEq)]
pub struct AppletAccessOperation {
    auth: AuthContext,
    applet_id: Ulid,
    need: Need,
    step: AccessStep,
}

impl AppletAccessOperation {
    pub fn new(auth: &AuthContext, applet_id: Ulid, need: Need) -> Self {
        Self {
            auth: auth.clone(),
            applet_id,
            need,
            step: AccessStep::Record,
        }
    }
}

impl Operation for AppletAccessOperation {
    type Output = Applet;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<Applet>, AppletError> {
        Ok(Next::Call(read_applet(self.applet_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Applet>, AppletError> {
        match std::mem::replace(&mut self.step, AccessStep::Visible) {
            AccessStep::Record => {
                let record = applet_answer(result)?.ok_or(AppletError::NotFound)?;
                let call = authorize(&self.auth, &record, self.need);
                self.step = AccessStep::Need(Box::new(record));
                Ok(Next::Call(call))
            }
            AccessStep::Need(record) => match (allowed(result)?, self.need) {
                (true, _) => Ok(Next::Done(*record)),
                (false, Need::Read) => Err(AppletError::NotFound),
                (false, _) => Ok(Next::Call(authorize(&self.auth, &record, Need::Read))),
            },
            AccessStep::Visible => match allowed(result)? {
                true => Err(AppletError::Forbidden),
                false => Err(AppletError::NotFound),
            },
        }
    }

    fn expected_error(error: &AppletError) -> bool {
        matches!(error, AppletError::NotFound | AppletError::Forbidden)
    }
}

#[cfg(test)]
#[path = "flow_tests.rs"]
mod flow_tests;
