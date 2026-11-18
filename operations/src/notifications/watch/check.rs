//! Decides whether a watch may be created, listed or delivered: the owner must still hold READ
//! on the watched permission path, and the realm and group policies must allow the request.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::errors::ConversionError;
use aruna_core::metrics::WatchMetricReason;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::NotificationKind;
use aruna_core::structs::execution::notification_watch::{
    WatchAuthorizationBinding, WatchEvent, WatchEventKind, WatchEventMask, WatchSubscription,
    watch_path_matches,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use thiserror::Error;
use tracing::warn;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::enforcement::{PolicyEvaluator, PolicyLoadOperation};
use crate::auth::error::PolicyEnforcementError;
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::notifications::placement::filter_local_subscriptions;
use crate::notifications::watch::listing::{ListWatchesOperation, WatchPage, WatchScope};
use crate::notifications::watch::paths::{
    WatchAuthorization, WatchAuthorizationDenial, event_permission_path,
    notification_permission_path, watch_permission_path,
};
use crate::notifications::watch::records::WatchSubscriptionError;

const WATCH_CREATE_OPERATION: &str = "notifications.create_watch";
const WATCH_LIST_OPERATION: &str = "notifications.list_watches";
const WATCH_DELIVERY_OPERATION: &str = "notifications.deliver_watch";

#[derive(Debug, Error, PartialEq)]
pub enum WatchCheckError {
    /// The permission rules could not be evaluated; never a grant.
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    /// A watch of the listed owner could not be checked now.
    #[error("{0}")]
    Unavailable(String),
    #[error(transparent)]
    Listing(#[from] WatchSubscriptionError),
    #[error(transparent)]
    Io(#[from] IoError),
}

/// One permission path the caller must hold READ on, for one policy operation name.
#[derive(Clone, Debug, PartialEq)]
struct PathCheck {
    realm_id: RealmId,
    auth_context: AuthContext,
    permission_path: String,
    operation: &'static str,
}

impl PathCheck {
    /// The check of an owner's watch scope or event; a token restriction never applies here.
    fn owner(
        realm_id: RealmId,
        owner: UserId,
        permission_path: String,
        operation: &'static str,
    ) -> Self {
        let auth_context = AuthContext {
            user_id: owner,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        Self {
            realm_id,
            auth_context,
            permission_path,
            operation,
        }
    }

    /// A denial decided before any read: no user owner, a foreign realm or a restricted token.
    fn refused(&self) -> Option<WatchAuthorization> {
        let auth = &self.auth_context;
        let denial = if auth.user_id.is_nil()
            || auth.user_id.realm_id != self.realm_id
            || auth.realm_id != self.realm_id
        {
            WatchAuthorizationDenial::InvalidOwner
        } else if auth.path_restrictions.is_some() {
            WatchAuthorizationDenial::TokenRestricted
        } else {
            return None;
        };
        Some(WatchAuthorization::Denied(denial))
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum CheckState {
    #[default]
    Permission,
    Policy,
}

/// Runs its path checks in order and answers with the first result that is not `Authorized`.
/// Missing authorization state is denied to hide existence but marked unavailable, so interest
/// publication keeps its retry marker.
#[derive(Clone, Debug, PartialEq)]
pub struct WatchCheckOperation {
    checks: VecDeque<PathCheck>,
    /// The answer decided without any read, such as an invalid binding.
    decided: Option<WatchAuthorization>,
    state: CheckState,
}

type Turn = Result<Next<WatchAuthorization>, WatchCheckError>;

fn denied(denial: WatchAuthorizationDenial) -> WatchCheckOperation {
    WatchCheckOperation {
        checks: VecDeque::new(),
        decided: Some(WatchAuthorization::Denied(denial)),
        state: CheckState::Permission,
    }
}

fn checking(checks: impl IntoIterator<Item = PathCheck>) -> WatchCheckOperation {
    WatchCheckOperation {
        checks: checks.into_iter().collect(),
        decided: None,
        state: CheckState::Permission,
    }
}

impl WatchCheckOperation {
    /// The owner's READ on the watched prefix, as listing the watch asks it.
    pub fn listing(
        realm_id: RealmId,
        owner: UserId,
        path_prefix: &str,
        event_mask: WatchEventMask,
        binding: &WatchAuthorizationBinding,
    ) -> Self {
        Self::scope(
            realm_id,
            owner,
            path_prefix,
            event_mask,
            binding,
            WATCH_LIST_OPERATION,
        )
    }

    /// The owner's READ on the watched prefix, as delivering an event asks it.
    pub fn delivery(
        realm_id: RealmId,
        owner: UserId,
        path_prefix: &str,
        event_mask: WatchEventMask,
        binding: &WatchAuthorizationBinding,
    ) -> Self {
        Self::scope(
            realm_id,
            owner,
            path_prefix,
            event_mask,
            binding,
            WATCH_DELIVERY_OPERATION,
        )
    }

    /// A watch forwarded to the holder: its binding must name the same prefix.
    pub fn forwarded(
        realm_id: RealmId,
        owner: UserId,
        path_prefix: &str,
        event_mask: WatchEventMask,
        binding: &WatchAuthorizationBinding,
    ) -> Self {
        if !binding.watch_path_prefix.is_empty() && binding.watch_path_prefix != path_prefix {
            return denied(WatchAuthorizationDenial::InvalidState);
        }
        Self::scope(
            realm_id,
            owner,
            path_prefix,
            event_mask,
            binding,
            WATCH_CREATE_OPERATION,
        )
    }

    /// A new watch, checked with the caller's own authorization, which may be restricted.
    pub fn creation(
        realm_id: RealmId,
        auth_context: &AuthContext,
        path_prefix: &str,
        event_mask: WatchEventMask,
    ) -> Self {
        let Some(permission_path) = watch_permission_path(realm_id, path_prefix, event_mask) else {
            return denied(WatchAuthorizationDenial::InvalidState);
        };
        checking([PathCheck {
            realm_id,
            auth_context: auth_context.clone(),
            permission_path,
            operation: WATCH_CREATE_OPERATION,
        }])
    }

    /// The owner's READ on the resource one watch event names.
    pub fn event(owner: UserId, binding: &WatchAuthorizationBinding, event: &WatchEvent) -> Self {
        if !binding.is_valid() {
            return denied(WatchAuthorizationDenial::InvalidState);
        }
        let Some(permission_path) = event_permission_path(event) else {
            return denied(WatchAuthorizationDenial::InvalidState);
        };
        let check = PathCheck::owner(
            event.realm_id,
            owner,
            permission_path,
            WATCH_DELIVERY_OPERATION,
        );
        checking([check])
    }

    /// A watch notice in the recipient's inbox: the watch scope it came from, then the
    /// resource it names.
    pub fn notification(
        recipient: UserId,
        kind: &NotificationKind,
        binding: &WatchAuthorizationBinding,
    ) -> Self {
        let realm_id = recipient.realm_id;
        let Some(kind_bit) = notice_kind(kind, &binding.watch_path_prefix) else {
            return denied(WatchAuthorizationDenial::InvalidState);
        };
        let event_mask = WatchEventMask::from_kinds([kind_bit]);
        let scope = Self::listing(
            realm_id,
            recipient,
            &binding.watch_path_prefix,
            event_mask,
            binding,
        );
        if scope.decided.is_some() {
            return scope;
        }
        let Some(permission_path) = notification_permission_path(realm_id, kind) else {
            return denied(WatchAuthorizationDenial::InvalidState);
        };
        let notice = PathCheck::owner(realm_id, recipient, permission_path, WATCH_LIST_OPERATION);
        checking(scope.checks.into_iter().chain([notice]))
    }

    fn scope(
        realm_id: RealmId,
        owner: UserId,
        path_prefix: &str,
        event_mask: WatchEventMask,
        binding: &WatchAuthorizationBinding,
        operation: &'static str,
    ) -> Self {
        if !binding.is_valid() {
            return denied(WatchAuthorizationDenial::InvalidState);
        }
        let Some(permission_path) = watch_permission_path(realm_id, path_prefix, event_mask) else {
            return denied(WatchAuthorizationDenial::InvalidState);
        };
        checking([PathCheck::owner(
            realm_id,
            owner,
            permission_path,
            operation,
        )])
    }

    /// The next check, or `Authorized` once every check passed.
    fn next_check(&mut self) -> Turn {
        let Some(check) = self.checks.front() else {
            return Ok(Next::Done(WatchAuthorization::Authorized));
        };
        if let Some(refused) = check.refused() {
            return Ok(Next::Done(refused));
        }
        self.state = CheckState::Permission;
        let config = CheckPermissionsConfig {
            auth_context: check.auth_context.clone(),
            path: check.permission_path.clone(),
            required_permission: Permission::READ,
        };
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            config,
        ))))
    }

    fn permitted(&mut self, allowed: Result<bool, AuthorizationError>) -> Turn {
        let allowed = match allowed {
            Ok(allowed) => allowed,
            Err(
                error @ (AuthorizationError::InvalidRealmId
                | AuthorizationError::InvalidGroupId
                | AuthorizationError::GroupNotFound
                | AuthorizationError::DocNotFound),
            ) => {
                return Ok(Next::Done(WatchAuthorization::Unavailable(
                    error.to_string(),
                )));
            }
            Err(error) => return Err(error.into()),
        };
        if !allowed {
            let denial = WatchAuthorizationDenial::PermissionDenied;
            return Ok(Next::Done(WatchAuthorization::Denied(denial)));
        }
        let Some(check) = self.checks.front() else {
            return Err(IoError::Unfinished.into());
        };
        self.state = CheckState::Policy;
        let request = policy_request(check);
        let load = PolicyLoadOperation::for_request(check.realm_id, &request);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn evaluated(&mut self, loaded: Result<PolicyEvaluator, PolicyEnforcementError>) -> Turn {
        let Some(check) = self.checks.pop_front() else {
            return Err(IoError::Unfinished.into());
        };
        let verdict = loaded.and_then(|evaluator| evaluator.evaluate(&policy_request(&check)));
        match verdict {
            Ok(()) => self.next_check(),
            Err(PolicyEnforcementError::Denied { .. }) => Ok(Next::Done(
                WatchAuthorization::Denied(WatchAuthorizationDenial::PermissionDenied),
            )),
            Err(PolicyEnforcementError::Unavailable(error)) => {
                Ok(Next::Done(WatchAuthorization::Unavailable(error)))
            }
        }
    }
}

fn policy_request(check: &PathCheck) -> aruna_core::request_policy::PolicyRequest {
    policy_request_with(
        &check.permission_path,
        &Permission::READ,
        Some(&check.auth_context),
        PolicyRequestExtras::operation(check.operation),
    )
}

/// The watch event kind of a watch notice under `prefix`, if it is one.
fn notice_kind(kind: &NotificationKind, prefix: &str) -> Option<WatchEventKind> {
    let (event, path) = match kind {
        NotificationKind::MetadataCreated { path, .. } => (WatchEventKind::MetadataCreated, path),
        NotificationKind::DataUploaded { path, .. } => (WatchEventKind::DataUploaded, path),
        NotificationKind::SyncCompleted { path, .. } => (WatchEventKind::SyncCompleted, path),
        NotificationKind::SyncFailed { path, .. } => (WatchEventKind::SyncFailed, path),
        _ => return None,
    };
    watch_path_matches(event, path, prefix).then_some(event)
}

impl Operation for WatchCheckOperation {
    type Output = WatchAuthorization;
    type Error = WatchCheckError;

    fn first(&mut self) -> Turn {
        match self.decided.take() {
            Some(decided) => Ok(Next::Done(decided)),
            None => self.next_check(),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            CheckState::Permission => self.permitted(result.child::<bool, AuthorizationError>()?),
            CheckState::Policy => {
                let loaded = result.child::<PolicyEvaluator, PolicyEnforcementError>()?;
                self.evaluated(loaded)
            }
        }
    }
}

/// The watches a filter kept, whether it dropped any, and whether a check failed.
#[derive(Debug, Default, PartialEq)]
pub struct AuthorizedWatchSubscriptions {
    pub subscriptions: Vec<WatchSubscription>,
    pub dropped: bool,
    pub check_failed: bool,
}

/// Checks watches one at a time as listing asks. For one owner, it lists the owner's watches
/// first and keeps a denied one with only its deletion id, so the owner can still release quota;
/// for a realm, it drops a denied watch and one whose check failed.
#[derive(Debug, PartialEq)]
pub struct WatchFilterOperation {
    realm_id: RealmId,
    /// The owner whose watches are listed and redacted; `None` filters given watches.
    owner: Option<UserId>,
    /// Waits for the owner's watch list.
    listing: bool,
    pending: VecDeque<WatchSubscription>,
    current: Option<WatchSubscription>,
    outcome: AuthorizedWatchSubscriptions,
}

type FilterTurn = Result<Next<AuthorizedWatchSubscriptions>, WatchCheckError>;

impl WatchFilterOperation {
    pub fn owner(owner: UserId) -> Self {
        Self {
            realm_id: owner.realm_id,
            owner: Some(owner),
            listing: true,
            pending: VecDeque::new(),
            current: None,
            outcome: AuthorizedWatchSubscriptions::default(),
        }
    }

    /// The watches of a realm this node holds; a watch held elsewhere is dropped as stale.
    pub fn realm(
        realm_id: RealmId,
        realm_config: &RealmConfigDocument,
        local_node_id: NodeId,
        subscriptions: Vec<WatchSubscription>,
    ) -> Result<Self, ConversionError> {
        let (held, stale) = filter_local_subscriptions(subscriptions, realm_config, local_node_id)?;
        Ok(Self {
            realm_id,
            owner: None,
            listing: false,
            pending: held.into(),
            current: None,
            outcome: AuthorizedWatchSubscriptions {
                dropped: stale,
                ..AuthorizedWatchSubscriptions::default()
            },
        })
    }

    fn next_watch(&mut self) -> FilterTurn {
        let Some(watch) = self.pending.pop_front() else {
            return Ok(Next::Done(std::mem::take(&mut self.outcome)));
        };
        let check = WatchCheckOperation::listing(
            self.realm_id,
            watch.owner,
            &watch.path_prefix,
            watch.event_mask,
            &watch.authorization,
        );
        self.current = Some(watch);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn checked(&mut self, check: Result<WatchAuthorization, WatchCheckError>) -> FilterTurn {
        let mut watch = self.current.take().ok_or(IoError::Unfinished)?;
        match (self.owner.is_some(), check) {
            (_, Ok(WatchAuthorization::Authorized)) => self.outcome.subscriptions.push(watch),
            (true, Ok(WatchAuthorization::Denied(_))) => {
                watch.path_prefix.clear();
                watch.event_mask = WatchEventMask::empty();
                watch.created_at_ms = 0;
                self.outcome.subscriptions.push(watch);
            }
            (true, Ok(WatchAuthorization::Unavailable(error))) => {
                return Err(WatchCheckError::Unavailable(error));
            }
            (true, Err(error)) => return Err(error),
            (false, Ok(WatchAuthorization::Denied(_))) => self.outcome.dropped = true,
            (false, Ok(WatchAuthorization::Unavailable(_)) | Err(_)) => {
                self.outcome.dropped = true;
                self.outcome.check_failed = true;
                warn!(
                    parent: None,
                    reason = WatchMetricReason::AuthorizationUnavailable.as_str(),
                    "Notification watch authorization check failed"
                );
            }
        }
        self.next_watch()
    }
}

impl Operation for WatchFilterOperation {
    type Output = AuthorizedWatchSubscriptions;
    type Error = WatchCheckError;

    fn first(&mut self) -> FilterTurn {
        match self.owner {
            Some(owner) => {
                let list = ListWatchesOperation::new(WatchScope::Owner(owner));
                Ok(Next::Call(IoCall::child(list)))
            }
            None => self.next_watch(),
        }
    }

    fn next(&mut self, result: IoResult) -> FilterTurn {
        if self.listing {
            self.listing = false;
            let listed = result.child::<WatchPage, WatchSubscriptionError>()??;
            self.pending = listed.subscriptions.into();
            return self.next_watch();
        }
        let check = result.child::<WatchAuthorization, WatchCheckError>()?;
        self.checked(check)
    }
}

#[cfg(test)]
#[path = "check_tests.rs"]
mod tests;
