//! Deactivates or reactivates an account and cuts off the credentials it held before.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::auth::{REVOCATION_GRACE_SECS, user_cutoff_expiry, user_cutoff_hash};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::done;
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::structs::identity::user::User;
use aruna_core::types::{GroupId, Value};
use aruna_core::user::validation::{DEACTIVATED_ATTRIBUTE, STOPPED_BY_INSTALL};
use byteview::ByteView;
use std::collections::HashMap;
use thiserror::Error;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::revoke_token::{RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenOperation};
use crate::users::update_user::{UpdateUserInput, UpdateUserOperation};

pub(crate) const REALM_ADMIN_ROLE: &str = "realm_admin";

#[derive(Clone, Debug, PartialEq)]
pub struct AccountStatusConfig {
    pub actor: Actor,
    pub auth_context: AuthContext,
    pub target: UserId,
    /// `false` deactivates the account, `true` reactivates it.
    pub active: bool,
    pub now: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum AccountStatusError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error("account not found")]
    NotFound,
    #[error("caller may not change this account")]
    Unauthorized,
    #[error("a realm administrator cannot be deactivated; remove the role first")]
    Administrator,
    #[error("account update failed: {0}")]
    Update(String),
    #[error("credential cutoff failed: {0}")]
    Cutoff(String),
    #[error("account status operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for AccountStatusError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "AccountStatus".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

type Turn = Result<Next<()>, AccountStatusError>;

/// Account status is a reserved user attribute; deactivation first writes a realm-wide cutoff so
/// every credential issued before it is denied on every node.
#[derive(Debug, PartialEq)]
pub struct AccountStatusOperation {
    config: AccountStatusConfig,
    /// Whether the target holds the realm administrator role, known once the records are read.
    administrator: Option<bool>,
    /// The group owning a service account target, whose administrators may read its audit row.
    owner_group: Option<GroupId>,
    guard: Option<aruna_core::plugin_installs::InstallGuard>,
    /// Marks a deactivation as the plugin install's own, not an administrator's.
    by_install: bool,
}

impl AccountStatusOperation {
    pub fn new(config: AccountStatusConfig) -> Self {
        Self {
            config,
            administrator: None,
            owner_group: None,
            guard: None,
            by_install: false,
        }
    }

    fn deactivated_value(&self) -> String {
        match self.by_install {
            true => STOPPED_BY_INSTALL.to_string(),
            false => "true".to_string(),
        }
    }

    /// Records the deactivation as the account's own plugin install stopping it.
    pub fn by_install(mut self) -> Self {
        self.by_install = true;
        self
    }

    /// Marks the account only while the guard admits it, read in the same transaction.
    pub fn guarded(mut self, guard: aruna_core::plugin_installs::InstallGuard) -> Self {
        self.guard = Some(guard);
        self
    }

    fn user_read(user_id: &UserId) -> (String, ByteView) {
        (
            USER_KEYSPACE.to_string(),
            ByteView::from(user_id.to_bytes()),
        )
    }

    fn authorize(&mut self, values: Vec<Option<Value>>) -> Turn {
        let [target, caller, realm_auth] =
            <[_; 3]>::try_from(values).map_err(|_| AccountStatusError::NotFound)?;
        // A federated user has no record here; only its credentials can be cut off.
        let target = match target.as_deref().map(User::from_bytes) {
            Some(target) => Some(target?),
            None if self.foreign() => None,
            None => return Err(AccountStatusError::NotFound),
        };
        // Service accounts never manage other accounts, whatever roles they hold.
        let caller = caller.as_deref().map(User::from_bytes).transpose()?;
        if caller.is_some_and(|caller| caller.service_group().is_some()) {
            return Err(AccountStatusError::Unauthorized);
        }
        let realm_auth = (realm_auth.as_deref())
            .map(RealmAuthorizationDocument::from_bytes)
            .transpose()?;
        self.administrator = Some(holds_admin_role(realm_auth.as_ref(), &self.config.target));
        let realm_id = self.config.actor.realm_id;
        self.owner_group = target.as_ref().and_then(User::service_group);
        let path = match self.owner_group {
            Some(group_id) => format!("/{realm_id}/g/{group_id}/admin"),
            None => format!("/{realm_id}/admin/config"),
        };
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.config.auth_context.clone(),
            path,
            required_permission: Permission::WRITE,
        });
        let child = boxed_suboperation(check, |allowed| {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed })
        });
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn authorized(&mut self, allowed: Result<bool, AuthorizationError>) -> Turn {
        if !allowed? {
            return Err(AccountStatusError::Unauthorized);
        }
        // No deactivation lowers the administrator count, so no count must agree across nodes.
        if self.administrator == Some(true) && !self.config.active {
            return Err(AccountStatusError::Administrator);
        }
        self.next_write()
    }

    /// Deactivation cuts credentials off before it marks the account, so a failure between the
    /// two leaves an active account without old credentials, and a retry completes it.
    fn next_write(&mut self) -> Turn {
        if self.config.active {
            return self.mark();
        }
        // The grace also cuts off tokens another node mints before the status reaches it.
        let cutoff = self.config.now.saturating_add(REVOCATION_GRACE_SECS);
        let revoke = RevokeTokenOperation::new(RevokeTokenConfig {
            actor: self.config.actor.clone(),
            token_hash: user_cutoff_hash(&self.config.target),
            expires_at: user_cutoff_expiry(cutoff),
            token_owner: self.config.target,
            admission: RevokeTokenAdmission::Privileged,
            now: self.config.now,
        });
        let child = boxed_suboperation(revoke, |result| {
            Event::SubOperation(SubOperationEvent::TokenRevoked {
                result: result.map(|_| ()).map_err(|error| error.to_string()),
            })
        });
        Ok(Next::Call(IoCall::Child(child)))
    }

    /// Updates the local account, or writes only the audit row for a federated one.
    fn mark(&mut self) -> Turn {
        if self.foreign() {
            return self.finish();
        }
        let (set_attributes, remove_attributes) = if self.config.active {
            (HashMap::new(), vec![DEACTIVATED_ATTRIBUTE.to_string()])
        } else {
            (
                HashMap::from([(DEACTIVATED_ATTRIBUTE.to_string(), self.deactivated_value())]),
                Vec::new(),
            )
        };
        let update = UpdateUserOperation::new(UpdateUserInput {
            actor: self.config.actor.clone(),
            auth_context: self.config.auth_context.clone(),
            self_realm_id: self.config.actor.realm_id,
            user_id: self.config.target.to_string(),
            name: None,
            set_attributes,
            remove_attributes,
            system: true,
            alias: None,
            subject: None,
        })
        .audited(self.audit_record())
        .guarded(self.guard);
        let child = boxed_suboperation(update, |result| {
            Event::SubOperation(SubOperationEvent::UserUpdated {
                result: result.map(|_| ()).map_err(|error| error.to_string()),
            })
        });
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn foreign(&self) -> bool {
        self.config.target.realm_id != self.config.actor.realm_id
    }

    fn audit_record(&self) -> AdminAuditRecord {
        let action = match self.config.active {
            true => AdminAuditAction::AccountReactivated,
            false => AdminAuditAction::AccountDeactivated,
        };
        let mut record =
            AdminAuditRecord::new(action, self.config.actor.user_id, self.config.target);
        record.group_id = self.owner_group;
        record
    }

    /// A federated target has no local user row to commit the audit row with.
    fn finish(&mut self) -> Turn {
        let writes = self.audit_record().entries()?;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn answered(&mut self, event: Event) -> Turn {
        match event {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                self.authorized(allowed)
            }
            Event::SubOperation(SubOperationEvent::TokenRevoked { result }) => {
                result.map_err(AccountStatusError::Cutoff)?;
                self.mark()
            }
            Event::SubOperation(SubOperationEvent::UserUpdated { result }) => {
                result.map_err(AccountStatusError::Update)?;
                Ok(Next::Done(()))
            }
            other => Err(AccountStatusError::UnexpectedEvent {
                state: "AccountStatus".to_string(),
                expected: "authorization, token revocation or user update result",
                got: other.label(),
            }),
        }
    }
}

fn holds_admin_role(realm_auth: Option<&RealmAuthorizationDocument>, target: &UserId) -> bool {
    realm_auth.is_some_and(|realm_auth| {
        realm_auth
            .roles
            .values()
            .any(|role| role.name == REALM_ADMIN_ROLE && role.assigned_users.contains(target))
    })
}

impl Operation for AccountStatusOperation {
    type Output = ();
    type Error = AccountStatusError;

    fn first(&mut self) -> Turn {
        // Federated callers never change account status; local admins may cut them off.
        if self.config.auth_context.user_id.realm_id != self.config.actor.realm_id {
            return Err(AccountStatusError::Unauthorized);
        }
        let realm_id = self.config.actor.realm_id;
        let realm_auth = DocumentTarget::RealmAuthorization { realm_id };
        Ok(Next::Call(IoCall::Read(vec![
            Self::user_read(&self.config.target),
            Self::user_read(&self.config.auth_context.user_id),
            (
                realm_auth.storage_keyspace().to_string(),
                realm_auth.storage_key(),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match result {
            IoResult::Values(values) if self.administrator.is_none() => self.authorize(values),
            IoResult::Event(event) => self.answered(*event),
            result => {
                done(result)?;
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::auth::Role;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::user::validation::SERVICE_GROUP_ATTRIBUTE;
    use std::collections::HashSet;
    use ulid::Ulid;

    struct Fixture {
        actor: Actor,
        target: UserId,
        other: UserId,
    }

    fn fixture() -> Fixture {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let caller = UserId::local(Ulid::from_bytes([1u8; 16]), realm_id);
        Fixture {
            actor: Actor {
                node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
                user_id: caller,
                realm_id,
            },
            target: UserId::local(Ulid::from_bytes([2u8; 16]), realm_id),
            other: UserId::local(Ulid::from_bytes([3u8; 16]), realm_id),
        }
    }

    fn operation(fixture: &Fixture, active: bool) -> AccountStatusOperation {
        AccountStatusOperation::new(AccountStatusConfig {
            actor: fixture.actor.clone(),
            auth_context: AuthContext {
                user_id: fixture.actor.user_id,
                realm_id: fixture.actor.realm_id,
                path_restrictions: None,
                session: None,
            },
            target: fixture.target,
            active,
            now: 1_000,
        })
    }

    fn user_bytes(fixture: &Fixture, user_id: UserId, attributes: &[(&str, &str)]) -> ByteView {
        let user = User {
            user_id,
            name: "user".to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: attributes
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        };
        ByteView::from(user.to_bytes(&fixture.actor).unwrap())
    }

    fn realm_auth(fixture: &Fixture, admins: &[UserId]) -> ByteView {
        let role_id = Ulid::from_bytes([9u8; 16]);
        let document = RealmAuthorizationDocument {
            realm_id: fixture.actor.realm_id,
            roles: HashMap::from([(
                role_id,
                Role {
                    role_id,
                    name: REALM_ADMIN_ROLE.to_string(),
                    permissions: HashMap::new(),
                    assigned_users: admins.iter().copied().collect::<HashSet<_>>(),
                },
            )]),
            operation_restrictions: HashMap::new(),
        };
        ByteView::from(document.to_bytes(&fixture.actor).unwrap())
    }

    fn records(target: ByteView, caller: Option<ByteView>, realm_auth: ByteView) -> IoResult {
        IoResult::Values(vec![Some(target), caller, Some(realm_auth)])
    }

    fn answer(event: SubOperationEvent) -> IoResult {
        IoResult::Event(Box::new(Event::SubOperation(event)))
    }

    fn allowed() -> IoResult {
        answer(SubOperationEvent::AuthorizationResult { allowed: Ok(true) })
    }

    fn revoked() -> IoResult {
        answer(SubOperationEvent::TokenRevoked { result: Ok(()) })
    }

    fn updated() -> IoResult {
        answer(SubOperationEvent::UserUpdated { result: Ok(()) })
    }

    fn is_child(turn: Turn) -> bool {
        matches!(turn, Ok(Next::Call(IoCall::Child(_))))
    }

    /// Checks the audit write, finishes it and returns the row the operation recorded.
    fn audited(operation: &mut AccountStatusOperation, turn: Turn) -> AdminAuditRecord {
        let Ok(Next::Call(IoCall::Write(writes))) = turn else {
            panic!("expected the audit write, got {turn:?}");
        };
        let record = AdminAuditRecord::from_bytes(&writes[0].2).unwrap();
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
        record
    }

    /// Checks that the user update ends the operation and returns the row it committed.
    fn committed(operation: &mut AccountStatusOperation, turn: Turn) -> AdminAuditRecord {
        assert_eq!(turn, Ok(Next::Done(())), "no write follows the update");
        operation.audit_record()
    }

    #[test]
    fn service_caller_refused() {
        let fixture = fixture();
        let mut operation = operation(&fixture, false);
        operation.first().unwrap();
        let caller = user_bytes(
            &fixture,
            fixture.actor.user_id,
            &[(SERVICE_GROUP_ATTRIBUTE, &Ulid::generate().to_string())],
        );
        let refused = operation.next(records(
            user_bytes(&fixture, fixture.target, &[]),
            Some(caller),
            realm_auth(&fixture, &[fixture.actor.user_id]),
        ));
        assert_eq!(refused, Err(AccountStatusError::Unauthorized));
    }

    #[test]
    fn administrator_kept_active() {
        // A realm administrator is refused before anything is written, cutoff included.
        let fixture = fixture();
        let mut operation = operation(&fixture, false);
        operation.first().unwrap();
        operation
            .next(records(
                user_bytes(&fixture, fixture.target, &[]),
                None,
                realm_auth(&fixture, &[fixture.target, fixture.other]),
            ))
            .unwrap();
        let refused = operation.next(allowed());
        assert_eq!(refused, Err(AccountStatusError::Administrator));
    }

    #[test]
    fn deactivation_writes_cutoff() {
        let fixture = fixture();
        let mut operation = operation(&fixture, false);
        operation.first().unwrap();
        operation
            .next(records(
                user_bytes(&fixture, fixture.target, &[]),
                None,
                realm_auth(&fixture, &[fixture.other]),
            ))
            .unwrap();
        assert!(is_child(operation.next(allowed())));
        // The cutoff comes first, so a failed status write never leaves old credentials usable.
        assert!(is_child(operation.next(revoked())));
        let turn = operation.next(updated());
        let record = committed(&mut operation, turn);
        assert_eq!(record.action, AdminAuditAction::AccountDeactivated);
        assert_eq!(
            (record.actor, record.target),
            (fixture.actor.user_id, fixture.target)
        );
        assert_eq!(record.group_id, None);
    }

    #[test]
    fn reactivation_skips_cutoff() {
        // Reactivation removes only the status; the old cutoff keeps older credentials dead.
        let fixture = fixture();
        let mut operation = operation(&fixture, true);
        operation.first().unwrap();
        operation
            .next(records(
                user_bytes(&fixture, fixture.target, &[(DEACTIVATED_ATTRIBUTE, "true")]),
                None,
                realm_auth(&fixture, &[fixture.target]),
            ))
            .unwrap();
        assert!(is_child(operation.next(allowed())));
        let turn = operation.next(updated());
        let record = committed(&mut operation, turn);
        assert_eq!(record.action, AdminAuditAction::AccountReactivated);
    }

    #[test]
    fn service_target_audited() {
        // The owning group's administrators may read the row of a service account change.
        let fixture = fixture();
        let group = Ulid::from_bytes([9u8; 16]);
        let mut operation = operation(&fixture, false);
        operation.first().unwrap();
        operation
            .next(records(
                user_bytes(
                    &fixture,
                    fixture.target,
                    &[(SERVICE_GROUP_ATTRIBUTE, &group.to_string())],
                ),
                None,
                realm_auth(&fixture, &[fixture.other]),
            ))
            .unwrap();
        operation.next(allowed()).unwrap();
        operation.next(revoked()).unwrap();
        let turn = operation.next(updated());
        let record = committed(&mut operation, turn);
        assert_eq!(record.group_id, Some(group));
    }

    #[test]
    fn denied_caller_refused() {
        let fixture = fixture();
        let mut operation = operation(&fixture, false);
        operation.first().unwrap();
        operation
            .next(records(
                user_bytes(&fixture, fixture.target, &[]),
                None,
                realm_auth(&fixture, &[fixture.other]),
            ))
            .unwrap();
        let denied = answer(SubOperationEvent::AuthorizationResult { allowed: Ok(false) });
        assert_eq!(
            operation.next(denied),
            Err(AccountStatusError::Unauthorized)
        );
    }

    #[test]
    fn cuts_federated_user() {
        // A federated user has no record here; deactivation writes only the cutoff.
        let mut fixture = fixture();
        fixture.target = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId::from_bytes([6u8; 32]));
        let mut operation = operation(&fixture, false);
        operation.first().unwrap();
        let rows = IoResult::Values(vec![None, None, Some(realm_auth(&fixture, &[]))]);
        operation.next(rows).unwrap();
        assert!(is_child(operation.next(allowed())));
        let turn = operation.next(revoked());
        audited(&mut operation, turn);
    }

    #[test]
    fn federated_caller_refused() {
        // Group admin roles of a federated user never reach service-account status changes.
        let mut fixture = fixture();
        fixture.actor.user_id =
            UserId::new(Ulid::from_bytes([1u8; 16]), RealmId::from_bytes([6u8; 32]));
        let mut operation = operation(&fixture, false);
        assert_eq!(operation.first(), Err(AccountStatusError::Unauthorized));
    }

    #[test]
    fn missing_target_refused() {
        let fixture = fixture();
        let mut operation = operation(&fixture, false);
        operation.first().unwrap();
        let missing = operation.next(IoResult::Values(vec![None, None, None]));
        assert_eq!(missing, Err(AccountStatusError::NotFound));
    }
}
