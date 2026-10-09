//! Deactivates or reactivates an account and cuts off the credentials it held before.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::auth::{REVOCATION_GRACE_SECS, user_cutoff_expiry, user_cutoff_hash};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::structs::identity::user::User;
use aruna_core::types::Effects;
use aruna_core::user::validation::DEACTIVATED_ATTRIBUTE;
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::HashMap;
use thiserror::Error;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::revoke_token::{RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenOperation};
use crate::users::update_user::{UpdateUserInput, UpdateUserOperation};

const REALM_ADMIN_ROLE: &str = "realm_admin";

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
        got: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
enum AccountStatusState {
    Init,
    ReadRecords,
    Authorize { administrator: bool },
    UpdateUser,
    WriteCutoff,
    Finish,
    Error,
}

/// Account status is a reserved user attribute; deactivation first writes a realm-wide cutoff so
/// every credential issued before it is denied on every node.
#[derive(Debug, PartialEq)]
pub struct AccountStatusOperation {
    config: AccountStatusConfig,
    state: AccountStatusState,
    output: Option<Result<(), AccountStatusError>>,
}

impl AccountStatusOperation {
    pub fn new(config: AccountStatusConfig) -> Self {
        Self {
            config,
            state: AccountStatusState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: AccountStatusError) -> Effects {
        self.state = AccountStatusState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        self.fail(AccountStatusError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{event:?}"),
        })
    }

    fn user_read(user_id: &UserId) -> (String, ByteView) {
        (
            USER_KEYSPACE.to_string(),
            ByteView::from(user_id.to_bytes()),
        )
    }

    fn handle_records(&mut self, event: Event) -> Effects {
        let values = match event {
            Event::Storage(StorageEvent::BatchReadResult { values }) => values,
            Event::Storage(StorageEvent::Error { error }) => return self.fail(error.into()),
            other => return self.unexpected("batch read result", other),
        };
        let [(_, target), (_, caller), (_, realm_auth)] = values.as_slice() else {
            return self.fail(AccountStatusError::NotFound);
        };
        // A federated user has no record here; only its credentials can be cut off.
        let target = match target.as_deref().map(User::from_bytes) {
            Some(Ok(target)) => Some(target),
            Some(Err(error)) => return self.fail(error.into()),
            None if self.foreign() => None,
            None => return self.fail(AccountStatusError::NotFound),
        };
        // Service accounts never manage other accounts, whatever roles they hold.
        let caller = caller.as_deref().map(User::from_bytes).transpose();
        match caller {
            Ok(Some(caller)) if caller.service_group().is_some() => {
                return self.fail(AccountStatusError::Unauthorized);
            }
            Ok(_) => {}
            Err(error) => return self.fail(error.into()),
        }
        let realm_auth = realm_auth
            .as_deref()
            .map(RealmAuthorizationDocument::from_bytes)
            .transpose();
        let administrator = match realm_auth {
            Ok(realm_auth) => holds_admin_role(realm_auth.as_ref(), &self.config.target),
            Err(error) => return self.fail(error.into()),
        };
        let realm_id = self.config.actor.realm_id;
        let path = match target.as_ref().and_then(User::service_group) {
            Some(group_id) => format!("/{realm_id}/g/{group_id}/admin"),
            None => format!("/{realm_id}/admin/config"),
        };
        self.state = AccountStatusState::Authorize { administrator };
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path,
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn handle_authorized(&mut self, event: Event, administrator: bool) -> Effects {
        let allowed = match event {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => allowed,
            other => return self.unexpected("authorization result", other),
        };
        match allowed {
            Ok(true) => {}
            Ok(false) => return self.fail(AccountStatusError::Unauthorized),
            Err(error) => return self.fail(error.into()),
        }
        // No deactivation lowers the administrator count, so no count must agree across nodes.
        if administrator && !self.config.active {
            return self.fail(AccountStatusError::Administrator);
        }
        self.next_write()
    }

    /// Deactivation cuts credentials off before it marks the account, so a failure between the
    /// two leaves an active account without old credentials, and a retry completes it.
    fn next_write(&mut self) -> Effects {
        if self.config.active {
            return if self.foreign() {
                self.finish()
            } else {
                self.update_user()
            };
        }
        // The grace also cuts off tokens another node mints before the status reaches it.
        let cutoff = self.config.now.saturating_add(REVOCATION_GRACE_SECS);
        self.state = AccountStatusState::WriteCutoff;
        smallvec![Effect::SubOperation(boxed_suboperation(
            RevokeTokenOperation::new(RevokeTokenConfig {
                actor: self.config.actor.clone(),
                token_hash: user_cutoff_hash(&self.config.target),
                expires_at: user_cutoff_expiry(cutoff),
                token_owner: self.config.target,
                admission: RevokeTokenAdmission::Privileged,
                now: self.config.now,
            }),
            |result| Event::SubOperation(SubOperationEvent::TokenRevoked {
                result: result.map(|_| ()).map_err(|error| error.to_string()),
            }),
        ))]
    }

    fn update_user(&mut self) -> Effects {
        let (set_attributes, remove_attributes) = if self.config.active {
            (HashMap::new(), vec![DEACTIVATED_ATTRIBUTE.to_string()])
        } else {
            (
                HashMap::from([(DEACTIVATED_ATTRIBUTE.to_string(), "true".to_string())]),
                Vec::new(),
            )
        };
        self.state = AccountStatusState::UpdateUser;
        smallvec![Effect::SubOperation(boxed_suboperation(
            UpdateUserOperation::new(UpdateUserInput {
                actor: self.config.actor.clone(),
                auth_context: self.config.auth_context.clone(),
                self_realm_id: self.config.actor.realm_id,
                user_id: self.config.target.to_string(),
                name: None,
                set_attributes,
                remove_attributes,
                system: true,
                alias: None,
            }),
            |result| Event::SubOperation(SubOperationEvent::UserUpdated {
                result: result.map(|_| ()).map_err(|error| error.to_string()),
            }),
        ))]
    }

    fn handle_updated(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::UserUpdated { result: Ok(()) }) => self.finish(),
            Event::SubOperation(SubOperationEvent::UserUpdated { result: Err(error) }) => {
                self.fail(AccountStatusError::Update(error))
            }
            other => self.unexpected("user update result", other),
        }
    }

    fn handle_cutoff(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::TokenRevoked { result: Ok(()) }) => {
                if self.foreign() {
                    self.finish()
                } else {
                    self.update_user()
                }
            }
            Event::SubOperation(SubOperationEvent::TokenRevoked { result: Err(error) }) => {
                self.fail(AccountStatusError::Cutoff(error))
            }
            other => self.unexpected("token revocation result", other),
        }
    }

    fn foreign(&self) -> bool {
        self.config.target.realm_id != self.config.actor.realm_id
    }

    fn finish(&mut self) -> Effects {
        self.state = AccountStatusState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
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

    fn start(&mut self) -> Effects {
        // Federated callers never change account status; local admins may cut them off.
        if self.config.auth_context.user_id.realm_id != self.config.actor.realm_id {
            return self.fail(AccountStatusError::Unauthorized);
        }
        let realm_id = self.config.actor.realm_id;
        let realm_auth = DocumentTarget::RealmAuthorization { realm_id };
        self.state = AccountStatusState::ReadRecords;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                Self::user_read(&self.config.target),
                Self::user_read(&self.config.auth_context.user_id),
                (
                    realm_auth.storage_keyspace().to_string(),
                    realm_auth.storage_key()
                ),
            ],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match std::mem::replace(&mut self.state, AccountStatusState::Error) {
            AccountStatusState::ReadRecords => self.handle_records(event),
            AccountStatusState::Authorize { administrator } => {
                self.handle_authorized(event, administrator)
            }
            AccountStatusState::UpdateUser => self.handle_updated(event),
            AccountStatusState::WriteCutoff => self.handle_cutoff(event),
            AccountStatusState::Init => self.unexpected("no event before start", event),
            state @ (AccountStatusState::Finish | AccountStatusState::Error) => {
                self.state = state;
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AccountStatusState::Finish | AccountStatusState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(AccountStatusError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
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

    fn records(target: ByteView, caller: Option<ByteView>, realm_auth: ByteView) -> Event {
        let key = ByteView::from(Vec::new());
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (key.clone(), Some(target)),
                (key.clone(), caller),
                (key, Some(realm_auth)),
            ],
        })
    }

    fn allowed() -> Event {
        Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: Ok(true) })
    }

    fn failure(operation: AccountStatusOperation) -> AccountStatusError {
        assert!(operation.is_complete());
        operation.finalize().unwrap_err()
    }

    #[test]
    fn service_caller_refused() {
        let fixture = fixture();
        let mut operation = operation(&fixture, false);
        operation.start();
        let caller = user_bytes(
            &fixture,
            fixture.actor.user_id,
            &[(SERVICE_GROUP_ATTRIBUTE, &Ulid::generate().to_string())],
        );
        operation.step(records(
            user_bytes(&fixture, fixture.target, &[]),
            Some(caller),
            realm_auth(&fixture, &[fixture.actor.user_id]),
        ));
        assert_eq!(failure(operation), AccountStatusError::Unauthorized);
    }

    #[test]
    fn administrator_kept_active() {
        // A realm administrator is refused before anything is written, cutoff included.
        let fixture = fixture();
        let mut operation = operation(&fixture, false);
        operation.start();
        operation.step(records(
            user_bytes(&fixture, fixture.target, &[]),
            None,
            realm_auth(&fixture, &[fixture.target, fixture.other]),
        ));
        let effects = operation.step(allowed());
        assert!(effects.is_empty());
        assert_eq!(failure(operation), AccountStatusError::Administrator);
    }

    #[test]
    fn deactivation_writes_cutoff() {
        let fixture = fixture();
        let mut operation = operation(&fixture, false);
        operation.start();
        operation.step(records(
            user_bytes(&fixture, fixture.target, &[]),
            None,
            realm_auth(&fixture, &[fixture.other]),
        ));
        let effects = operation.step(allowed());
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        // The cutoff comes first, so a failed status write never leaves old credentials usable.
        let effects = operation.step(Event::SubOperation(SubOperationEvent::TokenRevoked {
            result: Ok(()),
        }));
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        assert!(!operation.is_complete());
        operation.step(Event::SubOperation(SubOperationEvent::UserUpdated {
            result: Ok(()),
        }));
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn reactivation_skips_cutoff() {
        // Reactivation removes only the status; the old cutoff keeps older credentials dead.
        let fixture = fixture();
        let mut operation = operation(&fixture, true);
        operation.start();
        operation.step(records(
            user_bytes(&fixture, fixture.target, &[(DEACTIVATED_ATTRIBUTE, "true")]),
            None,
            realm_auth(&fixture, &[fixture.target]),
        ));
        let effects = operation.step(allowed());
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        operation.step(Event::SubOperation(SubOperationEvent::UserUpdated {
            result: Ok(()),
        }));
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn denied_caller_refused() {
        let fixture = fixture();
        let mut operation = operation(&fixture, false);
        operation.start();
        operation.step(records(
            user_bytes(&fixture, fixture.target, &[]),
            None,
            realm_auth(&fixture, &[fixture.other]),
        ));
        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));
        assert_eq!(failure(operation), AccountStatusError::Unauthorized);
    }

    #[test]
    fn cuts_off_federated_user() {
        // A federated user has no record here; deactivation writes only the cutoff.
        let mut fixture = fixture();
        fixture.target = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId::from_bytes([6u8; 32]));
        let mut operation = operation(&fixture, false);
        operation.start();
        let key = ByteView::from(Vec::new());
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (key.clone(), None),
                (key, None),
                (ByteView::from(Vec::new()), Some(realm_auth(&fixture, &[]))),
            ],
        }));
        let effects = operation.step(allowed());
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let effects = operation.step(Event::SubOperation(SubOperationEvent::TokenRevoked {
            result: Ok(()),
        }));
        assert!(effects.is_empty());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn federated_caller_refused() {
        // Group admin roles of a federated user never reach service-account status changes.
        let mut fixture = fixture();
        fixture.actor.user_id =
            UserId::new(Ulid::from_bytes([1u8; 16]), RealmId::from_bytes([6u8; 32]));
        let mut operation = operation(&fixture, false);
        assert!(operation.start().is_empty());
        assert_eq!(failure(operation), AccountStatusError::Unauthorized);
    }

    #[test]
    fn missing_target_refused() {
        let fixture = fixture();
        let mut operation = operation(&fixture, false);
        operation.start();
        let key = ByteView::from(Vec::new());
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(key.clone(), None), (key.clone(), None), (key, None)],
        }));
        assert_eq!(failure(operation), AccountStatusError::NotFound);
    }
}
