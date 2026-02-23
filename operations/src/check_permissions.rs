use aruna_core::consts::AUTH_KEYSPACE;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{AuthContext, GroupAuthorizationDocument, Permission, RealmAuthorizationDocument, RealmId, Role};
use aruna_core::types::{Effects, GroupId, TxnId};
use globset::Glob;
use smallvec::smallvec;
use std::collections::HashMap;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug)]
pub struct CheckPermissionsConfig {
    pub auth_context: AuthContext,
    pub path: String,
    pub required_permission: Permission,
}

#[derive(Debug)]
pub struct CheckPermissionsOperation {
    config: CheckPermissionsConfig,
    txn_id: Option<TxnId>,
    group_id: Option<Ulid>,
    realm_auth_doc: Option<RealmAuthorizationDocument>,
    group_auth_doc: Option<GroupAuthorizationDocument>,
    output: Option<Result<bool, CheckPermissionsError>>,
    state: CheckPermissionsState,
}

#[derive(Debug, Clone, Copy)]
pub enum CheckPermissionsState {
    Init,
    StartTransaction,
    GetRealmAuthDoc,
    GetGroupAuthDoc,
    CheckPermissions,
    Finish,
    Error,
}

#[derive(Debug, Error)]
pub enum CheckPermissionsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    GlobError(#[from] globset::Error),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Invalid realm id")]
    InvalidRealmId,
    #[error("Invalid group id")]
    InvalidGroupId,
    #[error("No group found")]
    GroupNotFound,
    #[error("No group found")]
    AuthDocNotFound,
    #[error("Creating Group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: CheckPermissionsState,
        expected: &'static str,
        got: String,
    },
}

impl CheckPermissionsOperation {
    pub fn new(config: CheckPermissionsConfig) -> Self {
        CheckPermissionsOperation {
            config,
            txn_id: None,
            realm_auth_doc: None,
            group_id: None,
            group_auth_doc: None,
            output: None,
            state: CheckPermissionsState::Init,
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        if let (
            CheckPermissionsState::StartTransaction,
            Event::Storage(StorageEvent::TransactionStarted { txn_id }),
        ) = (self.state, event)
        {
            self.txn_id = Some(txn_id);
            let effects = self.get_realm_auth_doc();
            match effects {
                Ok(effects) => effects,
                Err(err) => self.fail(err),
            }
        } else {
            return self.unexpected_event(
                self.state,
                "Event::Storage(StorageEvent::TransactionStart)",
                got,
            );
        }
    }

    fn handle_realm_auth(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        if let (
            CheckPermissionsState::GetRealmAuthDoc,
            Event::Storage(StorageEvent::ReadResult { value, .. }),
        ) = (self.state, event)
        {
            match self.emit_realm_auth_doc(value) {
                Ok(effects) => effects,
                Err(err) => self.fail(err),
            }
        } else {
            return self.unexpected_event(
                self.state,
                "Event::Storage(StorageEvent::ReadResult)",
                got,
            );
        }
    }

    fn handle_group_auth(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        if let (
            CheckPermissionsState::GetGroupAuthDoc,
            Event::Storage(StorageEvent::ReadResult { value, .. }),
        ) = (self.state, event)
        {
            match self.emit_group_auth_doc(value) {
                Ok(effects) => effects,
                Err(err) => self.fail(err),
            }
        } else {
            return self.unexpected_event(
                self.state,
                "Event::Storage(StorageEvent::ReadResult)",
                got,
            );
        }
    }

    fn handle_check_permissions(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        if let (
            CheckPermissionsState::CheckPermissions,
            Event::Storage(StorageEvent::TransactionCommitted { .. }),
        ) = (self.state, event)
        {
            match self.emit_check_permissions() {
                Ok(effects) => effects,
                Err(err) => self.fail(err),
            }
        } else {
            return self.unexpected_event(
                self.state,
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        }
    }

    fn parse_path(path: &str) -> Result<(RealmId, Option<GroupId>), CheckPermissionsError> {
        let mut levels = path.split("/");
        levels.next();
        let realm = levels
            .next()
            .and_then(|rid| RealmId::from_base64(rid).ok())
            .ok_or_else(|| CheckPermissionsError::InvalidRealmId)?;

        levels.next();
        let group = levels.next().and_then(|g| Ulid::from_string(g).ok());

        Ok((realm, group))
    }

    fn get_realm_auth_doc(&mut self) -> Result<Effects, CheckPermissionsError> {
        self.state = CheckPermissionsState::GetRealmAuthDoc;
        let (realm, group) = CheckPermissionsOperation::parse_path(&self.config.path)?;
        self.group_id = group;
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.to_string(),
            key: (*realm.as_bytes()).into(),
            txn_id: self.txn_id
        })])
    }

    fn emit_realm_auth_doc(
        &mut self,
        value: Option<byteview::ByteView>,
    ) -> Result<Effects, CheckPermissionsError> {
        self.realm_auth_doc = Some(RealmAuthorizationDocument::from_bytes(
            &value.ok_or_else(|| CheckPermissionsError::AuthDocNotFound)?,
        )?);

        match self.group_id {
            Some(group) => {
                self.state = CheckPermissionsState::GetGroupAuthDoc;
                Ok(smallvec![Effect::Storage(StorageEffect::Read {
                    txn_id: self.txn_id,
                    key_space: AUTH_KEYSPACE.to_string(),
                    key: group.to_bytes().into(),
                })])
            }
            None => {
                self.state = CheckPermissionsState::CheckPermissions;
                Ok(smallvec![Effect::Storage(
                    StorageEffect::CommitTransaction {
                        txn_id: self
                            .txn_id
                            .ok_or_else(|| CheckPermissionsError::NoTransactionFound)?
                    }
                )])
            }
        }
    }

    fn emit_group_auth_doc(
        &mut self,
        value: Option<byteview::ByteView>,
    ) -> Result<Effects, CheckPermissionsError> {
        self.state = CheckPermissionsState::CheckPermissions;
        self.group_auth_doc = Some(GroupAuthorizationDocument::from_bytes(
            &value.ok_or_else(|| CheckPermissionsError::AuthDocNotFound)?,
        )?);

        Ok(smallvec![Effect::Storage(
            StorageEffect::CommitTransaction {
                txn_id: self
                    .txn_id
                    .ok_or_else(|| CheckPermissionsError::NoTransactionFound)?
            }
        )])
    }

    fn emit_check_permissions(&mut self) -> Result<Effects, CheckPermissionsError> {
        self.state = CheckPermissionsState::Finish;
        let roles = self.collect_roles()?;
        self.output = Some(self.check_permissions(roles));
        Ok(smallvec![])
    }

    fn collect_roles(&mut self) -> Result<HashMap<Ulid, Role>, CheckPermissionsError> {
        let mut roles = self
            .realm_auth_doc
            .as_ref()
            .ok_or_else(|| CheckPermissionsError::AuthDocNotFound)?
            .roles
            .clone();
        if let Some(group) = &self.group_auth_doc {
            roles.extend(group.roles.clone());
        }
        Ok(roles)
    }

    fn check_permissions(
        &mut self,
        roles: HashMap<Ulid, Role>,
    ) -> Result<bool, CheckPermissionsError> {
        let mut allowed = false;
        for (_, role) in roles {
            for (path, permission) in role.permissions {
                let glob = Glob::new(&path)?.compile_matcher();
                if glob.is_match(&self.config.path) {
                    match permission {
                        Permission::DENY => {
                            return Ok(false);
                        }
                        Permission::READ => {
                            if self.config.required_permission == Permission::READ {
                                allowed = true;
                            }
                        }
                        Permission::WRITE => allowed = true,
                    }
                }
            }
        }
        Ok(allowed)
    }

    fn fail(&mut self, err: CheckPermissionsError) -> Effects {
        self.state = CheckPermissionsState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn fail_with_cleanup(
        &mut self,
        err: CheckPermissionsError,
        cleanup_effects: Effects,
    ) -> Effects {
        self.state = CheckPermissionsState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: CheckPermissionsState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            CheckPermissionsError::UnexpectedEvent {
                state,
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }
}

impl Operation for CheckPermissionsOperation {
    type Output = bool;

    type Error = CheckPermissionsError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = CheckPermissionsState::StartTransaction;

        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            CheckPermissionsState::StartTransaction => self.handle_start_transaction(event),
            CheckPermissionsState::GetRealmAuthDoc => self.handle_realm_auth(event),
            CheckPermissionsState::GetGroupAuthDoc => self.handle_group_auth(event),
            CheckPermissionsState::CheckPermissions => self.handle_check_permissions(event),
            CheckPermissionsState::Finish
            | CheckPermissionsState::Init
            | CheckPermissionsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CheckPermissionsState::Finish | CheckPermissionsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or_else(|| CheckPermissionsError::NotFinished)?
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod test {
    use aruna_core::structs::{Permission, RealmId};
    use aruna_storage::storage;
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};

    #[tokio::test]
    pub async fn test_path_parsing() {
        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let pubkey = realm_signing_key.verifying_key().to_bytes();
        let realm_id = RealmId::from_bytes(pubkey);
        let group_id = Ulid::new();
        let path = format!("/{}/g/{}", realm_id, group_id.to_string());
        let (parsed_realm_id, parsed_group_id) =
            CheckPermissionsOperation::parse_path(&path).unwrap();

        assert_eq!(realm_id, parsed_realm_id);
        assert_eq!(group_id, parsed_group_id.unwrap());

        let path = format!("/{}/admin", realm_id);
        let (parsed_realm_id, parsed_group_id) =
            CheckPermissionsOperation::parse_path(&path).unwrap();

        assert_eq!(realm_id, parsed_realm_id);
        assert!(parsed_group_id.is_none());

        let path = format!("/abcd/g/{}", Ulid::new().to_string());
        assert!(CheckPermissionsOperation::parse_path(&path).is_err());
    }

    #[tokio::test]
    pub async fn test_check_permissions() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(&random_path.path().to_str().unwrap()).unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
        };

        let user_id = Ulid::new();
        let realm_id = RealmId([0u8; 32]);

        let realm_config = CreateRealmConfig {
            user_id,
            realm_id: realm_id.clone(),
            realm_description: "A description".to_string(),
        };

        let realm_operation = CreateRealmOperation::new(realm_config.clone());
        let _result = drive(realm_operation, &context).await.unwrap();

        let group_config = CreateGroupConfig {
            user_id,
            realm_id: realm_id.clone(),
            display_name: "Test group".to_string(),
        };

        let group_operation = CreateGroupOperation::new(group_config.clone());
        let result = drive(group_operation, &context).await.unwrap();
        let group_id = result.0.group_id;

        let perm_config = CheckPermissionsConfig {
            auth_context: aruna_core::structs::AuthContext {
                user_id,
                realm_id: realm_id.clone(),
                path_restrictions: None,
            },
            path: format!(
                "/{}/g/{}/resources/{}",
                realm_id.to_string(),
                group_id.to_string(),
                Ulid::new().to_string()
            ),
            required_permission: Permission::WRITE,
        };
        let perm_operation = CheckPermissionsOperation::new(perm_config.clone());
        let check_result = drive(perm_operation, &context).await.unwrap();
        assert!(check_result);
    }
}
