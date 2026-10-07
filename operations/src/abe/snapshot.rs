//! Compiles scopes from the same authority rows that fence publication.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::auth::permission_rules::{CollectedRole, PermissionRules};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument};
use aruna_core::structs::identity::user::User;
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::encryption::{BucketHolder, HolderOrigin, KeyState};
use aruna_core::structs::storage::holders::admin_users;

#[derive(Debug, PartialEq)]
pub(super) struct Snapshot {
    pub parameters: AbeParameters,
    pub epoch: u64,
    pub revisions: Vec<[u8; 32]>,
    pub rules: PermissionRules,
    pub holder: bool,
    pub holders: std::collections::BTreeSet<aruna_core::UserId>,
    pub policies: bool,
    pub due: bool,
}
impl KeyOperation {
    pub(super) fn snapshot_read(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
    ) -> Result<(), KeyError> {
        if !matches!(values.len(), 8 | 9) {
            return Err(AbeError::Context.into());
        }
        let issues = matches!(
            self.action,
            KeyAction::Request(_)
                | KeyAction::Member(_)
                | KeyAction::Token { .. }
                | KeyAction::Publish(_)
        );
        // Without the user row the account status is unknown, so the grant waits and retries.
        if issues && values[7].1.is_none() {
            return Err(KeyError::Storage);
        }
        let bytes = |index: usize| values[index].1.as_deref().ok_or(KeyError::Missing);
        let realm =
            RealmAuthorizationDocument::from_bytes(bytes(0)?).map_err(|_| AbeError::Context)?;
        let group =
            GroupAuthorizationDocument::from_bytes(bytes(1)?).map_err(|_| AbeError::Context)?;
        let config = RealmConfigDocument::from_bytes(bytes(2)?).map_err(|_| AbeError::Context)?;
        let parameters = AbeParameters::from_bytes(bytes(3)?)?;
        let epoch = u64::from_be_bytes(bytes(4)?.try_into().map_err(|_| AbeError::Epoch)?);
        let key = BucketKeyRecord::from_bytes(bytes(5)?).map_err(|_| AbeError::Context)?;
        if parameters.key != key.key
            || key.state == KeyState::Retired
            || epoch == 0
            || parameters.realm_id != self.auth.realm_id
            || parameters.node_id != self.node
        {
            return Err(AbeError::Parameters.into());
        }
        let info = self.info.as_ref().ok_or(KeyError::Missing)?;
        let mut holders = admin_users(
            realm.roles.values().chain(group.roles.values()),
            &group_admin_path(self.auth.realm_id, info.group_id),
        );
        let admin = holders.contains(&self.auth.user_id);
        holders.insert(info.created_by);
        let explicit = values[6]
            .1
            .as_ref()
            .map(|v| BucketHolder::from_bytes(v))
            .transpose()
            .map_err(|_| AbeError::Context)?
            .is_some_and(|h| h.user_id == self.auth.user_id && h.origin == HolderOrigin::Explicit);
        let recipient = self.recipient();
        let user = values[7].1.as_deref().map(User::from_bytes).transpose();
        let deactivated = user
            .map_err(|_| AbeError::Context)?
            .is_some_and(|u| u.is_deactivated());
        let cut_off = self.token_credential().is_some_and(|key| {
            let issued = Ulid::from_string(&key).map_or(0, |id| id.timestamp_ms() / 1000);
            config
                .user_cutoff(&recipient, self.now / 1000)
                .is_some_and(|cutoff| issued < cutoff)
        });
        // A deactivated recipient or a credential issued before its user cutoff gets no new grant.
        if issues && (deactivated || cut_off) {
            return Err(KeyError::Denied);
        }
        let mut roles = realm.roles;
        roles.extend(group.roles.clone());
        let roles = roles
            .into_values()
            .filter_map(|role| {
                let public = role.is_public(self.auth.realm_id);
                let direct = !recipient.is_nil() && role.assigned_users.contains(&recipient);
                (direct || public).then_some(CollectedRole {
                    role,
                    direct,
                    public,
                })
            })
            .collect();
        let restrictions = match &self.action {
            KeyAction::Publish(grant) => grant.context.request.restrictions.as_deref(),
            KeyAction::Member(_) => None,
            KeyAction::Token { restrictions, .. } => restrictions.as_deref(),
            _ => self.auth.path_restrictions.as_deref(),
        };
        let rules =
            PermissionRules::from_roles(roles, restrictions).map_err(|_| KeyError::Denied)?;
        let revisions = values[..3]
            .iter()
            .map(|(_, v)| *blake3::hash(v.as_deref().unwrap_or_default()).as_bytes())
            .collect();
        let policies = config
            .request_policies
            .iter()
            .chain(group.policies.iter())
            .any(|p| p.applies_to_reads());
        self.snapshot = Some(Snapshot {
            parameters,
            epoch,
            revisions,
            rules,
            holder: info.created_by == self.auth.user_id || admin || explicit,
            holders,
            policies,
            due: values.get(8).is_some_and(|(_, v)| v.is_some()),
        });
        Ok(())
    }
    pub(super) fn scope_allowed(&self, scope: &KeyScope) -> Result<(), KeyError> {
        let snapshot = self.snapshot.as_ref().ok_or(KeyError::Missing)?;
        if snapshot.policies {
            return Err(AbeError::Scope.into());
        }
        if !snapshot.rules.admits_scope(&self.root()?, scope) {
            return Err(KeyError::Denied);
        }
        Ok(())
    }
    /// Literal scopes of the recipient's direct READ or WRITE rules and a token's restrictions;
    /// the whole bucket wins.
    pub(super) fn member_scopes(&self) -> Result<Vec<KeyScope>, KeyError> {
        let snapshot = self.snapshot.as_ref().ok_or(KeyError::Missing)?;
        if snapshot.policies {
            return Ok(Vec::new());
        }
        let root = self.root()?;
        let inner = format!("{root}/");
        let glob = |v: &str| v.contains(['*', '?', '[', ']', '{', '}', '\\']);
        let mut scopes = Vec::new();
        let mut patterns = snapshot.rules.direct_patterns();
        if let KeyAction::Token {
            restrictions: Some(restrictions),
            ..
        } = &self.action
        {
            patterns.extend(
                restrictions
                    .iter()
                    .map(|r| (r.pattern.clone(), r.permission.clone())),
            );
        }
        for (pattern, permission) in patterns {
            let scope = match pattern.strip_suffix("**") {
                _ if permission == Permission::DENY => continue,
                Some(base) if !glob(base) && inner.starts_with(base) => {
                    KeyScope::Subtree(String::new())
                }
                Some(base) if !glob(base) => match base.strip_prefix(&inner) {
                    Some(prefix) => KeyScope::Subtree(prefix.to_string()),
                    None => continue,
                },
                None if !glob(&pattern) => match pattern.strip_prefix(&inner) {
                    Some(key) => KeyScope::Exact(key.to_string()),
                    None => continue,
                },
                _ => continue,
            };
            if !scopes.contains(&scope) && snapshot.rules.admits_scope(&root, &scope) {
                scopes.push(scope);
            }
        }
        let whole = KeyScope::Subtree(String::new());
        if scopes.contains(&whole) {
            return Ok(vec![whole]);
        }
        scopes.truncate(MAX_REQUESTS);
        Ok(scopes)
    }
    pub(super) fn root(&self) -> Result<String, KeyError> {
        let info = self.info.as_ref().ok_or(KeyError::Missing)?;
        Ok(aruna_core::structs::storage::blob::bucket_permission_path(
            self.auth.realm_id,
            info.group_id,
            self.node,
            &self.bucket,
        ))
    }
    pub(super) fn recipient(&self) -> aruna_core::UserId {
        match &self.action {
            KeyAction::Publish(grant) => grant.context.request.recipient_user,
            KeyAction::Member(user) => *user,
            _ => self.auth.user_id,
        }
    }
}
