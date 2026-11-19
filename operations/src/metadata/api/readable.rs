//! Decides whether a caller may read one metadata registry record or act on one path.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::all_values;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::telemetry::record_stage;
use aruna_core::types::GroupId;

use super::MetadataApiError;
use crate::auth::check_permissions::CheckPermissionsConfig;
use crate::auth::enforcement::{policy_reads, read_policies};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use aruna_core::errors::AuthorizationError;

/// Whether the caller may read `record`. A public record needs only the realm and group
/// policies; a private one needs the caller's read permission on its path and the policies too.
/// Every denial reads as not found, so read-by-id is no existence oracle. Its reads are one
/// batch, so a parent in a transaction can run it embedded.
#[derive(Debug, PartialEq)]
pub struct RecordReadableOperation {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    record: MetadataRegistryRecord,
    check: Option<CheckPermissionsConfig>,
    now_ms: u64,
    started_ms: u64,
}

impl RecordReadableOperation {
    pub fn new(
        realm_id: RealmId,
        auth: Option<AuthContext>,
        record: MetadataRegistryRecord,
    ) -> Self {
        Self {
            realm_id,
            auth,
            record,
            check: None,
            now_ms: 0,
            started_ms: 0,
        }
    }

    fn policy_reads(&self) -> Vec<(String, aruna_core::types::Key)> {
        policy_reads(self.realm_id, Some(self.record.group_id))
    }
}

fn permission_error(error: AuthorizationError) -> MetadataApiError {
    match error {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::DocNotFound => MetadataApiError::NotFound,
        other => MetadataApiError::Internal(other.to_string()),
    }
}

impl Operation for RecordReadableOperation {
    type Output = ();
    type Error = MetadataApiError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<()>, MetadataApiError> {
        self.started_ms = self.now_ms;
        if self.record.public {
            return Ok(Next::Call(IoCall::Read(self.policy_reads())));
        }
        let Some(auth) = self.auth.clone() else {
            return Err(MetadataApiError::NotFound);
        };
        if auth.realm_id != self.realm_id {
            return Err(MetadataApiError::NotFound);
        }
        let check = CheckPermissionsConfig {
            auth_context: auth,
            path: self.record.permission_path.clone(),
            required_permission: Permission::READ,
        };
        let mut reads = check.reads().map_err(permission_error)?;
        reads.extend(self.policy_reads());
        self.check = Some(check);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MetadataApiError> {
        // A private record's check is the request's "permission" stage, as the API reports it.
        if self.check.is_some() {
            let elapsed = self.now_ms.saturating_sub(self.started_ms);
            record_stage("permission", Duration::from_millis(elapsed));
        }
        let mut values = all_values(result)?;
        if let Some(check) = self.check.as_ref() {
            let rules = check.reads().map_err(permission_error)?.len();
            let policies = values.split_off(rules.min(values.len()));
            if !check.allowed(values).map_err(permission_error)? {
                return Err(MetadataApiError::NotFound);
            }
            values = policies;
        }
        // Policies see the permission the RBAC check enforced.
        let request = policy_request_with(
            &self.record.permission_path,
            &Permission::READ,
            self.auth.as_ref(),
            PolicyRequestExtras::operation("metadata.read"),
        );
        read_policies(self.realm_id, Some(self.record.group_id), values)
            .and_then(|evaluator| evaluator.evaluate(&request))
            .map_err(|_| MetadataApiError::NotFound)?;
        Ok(Next::Done(()))
    }
}

fn path_error(error: AuthorizationError) -> MetadataApiError {
    match error {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::DocNotFound => MetadataApiError::Forbidden,
        other => MetadataApiError::Internal(other.to_string()),
    }
}

/// Whether the caller holds `permission` on a path of `group_id` and the realm and group
/// policies allow it, all read in one batch; every denial is forbidden.
#[derive(Debug, PartialEq)]
pub(crate) struct PathPermissionOperation {
    realm_id: RealmId,
    group_id: GroupId,
    check: CheckPermissionsConfig,
    now_ms: u64,
    started_ms: u64,
}

impl PathPermissionOperation {
    pub(crate) fn new(
        (realm_id, group_id): (RealmId, GroupId),
        auth: AuthContext,
        path: String,
        permission: Permission,
    ) -> Self {
        let check = CheckPermissionsConfig {
            auth_context: auth,
            path,
            required_permission: permission,
        };
        Self {
            realm_id,
            group_id,
            check,
            now_ms: 0,
            started_ms: 0,
        }
    }
}

impl Operation for PathPermissionOperation {
    type Output = ();
    type Error = MetadataApiError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<()>, MetadataApiError> {
        self.started_ms = self.now_ms;
        if self.check.auth_context.realm_id != self.realm_id {
            return Err(MetadataApiError::Forbidden);
        }
        let mut reads = self.check.reads().map_err(path_error)?;
        reads.extend(policy_reads(self.realm_id, Some(self.group_id)));
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MetadataApiError> {
        let elapsed = self.now_ms.saturating_sub(self.started_ms);
        record_stage("permission", Duration::from_millis(elapsed));
        let mut values = all_values(result)?;
        let rules = self.check.reads().map_err(path_error)?.len();
        let policies = values.split_off(rules.min(values.len()));
        if !self.check.allowed(values).map_err(path_error)? {
            return Err(MetadataApiError::Forbidden);
        }
        // Policies must see the permission the RBAC check enforced; a fixed read would let a
        // write-deny policy pass unevaluated.
        let request = policy_request_with(
            &self.check.path,
            &self.check.required_permission,
            Some(&self.check.auth_context),
            PolicyRequestExtras::operation("metadata.read"),
        );
        read_policies(self.realm_id, Some(self.group_id), policies)
            .and_then(|evaluator| evaluator.evaluate(&request))
            .map_err(|_| MetadataApiError::Forbidden)?;
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aruna_core::UserId;
    use ulid::Ulid;

    fn record(public: bool) -> MetadataRegistryRecord {
        let realm_id = RealmId::from_bytes([1; 32]);
        let group_id = Ulid::from_parts(2, 2);
        let document_id = Ulid::from_parts(3, 3);
        MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: "datasets/read".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                "datasets/read",
                document_id,
            ),
            placement: aruna_core::structs::placement::record::PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_parts(4, 4),
            last_event_id: Ulid::from_parts(4, 4),
        }
    }

    fn auth(realm_id: RealmId) -> AuthContext {
        AuthContext {
            user_id: UserId::local(Ulid::from_parts(5, 5), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    #[test]
    fn anonymous_private_hidden() {
        let mut check =
            RecordReadableOperation::new(RealmId::from_bytes([1; 32]), None, record(false));
        assert_eq!(check.first(), Err(MetadataApiError::NotFound));
        let foreign = auth(RealmId::from_bytes([9; 32]));
        let mut check = RecordReadableOperation::new(
            RealmId::from_bytes([1; 32]),
            Some(foreign),
            record(false),
        );
        assert_eq!(check.first(), Err(MetadataApiError::NotFound));
    }

    #[test]
    fn public_reads_policies() {
        let public = record(true);
        let mut check = RecordReadableOperation::new(public.realm_id, None, public.clone());
        assert_eq!(
            check.first(),
            Ok(Next::Call(IoCall::Read(policy_reads(
                public.realm_id,
                Some(public.group_id)
            ))))
        );
        // A realm without a stored config fails closed, which reads as not found.
        assert_eq!(
            check.next(IoResult::Values(vec![None, None, None])),
            Err(MetadataApiError::NotFound)
        );
    }

    #[test]
    fn private_rules_first() {
        let private = record(false);
        let mut check = RecordReadableOperation::new(
            private.realm_id,
            Some(auth(private.realm_id)),
            private.clone(),
        );
        let Ok(Next::Call(IoCall::Read(reads))) = check.first() else {
            panic!("expected the rule and policy reads");
        };
        let policies = policy_reads(private.realm_id, Some(private.group_id));
        assert!(reads.ends_with(&policies));
        assert!(reads.len() > policies.len());
        // Missing rules allow nothing.
        let answer = IoResult::Values(vec![None; reads.len()]);
        assert!(check.next(answer).is_err());
    }

    #[test]
    fn foreign_path_forbidden() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let foreign = auth(RealmId::from_bytes([9; 32]));
        let path = "aruna/data".to_string();
        let group = (realm_id, Ulid::from_parts(2, 2));
        let mut check = PathPermissionOperation::new(group, foreign, path, Permission::WRITE);
        assert_eq!(check.first(), Err(MetadataApiError::Forbidden));
    }

    #[test]
    fn path_reads_policies() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let group_id = Ulid::from_parts(2, 2);
        let path = record(false).permission_path;
        let mut check = PathPermissionOperation::new(
            (realm_id, group_id),
            auth(realm_id),
            path,
            Permission::WRITE,
        );
        let Ok(Next::Call(IoCall::Read(reads))) = check.first() else {
            panic!("expected one batch read");
        };
        let policies = policy_reads(realm_id, Some(group_id));
        assert!(reads.ends_with(&policies));
        assert!(reads.len() > policies.len());
        // Missing rules deny the write.
        let values = IoResult::Values(vec![None; reads.len()]);
        assert!(check.next(values).is_err());
    }
}
