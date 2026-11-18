//! Decides whether a caller may read one metadata registry record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::all_values;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;

use super::MetadataApiError;
use crate::auth::check_permissions::CheckPermissionsConfig;
use crate::auth::enforcement::{policy_reads, read_policies};
use crate::auth::error::AuthorizationError;
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};

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

    fn first(&mut self) -> Result<Next<()>, MetadataApiError> {
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
            placement: Default::default(),
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
    fn private_reads_rules_first() {
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
}
