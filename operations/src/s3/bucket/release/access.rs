//! Loads a caller's access to a bucket and checks release memberships with it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path};
use aruna_core::structs::storage::release::MembershipQuery;
use aruna_core::types::GroupId;

use super::{
    Access, BucketScope, MembersConfig, MembersOutput, MembershipCaller, ReleaseError,
    ReleaseMembersOperation,
};
use crate::auth::enforcement::{policy_reads, read_policies};
use crate::auth::permission_rules::{read_rules, rule_reads};
use crate::auth::request_authorization::BucketAccess;

fn unavailable(error: impl std::fmt::Display) -> ReleaseError {
    ReleaseError::Unavailable(error.to_string())
}

/// The caller's rules and policies for a bucket, read once for every key they decide.
#[derive(Debug, PartialEq)]
pub struct LoadAccessOperation {
    auth: AuthContext,
    realm_id: RealmId,
    group_id: GroupId,
    bucket_path: String,
    rule_rows: usize,
}

impl LoadAccessOperation {
    pub fn new(scope: BucketScope<'_>, auth: &AuthContext) -> Self {
        let group_id = scope.info.group_id;
        Self {
            auth: auth.clone(),
            realm_id: scope.realm_id,
            group_id,
            bucket_path: bucket_permission_path(
                scope.realm_id,
                group_id,
                scope.node_id,
                scope.bucket,
            ),
            rule_rows: 0,
        }
    }
}

impl Operation for LoadAccessOperation {
    type Output = Access;
    type Error = ReleaseError;

    fn first(&mut self) -> Result<Next<Access>, ReleaseError> {
        let mut reads = rule_reads(&self.bucket_path).map_err(unavailable)?;
        self.rule_rows = reads.len();
        reads.extend(policy_reads(self.realm_id, Some(self.group_id)));
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Access>, ReleaseError> {
        let IoResult::Values(mut rows) = result else {
            let got = result.label();
            let expected = "IoResult::Values";
            return Err(IoError::Unexpected { expected, got }.into());
        };
        let policies = rows.split_off(self.rule_rows.min(rows.len()));
        let rules = read_rules(&self.auth, rows).map_err(unavailable)?;
        let policies =
            read_policies(self.realm_id, Some(self.group_id), policies).map_err(unavailable)?;
        let path = std::mem::take(&mut self.bucket_path);
        let access = BucketAccess::from_parts(self.auth.clone(), path, rules, policies);
        Ok(Next::Done(Access(Arc::new(access))))
    }
}

/// Loads the caller's access once, then runs the membership check with it.
#[derive(Debug, PartialEq)]
pub struct MembersCheckOperation {
    realm_id: RealmId,
    node_id: NodeId,
    bucket: String,
    info: BucketInfo,
    auth: AuthContext,
    bearer: Option<String>,
    queries: Vec<MembershipQuery>,
    loaded: bool,
}

impl MembersCheckOperation {
    pub fn new(
        scope: BucketScope<'_>,
        caller: MembershipCaller<'_>,
        queries: &[MembershipQuery],
    ) -> Self {
        Self {
            realm_id: scope.realm_id,
            node_id: scope.node_id,
            bucket: scope.bucket.to_string(),
            info: scope.info.clone(),
            auth: caller.auth.clone(),
            bearer: caller.bearer.map(str::to_string),
            queries: queries.to_vec(),
            loaded: false,
        }
    }
}

impl Operation for MembersCheckOperation {
    type Output = MembersOutput;
    type Error = ReleaseError;

    fn first(&mut self) -> Result<Next<MembersOutput>, ReleaseError> {
        let scope = BucketScope {
            realm_id: self.realm_id,
            node_id: self.node_id,
            bucket: &self.bucket,
            info: &self.info,
        };
        let load = LoadAccessOperation::new(scope, &self.auth);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MembersOutput>, ReleaseError> {
        if self.loaded {
            let found = result.child::<MembersOutput, ReleaseError>()??;
            return Ok(Next::Done(found));
        }
        let access = result.child::<Access, ReleaseError>()??;
        self.loaded = true;
        let config = MembersConfig {
            realm_id: self.realm_id,
            node_id: self.node_id,
            bucket: self.bucket.clone(),
            info: self.info.clone(),
            auth: self.auth.clone(),
            access,
            bearer: self.bearer.take(),
            queries: std::mem::take(&mut self.queries),
            ranges: None,
            retention: false,
        };
        Ok(Next::Call(IoCall::child(ReleaseMembersOperation::new(
            config,
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{REALM, actor, node, user};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::structs::identity::auth::Permission;
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::types::{Key, Value};
    use ulid::Ulid;

    const GROUP: Ulid = Ulid::from_parts(2, 2);

    fn info() -> BucketInfo {
        BucketInfo {
            group_id: GROUP,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        }
    }

    fn auth() -> AuthContext {
        AuthContext::anonymous(REALM)
    }

    fn load() -> LoadAccessOperation {
        let info = info();
        let scope = BucketScope {
            realm_id: REALM,
            node_id: node(1),
            bucket: "db",
            info: &info,
        };
        LoadAccessOperation::new(scope, &auth())
    }

    fn check() -> MembersCheckOperation {
        let (info, auth) = (info(), auth());
        let scope = BucketScope {
            realm_id: REALM,
            node_id: node(1),
            bucket: "db",
            info: &info,
        };
        let caller = MembershipCaller {
            auth: &auth,
            bearer: None,
        };
        MembersCheckOperation::new(scope, caller, &[])
    }

    // The stored row each read of the access load asks for.
    fn stored(space: &str, key: &Key) -> Value {
        let actor = actor(node(1));
        let realm_key = Key::from(REALM.as_bytes().to_vec());
        let group_auth = GroupAuthorizationDocument::default_group_doc(user(), REALM, GROUP);
        let bytes = if space == GROUP_KEYSPACE {
            let group = Group {
                display_name: "g".to_string(),
                group_id: GROUP,
                realm_id: REALM,
                roles: group_auth.roles.keys().copied().collect(),
                owner: user(),
            };
            group.to_bytes(&actor).unwrap()
        } else if space != AUTH_KEYSPACE {
            let config = RealmConfigDocument::default_for_realm(REALM, Vec::new());
            config.to_bytes(&actor).unwrap()
        } else if *key == realm_key {
            let realm = RealmAuthorizationDocument::default_realm_doc(REALM);
            realm.to_bytes(&actor).unwrap()
        } else {
            group_auth.to_bytes(&actor).unwrap()
        };
        Value::from(bytes)
    }

    // Rules and policies come from one read, and the access answers from those rows alone.
    #[test]
    fn one_read_decides() {
        let mut operation = load();
        let Ok(Next::Call(IoCall::Read(reads))) = operation.first() else {
            panic!("the rules and policies are read together");
        };
        assert_eq!(operation.rule_rows, 2);
        let rows = reads
            .iter()
            .map(|(space, key)| Some(stored(space, key)))
            .collect();
        let Ok(Next::Done(access)) = operation.next(IoResult::Values(rows)) else {
            panic!("every row is present");
        };
        assert!(!access.0.allows(Some("k"), &Permission::WRITE));
    }

    // A missing document leaves the caller's access unknown.
    #[test]
    fn missing_rows_unavailable() {
        let mut operation = load();
        let Ok(Next::Call(IoCall::Read(reads))) = operation.first() else {
            panic!("the rules and policies are read together");
        };
        let rows = IoResult::Values(vec![None; reads.len()]);
        assert!(matches!(
            operation.next(rows),
            Err(ReleaseError::Unavailable(_))
        ));
    }

    // The check starts with the access load and ends when that load fails.
    #[test]
    fn load_failure_ends() {
        let mut operation = check();
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let failed = || ReleaseError::Unavailable("down".to_string());
        let answer = IoResult::Child(ChildOutcome::new(Err::<Access, _>(failed())));
        assert_eq!(operation.next(answer), Err(failed()));
    }
}
