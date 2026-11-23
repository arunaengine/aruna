//! Looks up display names for policy refs from the policy rows this node already holds.
//! A ref this node does not hold is left out, and the lookup never fetches from other nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::placement::policy::document::PlacementPolicyDocument;
use aruna_core::types::GroupId;
use std::collections::BTreeMap;
use thiserror::Error;
use ulid::Ulid;

/// Refs one response may carry, generously above the per-record ref cap.
pub const MAX_NAMED_REFS: usize = 32;

/// What a ref resolves to locally. Absent means this node holds no such rule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyName {
    pub name: String,
    pub owner_group_id: Option<GroupId>,
}

#[derive(Debug, Error, PartialEq)]
pub enum PolicyNamesError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event during the policy name lookup")]
    InvalidEvent,
}

impl From<IoError> for PolicyNamesError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            _ => Self::InvalidEvent,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct PolicyNamesOperation {
    realm_id: RealmId,
    /// Deduplicated and bounded at construction.
    ids: Vec<Ulid>,
}

impl PolicyNamesOperation {
    pub fn new(realm_id: RealmId, refs: &[PlacementPolicyRef]) -> Self {
        let mut ids: Vec<Ulid> = refs.iter().map(|policy_ref| policy_ref.policy_id).collect();
        ids.sort_unstable();
        ids.dedup();
        ids.truncate(MAX_NAMED_REFS);
        Self { realm_id, ids }
    }
}

impl Operation for PolicyNamesOperation {
    type Output = BTreeMap<Ulid, PolicyName>;
    type Error = PolicyNamesError;

    /// No refs is an empty read, which the runner answers without storage.
    fn first(&mut self) -> Result<Next<Self::Output>, PolicyNamesError> {
        let reads = (self.ids.iter())
            .map(|policy_id| {
                let target = DocumentTarget::PlacementPolicy {
                    policy_id: *policy_id,
                };
                (target.storage_keyspace().to_string(), target.storage_key())
            })
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, PolicyNamesError> {
        let IoResult::Values(values) = result else {
            return Err(PolicyNamesError::InvalidEvent);
        };
        let mut names = BTreeMap::new();
        for value in values.into_iter().flatten() {
            let document = PlacementPolicyDocument::from_bytes(value.as_ref())?;
            // A row stored for another realm names nothing here.
            if document.realm_id != self.realm_id {
                continue;
            }
            let name = PolicyName {
                name: document.policy.name.clone(),
                owner_group_id: document.policy.owner_group_id,
            };
            names.insert(document.policy.policy_id, name);
        }
        Ok(Next::Done(names))
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{PolicyName, PolicyNamesOperation};
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::{PlacementPolicy, PlacementSelector};
    use ulid::Ulid;

    fn realm() -> RealmId {
        RealmId::from_bytes([3u8; 32])
    }

    fn policy(
        seed: u8,
        owner: Option<Ulid>,
    ) -> aruna_core::structs::placement::policy::VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            "eu-residency".to_string(),
            vec![PlacementSelector {
                node_id: None,
                location: Some("eu-west".to_string()),
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        let policy = match owner {
            Some(owner) => policy.owned_by(owner).expect("owner is valid"),
            None => policy,
        };
        aruna_core::structs::placement::policy::VerifiedPolicy::verify(policy)
            .expect("policy verifies")
    }

    #[test]
    fn names_held_refs() {
        // A ref this node holds resolves to its name and owner; one it does not
        // hold is absent rather than an error.
        let owner = Ulid::from_bytes([5u8; 16]);
        let held = policy(1, Some(owner));
        let unknown = policy(2, None);
        let mut operation =
            PolicyNamesOperation::new(realm(), &[held.policy_ref(), unknown.policy_ref()]);
        operation.first().unwrap();

        let document = crate::tests::policy::signed_document(realm(), &held, 1);
        let row = Some(document.to_bytes().expect("document encodes").into());
        let Ok(Next::Done(names)) = operation.next(IoResult::Values(vec![row, None])) else {
            panic!("expected the names");
        };
        assert_eq!(
            names.get(&held.policy().policy_id),
            Some(&PolicyName {
                name: "eu-residency".to_string(),
                owner_group_id: Some(owner),
            })
        );
        assert!(!names.contains_key(&unknown.policy().policy_id));
    }

    #[test]
    fn empty_skips_read() {
        let mut operation = PolicyNamesOperation::new(realm(), &[]);
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(Vec::new()))));
        let names = operation.next(IoResult::Values(Vec::new()));
        assert_eq!(names, Ok(Next::Done(Default::default())));
    }

    #[test]
    fn skips_foreign_realm() {
        let held = policy(1, None);
        let mut operation = PolicyNamesOperation::new(realm(), &[held.policy_ref()]);
        operation.first().unwrap();

        let document =
            crate::tests::policy::signed_document(RealmId::from_bytes([9u8; 32]), &held, 1);
        let row = Some(document.to_bytes().expect("document encodes").into());
        let names = operation.next(IoResult::Values(vec![row]));
        assert_eq!(names, Ok(Next::Done(Default::default())));
    }
}
