//! Display names for policy refs, read from the rows this node already holds.
//!
//! A ref this node does not hold is simply absent from the answer: the lookup
//! is a display convenience for a caller that already holds the refs, never an
//! existence oracle, and it never fetches from a holder.

use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{PlacementPolicyDocument, PlacementPolicyRef, RealmId};
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NamesState {
    Init,
    Read,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct PolicyNamesOperation {
    realm_id: RealmId,
    /// Deduplicated and bounded at construction.
    ids: Vec<Ulid>,
    state: NamesState,
    output: Option<Result<BTreeMap<Ulid, PolicyName>, PolicyNamesError>>,
}

impl PolicyNamesOperation {
    pub fn new(realm_id: RealmId, refs: &[PlacementPolicyRef]) -> Self {
        let mut ids: Vec<Ulid> = refs.iter().map(|policy_ref| policy_ref.policy_id).collect();
        ids.sort_unstable();
        ids.dedup();
        ids.truncate(MAX_NAMED_REFS);
        Self {
            realm_id,
            ids,
            state: NamesState::Init,
            output: None,
        }
    }

    fn finish(&mut self, result: Result<BTreeMap<Ulid, PolicyName>, PolicyNamesError>) -> Effects {
        self.state = match result.is_ok() {
            true => NamesState::Finish,
            false => NamesState::Error,
        };
        self.output = Some(result);
        smallvec![]
    }
}

impl Operation for PolicyNamesOperation {
    type Output = BTreeMap<Ulid, PolicyName>;
    type Error = PolicyNamesError;

    fn start(&mut self) -> Effects {
        if self.ids.is_empty() {
            return self.finish(Ok(BTreeMap::new()));
        }
        self.state = NamesState::Read;
        let reads = self
            .ids
            .iter()
            .map(|policy_id| {
                let target = DocumentSyncTarget::PlacementPolicy {
                    policy_id: *policy_id,
                };
                (target.storage_keyspace().to_string(), target.storage_key())
            })
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            NamesState::Init => self.start(),
            NamesState::Read => {
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    return self.finish(Err(PolicyNamesError::InvalidEvent));
                };
                let mut names = BTreeMap::new();
                for (_, value) in values.into_iter().flat_map(|(key, value)| value.map(|v| (key, v)))
                {
                    let document = match PlacementPolicyDocument::from_bytes(value.as_ref()) {
                        Ok(document) => document,
                        Err(error) => return self.finish(Err(error.into())),
                    };
                    // A row stored for another realm names nothing here.
                    if document.realm_id != self.realm_id {
                        continue;
                    }
                    names.insert(
                        document.policy.policy_id,
                        PolicyName {
                            name: document.policy.name.clone(),
                            owner_group_id: document.policy.owner_group_id,
                        },
                    );
                }
                self.finish(Ok(names))
            }
            NamesState::Finish | NamesState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, NamesState::Finish | NamesState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PolicyNamesError::InvalidEvent))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{PolicyName, PolicyNamesOperation};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{PlacementPolicy, PlacementPolicyRef, PlacementSelector, RealmId};
    use ulid::Ulid;

    fn realm() -> RealmId {
        RealmId::from_bytes([3u8; 32])
    }

    fn policy(seed: u8, owner: Option<Ulid>) -> aruna_core::structs::VerifiedPolicy {
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
        aruna_core::structs::VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    #[test]
    fn names_held_refs() {
        // A ref this node holds resolves to its name and owner; one it does not
        // hold is absent rather than an error.
        let owner = Ulid::from_bytes([5u8; 16]);
        let held = policy(1, Some(owner));
        let unknown = policy(2, None);
        let mut operation = PolicyNamesOperation::new(
            realm(),
            &[held.policy_ref(), unknown.policy_ref()],
        );
        operation.start();

        let document = crate::placement_policy::tests::signed_document(realm(), &held, 1);
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    b"a".to_vec().into(),
                    Some(document.to_bytes().expect("document encodes").into()),
                ),
                (b"b".to_vec().into(), None),
            ],
        }));

        let names = operation.finalize().expect("lookup finishes");
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
    fn empty_needs_no_read() {
        let mut operation = PolicyNamesOperation::new(realm(), &[]);
        assert!(operation.start().is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(Default::default()));
    }

    #[test]
    fn skips_foreign_realm() {
        let held = policy(1, None);
        let mut operation = PolicyNamesOperation::new(realm(), &[held.policy_ref()]);
        operation.start();

        let document =
            crate::placement_policy::tests::signed_document(RealmId::from_bytes([9u8; 32]), &held, 1);
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(
                b"a".to_vec().into(),
                Some(document.to_bytes().expect("document encodes").into()),
            )],
        }));

        assert!(operation.finalize().expect("lookup finishes").is_empty());
    }
}
