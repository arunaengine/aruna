//! Reduces a document's Git records to one ref, LFS and lock state, equal on every holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::git::{GitChange, GitRecord, LfsLock, StoredObject, ZERO_OID};
use std::collections::BTreeMap;
use ulid::Ulid;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GitState {
    pub refs: BTreeMap<String, String>,
    pub lfs: BTreeMap<String, StoredObject>,
    pub locks: BTreeMap<String, LfsLock>,
    pub revision: Option<Ulid>,
    pub packs: Vec<StoredObject>,
    /// Records this state includes; a checkpoint of it covers exactly these.
    pub applied: Vec<Ulid>,
}

/// Known answers to "is the first commit an ancestor of the second".
pub type Ancestry = BTreeMap<(String, String), bool>;

/// Folds records in id order from the newest checkpoint. A ref update applies when its old
/// value matches or it fast-forwards a branch; otherwise its commit is kept as a conflict ref.
/// The state is final only when no commit pairs are returned; answer them and reduce again.
pub fn reduce(records: &[GitRecord], ancestry: &Ancestry) -> (GitState, Vec<(String, String)>) {
    let mut ordered: Vec<_> = records.iter().collect();
    ordered.sort_by_key(|record| record.event_id);
    let mut state = GitState::default();
    let mut skip = std::collections::BTreeSet::new();
    let checkpoint = ordered
        .iter()
        .rev()
        .find_map(|record| match &record.change {
            GitChange::Checkpoint(checkpoint) => Some((record.event_id, checkpoint)),
            _ => None,
        });
    if let Some((id, checkpoint)) = checkpoint {
        state.refs = checkpoint.refs.iter().cloned().collect();
        state.lfs = checkpoint
            .lfs
            .iter()
            .map(|object| (object.sha256.clone(), object.clone()))
            .collect();
        state.locks = checkpoint
            .locks
            .iter()
            .map(|lock| (lock.path.clone(), lock.clone()))
            .collect();
        state.revision = checkpoint.revision;
        state.packs.push(checkpoint.pack.clone());
        state.applied = checkpoint.covered.clone();
        state.applied.push(id);
        skip.extend(state.applied.iter().copied());
    }
    let mut needs = Vec::new();
    for record in ordered {
        if skip.contains(&record.event_id) {
            continue;
        }
        state.applied.push(record.event_id);
        match &record.change {
            GitChange::Objects {
                pack,
                refs,
                lfs,
                revision,
            } => {
                if let Some(revision) = revision {
                    // An equal or older snapshot lost a race to one already applied.
                    if state.revision.is_some_and(|applied| applied >= *revision) {
                        continue;
                    }
                    state.revision = Some(*revision);
                }
                state.packs.extend(pack.as_deref().cloned());
                // The first recorded location stays; later records name copies of it.
                for object in lfs {
                    state
                        .lfs
                        .entry(object.sha256.clone())
                        .or_insert_with(|| object.clone());
                }
                for update in refs {
                    let current = state.refs.get(&update.name).cloned();
                    let matches = current.as_deref().unwrap_or(ZERO_OID) == update.old;
                    let forward = !matches
                        && update.name.starts_with("refs/heads/")
                        && update.new != ZERO_OID
                        && current.as_ref().is_some_and(|current| {
                            let pair = (current.clone(), update.new.clone());
                            ancestry.get(&pair).copied().unwrap_or_else(|| {
                                needs.push(pair);
                                false
                            })
                        });
                    if matches || forward {
                        if update.new == ZERO_OID {
                            state.refs.remove(&update.name);
                        } else {
                            state.refs.insert(update.name.clone(), update.new.clone());
                        }
                    } else if update.new != ZERO_OID {
                        let name = update.name.trim_start_matches("refs/");
                        state.refs.insert(
                            format!("refs/conflicts/{name}/{}", record.event_id),
                            update.new.clone(),
                        );
                    }
                }
            }
            GitChange::Lock { id, path } => {
                state.locks.entry(path.clone()).or_insert_with(|| LfsLock {
                    id: *id,
                    path: path.clone(),
                    user_id: record.user_id,
                    locked_at_ms: record.occurred_at_ms,
                });
            }
            GitChange::Unlock { id } => state.locks.retain(|_, lock| lock.id != *id),
            // Only the newest checkpoint seeds the state; its records apply individually.
            GitChange::Checkpoint(_) => {}
        }
    }
    needs.sort();
    needs.dedup();
    (state, needs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::git::{GitCheckpoint, RefUpdate};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;

    fn oid(seed: char) -> String {
        seed.to_string().repeat(40)
    }

    fn record(id: u128, user: u128, change: GitChange) -> GitRecord {
        GitRecord {
            event_id: Ulid::from(id),
            realm_id: RealmId([1; 32]),
            group_id: Ulid::from(1),
            document_id: Ulid::from(2),
            placement: PlacementRef::NIL,
            user_id: UserId::new(Ulid::from(user), RealmId([1; 32])),
            node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
            occurred_at_ms: 0,
            change,
        }
    }

    fn update(id: u128, name: &str, old: &str, new: &str, revision: Option<u128>) -> GitRecord {
        record(
            id,
            1,
            GitChange::Objects {
                pack: None,
                refs: vec![RefUpdate {
                    name: name.into(),
                    old: old.into(),
                    new: new.into(),
                }],
                lfs: Vec::new(),
                revision: revision.map(Ulid::from),
            },
        )
    }

    fn done(records: &[GitRecord], ancestry: &Ancestry) -> GitState {
        let (state, needs) = reduce(records, ancestry);
        assert!(needs.is_empty(), "unanswered ancestry {needs:?}");
        state
    }

    #[test]
    fn linear_pushes() {
        let records = [
            update(20, "refs/heads/main", &oid('b'), &oid('c'), None),
            update(10, "refs/heads/main", ZERO_OID, &oid('b'), None),
        ];
        let state = done(&records, &Ancestry::new());
        assert_eq!(state.refs.get("refs/heads/main"), Some(&oid('c')));
        assert_eq!(state.applied, vec![Ulid::from(10), Ulid::from(20)]);
    }

    #[test]
    fn concurrent_conflict() {
        let records = [
            update(10, "refs/heads/main", ZERO_OID, &oid('b'), None),
            update(20, "refs/heads/main", &oid('b'), &oid('c'), None),
            update(30, "refs/heads/main", &oid('b'), &oid('d'), None),
        ];
        let pair = (oid('c'), oid('d'));
        assert_eq!(reduce(&records, &Ancestry::new()).1, vec![pair.clone()]);
        let state = done(&records, &Ancestry::from([(pair.clone(), false)]));
        assert_eq!(state.refs.get("refs/heads/main"), Some(&oid('c')));
        let conflict = format!("refs/conflicts/heads/main/{}", Ulid::from(30));
        assert_eq!(state.refs.get(&conflict), Some(&oid('d')));
        let state = done(&records, &Ancestry::from([(pair, true)]));
        assert_eq!(state.refs.get("refs/heads/main"), Some(&oid('d')));
        assert_eq!(state.refs.len(), 1);
    }

    #[test]
    fn tags_never_forward() {
        let records = [
            update(10, "refs/tags/v1", ZERO_OID, &oid('b'), None),
            update(20, "refs/tags/v1", ZERO_OID, &oid('c'), None),
        ];
        let state = done(&records, &Ancestry::new());
        assert_eq!(state.refs.get("refs/tags/v1"), Some(&oid('b')));
        assert_eq!(state.refs.len(), 2);
    }

    #[test]
    fn duplicate_snapshots() {
        let records = [
            update(10, "refs/heads/aruna", ZERO_OID, &oid('b'), Some(5)),
            update(20, "refs/heads/aruna", ZERO_OID, &oid('c'), Some(5)),
            update(30, "refs/heads/aruna", &oid('b'), &oid('d'), Some(4)),
            update(40, "refs/heads/aruna", &oid('b'), &oid('e'), Some(6)),
        ];
        let state = done(&records, &Ancestry::new());
        assert_eq!(state.refs.get("refs/heads/aruna"), Some(&oid('e')));
        assert_eq!(state.refs.len(), 1);
        assert_eq!(state.revision, Some(Ulid::from(6)));
    }

    #[test]
    fn checkpoint_keeps_late() {
        let pack = StoredObject {
            node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
            group_id: Some(Ulid::from(1)),
            bucket: "arc".into(),
            key: "pack".into(),
            version_id: Ulid::from(1),
            size: 1,
            sha256: "a".repeat(64),
            blake3: [0; 32],
        };
        let checkpoint = record(
            50,
            1,
            GitChange::Checkpoint(Box::new(GitCheckpoint {
                pack: pack.clone(),
                refs: vec![("refs/heads/main".into(), oid('c'))],
                lfs: Vec::new(),
                locks: Vec::new(),
                revision: None,
                covered: vec![Ulid::from(10), Ulid::from(20)],
            })),
        );
        let records = [
            update(10, "refs/heads/main", ZERO_OID, &oid('b'), None),
            update(20, "refs/heads/main", &oid('b'), &oid('c'), None),
            update(30, "refs/heads/feature", ZERO_OID, &oid('f'), None),
            checkpoint,
            update(60, "refs/heads/main", &oid('c'), &oid('d'), None),
        ];
        let state = done(&records, &Ancestry::new());
        assert_eq!(state.refs.get("refs/heads/main"), Some(&oid('d')));
        assert_eq!(state.refs.get("refs/heads/feature"), Some(&oid('f')));
        assert_eq!(state.packs, vec![pack]);
        assert_eq!(state.applied.len(), 5);
    }

    #[test]
    fn earliest_lock_wins() {
        let records = [
            record(
                10,
                1,
                GitChange::Lock {
                    id: Ulid::from(1),
                    path: "a.bin".into(),
                },
            ),
            record(
                20,
                2,
                GitChange::Lock {
                    id: Ulid::from(2),
                    path: "a.bin".into(),
                },
            ),
            record(
                30,
                2,
                GitChange::Lock {
                    id: Ulid::from(3),
                    path: "b.bin".into(),
                },
            ),
            record(40, 2, GitChange::Unlock { id: Ulid::from(3) }),
        ];
        let state = done(&records, &Ancestry::new());
        assert_eq!(state.locks.len(), 1);
        assert_eq!(state.locks["a.bin"].id, Ulid::from(1));
        assert_eq!(state.locks["a.bin"].user_id.user_ulid, Ulid::from(1));
    }
}
