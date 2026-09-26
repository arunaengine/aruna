//! Reduces a document's Git records to one ref, LFS and lock state, equal on every holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::git::{
    GitChange, GitCheckpoint, GitRecord, LfsLock, StoredObject, ZERO_OID, refs_clash,
};
use std::collections::{BTreeMap, BTreeSet};
use ulid::Ulid;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GitState {
    pub refs: BTreeMap<String, String>,
    pub lfs: BTreeMap<String, StoredObject>,
    pub locks: BTreeMap<String, LfsLock>,
    /// Claims that lost to a held lock; they are decided when that lock's unlock is known.
    pub waiting: Vec<LfsLock>,
    pub revision: Option<Ulid>,
    /// The graph digest of the newest applied snapshot.
    pub digest: Option<[u8; 32]>,
    pub packs: Vec<StoredObject>,
    /// Commits nodes made themselves; only their Aruna trailers are trusted.
    pub made: BTreeSet<String>,
    /// The newest checkpoint the state starts from.
    pub checkpoint: Option<Ulid>,
    /// Records applied on top of that checkpoint; the next checkpoint covers exactly these.
    pub applied: Vec<Ulid>,
    /// Packs and LFS objects those records added.
    pub new_packs: Vec<StoredObject>,
    pub new_lfs: Vec<StoredObject>,
    pub new_made: Vec<String>,
}

/// Known answers to "is the first commit an ancestor of the second".
pub type Ancestry = BTreeMap<(String, String), bool>;

/// The newest checkpoint whose previous checkpoints are all present, that chain from newest
/// to oldest, and every record id the chain covers, including the checkpoints themselves.
pub fn chain(records: &[GitRecord]) -> (Vec<(Ulid, &GitCheckpoint)>, BTreeSet<Ulid>) {
    let checkpoints: BTreeMap<Ulid, &GitCheckpoint> = records
        .iter()
        .filter_map(|record| match &record.change {
            GitChange::Checkpoint(checkpoint) => Some((record.event_id, checkpoint.as_ref())),
            _ => None,
        })
        .collect();
    'newest: for &newest in checkpoints.keys().rev() {
        let mut chain = Vec::new();
        let mut next = Some(newest);
        while let Some(id) = next {
            let Some(checkpoint) = checkpoints.get(&id) else {
                continue 'newest;
            };
            chain.push((id, *checkpoint));
            next = checkpoint.previous;
        }
        let covered = chain
            .iter()
            .flat_map(|(id, checkpoint)| {
                checkpoint
                    .covered
                    .iter()
                    .copied()
                    .chain(std::iter::once(*id))
            })
            .collect();
        return (chain, covered);
    }
    (Vec::new(), BTreeSet::new())
}

/// Folds records in id order from the newest checkpoint chain. A ref update applies when its
/// old value matches or it fast-forwards a branch; otherwise its commit is kept as a conflict
/// ref. The state is final only when no commit pairs are returned; answer them and reduce again.
pub fn reduce(records: &[GitRecord], ancestry: &Ancestry) -> (GitState, Vec<(String, String)>) {
    let mut ordered: Vec<_> = records.iter().collect();
    ordered.sort_by_key(|record| record.event_id);
    let mut state = GitState::default();
    let (chain, skip) = chain(records);
    if let Some((id, newest)) = chain.first() {
        state.checkpoint = Some(*id);
        state.refs = newest.refs.iter().cloned().collect();
        state.locks = newest
            .locks
            .iter()
            .map(|lock| (lock.path.clone(), lock.clone()))
            .collect();
        state.waiting = newest.waiting.clone();
        state.revision = newest.revision;
        state.digest = newest.digest;
        for (_, checkpoint) in chain.iter().rev() {
            state.made.extend(checkpoint.made.iter().cloned());
            for pack in &checkpoint.packs {
                if !state.packs.contains(pack) {
                    state.packs.push(pack.clone());
                }
            }
            for object in &checkpoint.lfs {
                state
                    .lfs
                    .entry(object.sha256.clone())
                    .or_insert_with(|| object.clone());
            }
        }
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
                digest,
                made,
            } => {
                // Objects always stay: other records may build on a losing record's commits.
                if let Some(pack) = pack.as_deref()
                    && !state.packs.contains(pack)
                {
                    state.packs.push(pack.clone());
                    state.new_packs.push(pack.clone());
                }
                // The first recorded location stays; later records name copies of it.
                for object in lfs {
                    if !state.lfs.contains_key(&object.sha256) {
                        state.lfs.insert(object.sha256.clone(), object.clone());
                        state.new_lfs.push(object.clone());
                    }
                }
                for commit in made {
                    if state.made.insert(commit.clone()) {
                        state.new_made.push(commit.clone());
                    }
                }
                let snapshot = revision.is_some();
                if snapshot && digest.is_some() && *digest == state.digest {
                    continue;
                }
                let mut applied = false;
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
                    // Git cannot store `a` next to `a/b`; a clashing new name loses.
                    let clash = current.is_none()
                        && state.refs.keys().any(|name| refs_clash(name, &update.name));
                    if (matches || forward) && !clash {
                        applied = true;
                        if update.new == ZERO_OID {
                            state.refs.remove(&update.name);
                        } else {
                            state.refs.insert(update.name.clone(), update.new.clone());
                        }
                    } else if update.new != ZERO_OID && !snapshot {
                        let name = update.name.trim_start_matches("refs/");
                        state.refs.insert(
                            format!("refs/conflicts/{name}/{}", record.event_id),
                            update.new.clone(),
                        );
                    }
                }
                // An outdated snapshot is dropped; the server makes a current one.
                if snapshot && (applied || refs.is_empty()) {
                    state.revision = *revision;
                    state.digest = *digest;
                }
            }
            GitChange::Lock { id, path } => {
                let claim = LfsLock {
                    id: *id,
                    path: path.clone(),
                    user_id: record.user_id,
                    locked_at_ms: record.occurred_at_ms,
                    claim: record.event_id,
                };
                // The earliest claim holds, also when it arrives late; the other one waits.
                match state.locks.get(path) {
                    Some(held) if held.claim < claim.claim => state.waiting.push(claim),
                    Some(held) => {
                        let held = held.clone();
                        state.waiting.push(held);
                        state.locks.insert(path.clone(), claim);
                    }
                    None => {
                        state.locks.insert(path.clone(), claim);
                    }
                }
            }
            GitChange::Unlock { id } => {
                state.waiting.retain(|waiting| waiting.id != *id);
                let released = state
                    .locks
                    .iter()
                    .find(|(_, lock)| lock.id == *id)
                    .map(|(path, _)| path.clone());
                if let Some(path) = released {
                    state.locks.remove(&path);
                    // Claims made while the lock was held were refused; the first later one holds.
                    let unlocked = record.event_id;
                    state
                        .waiting
                        .retain(|waiting| waiting.path != path || waiting.claim > unlocked);
                    let next = state
                        .waiting
                        .iter()
                        .enumerate()
                        .filter(|(_, waiting)| waiting.path == path)
                        .min_by_key(|(_, waiting)| waiting.claim)
                        .map(|(index, _)| index);
                    if let Some(index) = next {
                        let lock = state.waiting.remove(index);
                        state.locks.insert(path, lock);
                    }
                }
            }
            // Only the newest complete chain seeds the state; other checkpoints' records apply.
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
    use aruna_core::git::RefUpdate;
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
                digest: revision.map(|revision| [revision as u8; 32]),
                made: Vec::new(),
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
            // The same graph captured by another holder is skipped, not kept as a conflict.
            update(20, "refs/heads/aruna", ZERO_OID, &oid('c'), Some(5)),
            update(30, "refs/heads/aruna", &oid('b'), &oid('d'), Some(6)),
            // An outdated snapshot is dropped; its pack still stays.
            update(40, "refs/heads/aruna", &oid('b'), &oid('e'), Some(7)),
        ];
        let ancestry = Ancestry::from([((oid('d'), oid('e')), false)]);
        let state = done(&records, &ancestry);
        assert_eq!(state.refs.get("refs/heads/aruna"), Some(&oid('d')));
        assert_eq!(state.refs.len(), 1);
        assert_eq!(state.revision, Some(Ulid::from(6)));
        assert_eq!(state.digest, Some([6; 32]));
    }

    #[test]
    fn clashing_names_conflict() {
        let records = [
            update(10, "refs/heads/draft", ZERO_OID, &oid('b'), None),
            update(20, "refs/heads/draft/sub", ZERO_OID, &oid('c'), None),
        ];
        let state = done(&records, &Ancestry::new());
        assert_eq!(state.refs.get("refs/heads/draft"), Some(&oid('b')));
        assert!(!state.refs.contains_key("refs/heads/draft/sub"));
        let kept = format!("refs/conflicts/heads/draft/sub/{}", Ulid::from(20));
        assert_eq!(state.refs.get(&kept), Some(&oid('c')));
    }

    #[test]
    fn late_lock_wins() {
        let lock = |id: u128, user: u128, lock: u128| {
            record(
                id,
                user,
                GitChange::Lock {
                    id: Ulid::from(lock),
                    path: "a.bin".into(),
                },
            )
        };
        let mut late = checkpoint(40, None, &[], &[30]);
        if let GitChange::Checkpoint(checkpoint) = &mut late.change {
            checkpoint.locks = vec![LfsLock {
                id: Ulid::from(3),
                path: "a.bin".into(),
                user_id: UserId::new(Ulid::from(2), RealmId([1; 32])),
                locked_at_ms: 0,
                claim: Ulid::from(30),
            }];
        }
        let state = done(&[late.clone(), lock(20, 1, 2)], &Ancestry::new());
        assert_eq!(state.locks["a.bin"].id, Ulid::from(2));
        assert_eq!(state.locks["a.bin"].claim, Ulid::from(20));
        // Released before the checkpoint's claim was made, the late lock hands over to it.
        let released = record(25, 1, GitChange::Unlock { id: Ulid::from(2) });
        let state = done(&[late.clone(), lock(20, 1, 2), released], &Ancestry::new());
        assert_eq!(state.locks["a.bin"].id, Ulid::from(3));
        // Released after it, the checkpoint's claim was refused all along.
        let released = record(35, 1, GitChange::Unlock { id: Ulid::from(2) });
        let state = done(&[late, lock(20, 1, 2), released], &Ancestry::new());
        assert!(state.locks.is_empty());
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
                previous: None,
                packs: vec![pack.clone()],
                made: Vec::new(),
                digest: None,
                refs: vec![("refs/heads/main".into(), oid('c'))],
                lfs: Vec::new(),
                locks: Vec::new(),
                waiting: Vec::new(),
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
        assert_eq!(state.checkpoint, Some(Ulid::from(50)));
        assert_eq!(state.applied, vec![Ulid::from(30), Ulid::from(60)]);
    }

    fn checkpoint(
        id: u128,
        previous: Option<u128>,
        refs: &[(&str, char)],
        covered: &[u128],
    ) -> GitRecord {
        record(
            id,
            1,
            GitChange::Checkpoint(Box::new(GitCheckpoint {
                previous: previous.map(Ulid::from),
                packs: Vec::new(),
                made: Vec::new(),
                digest: None,
                refs: refs
                    .iter()
                    .map(|(name, seed)| (name.to_string(), oid(*seed)))
                    .collect(),
                lfs: Vec::new(),
                locks: Vec::new(),
                waiting: Vec::new(),
                revision: None,
                covered: covered.iter().copied().map(Ulid::from).collect(),
            })),
        )
    }

    #[test]
    fn checkpoint_chains() {
        let records = [
            update(10, "refs/heads/main", ZERO_OID, &oid('b'), None),
            checkpoint(20, None, &[("refs/heads/main", 'b')], &[10]),
            update(30, "refs/heads/main", &oid('b'), &oid('c'), None),
            // A holder that had not seen 30 checkpoints next to one that had.
            update(35, "refs/heads/side", ZERO_OID, &oid('s'), None),
            checkpoint(40, Some(20), &[("refs/heads/main", 'c')], &[30]),
            checkpoint(
                45,
                Some(20),
                &[("refs/heads/main", 'b'), ("refs/heads/side", 's')],
                &[35],
            ),
            update(50, "refs/heads/main", &oid('c'), &oid('d'), None),
        ];
        let state = done(&records, &Ancestry::new());
        // The newest chain (45 -> 20) seeds; what 40 alone covered applies on top.
        assert_eq!(state.checkpoint, Some(Ulid::from(45)));
        assert_eq!(state.refs.get("refs/heads/main"), Some(&oid('d')));
        assert_eq!(state.refs.get("refs/heads/side"), Some(&oid('s')));
        assert_eq!(
            state.applied,
            vec![Ulid::from(30), Ulid::from(40), Ulid::from(50)]
        );
        // A chain with a missing previous checkpoint is not used.
        let partial = [
            checkpoint(60, Some(55), &[("refs/heads/main", 'x')], &[50]),
            records[0].clone(),
        ];
        assert_eq!(done(&partial, &Ancestry::new()).checkpoint, None);
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
