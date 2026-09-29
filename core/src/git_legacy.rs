//! Git record encodings from before packs moved into Fjall, converted on read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::git::{GitChange, GitCheckpoint, GitPack, GitRecord, LfsLock, RefUpdate, StoredObject};
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::record::PlacementRef;
use crate::structs::storage::dataset_location::DatasetLocation;
use crate::types::GroupId;
use crate::{NodeId, UserId};
use serde::Deserialize;
use ulid::Ulid;

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct Record {
    event_id: Ulid,
    realm_id: RealmId,
    group_id: GroupId,
    document_id: Ulid,
    placement: PlacementRef,
    user_id: UserId,
    node_id: NodeId,
    occurred_at_ms: u64,
    change: Change,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
enum Change {
    Objects {
        pack: Option<Box<StoredObject>>,
        refs: Vec<RefUpdate>,
        lfs: Vec<StoredObject>,
        revision: Option<Ulid>,
        digest: Option<[u8; 32]>,
        made: Vec<String>,
    },
    Lock {
        id: Ulid,
        path: String,
    },
    Unlock {
        id: Ulid,
    },
    Checkpoint(Box<Checkpoint>),
    Location(DatasetLocation),
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct Checkpoint {
    previous: Option<Ulid>,
    packs: Vec<StoredObject>,
    made: Vec<String>,
    digest: Option<[u8; 32]>,
    refs: Vec<(String, String)>,
    lfs: Vec<StoredObject>,
    locks: Vec<LfsLock>,
    waiting: Vec<LfsLock>,
    released: Vec<(LfsLock, Ulid)>,
    revision: Option<Ulid>,
    covered: Vec<Ulid>,
}

fn pack(object: &StoredObject) -> GitPack {
    GitPack {
        sha256: object.sha256.clone(),
        size: object.size,
    }
}

/// The current form of an old record, if the bytes are exactly one valid old record.
pub(crate) fn decode(bytes: &[u8]) -> Option<GitRecord> {
    let Ok((old, [])) = postcard::take_from_bytes::<Record>(bytes) else {
        return None;
    };
    let change = match old.change {
        Change::Objects {
            pack: object,
            refs,
            lfs,
            revision,
            digest,
            made,
        } => GitChange::Objects {
            pack: object.as_deref().map(pack),
            refs,
            lfs,
            revision,
            digest,
            made,
        },
        Change::Lock { id, path } => GitChange::Lock { id, path },
        Change::Unlock { id } => GitChange::Unlock { id },
        Change::Checkpoint(old) => GitChange::Checkpoint(Box::new(GitCheckpoint {
            previous: old.previous,
            packs: old.packs.iter().map(pack).collect(),
            made: old.made,
            digest: old.digest,
            refs: old.refs,
            lfs: old.lfs,
            locks: old.locks,
            waiting: old.waiting,
            released: old.released,
            revision: old.revision,
            covered: old.covered,
        })),
        Change::Location(location) => GitChange::Location(location),
    };
    let record = GitRecord {
        event_id: old.event_id,
        realm_id: old.realm_id,
        group_id: old.group_id,
        document_id: old.document_id,
        placement: old.placement,
        user_id: old.user_id,
        node_id: old.node_id,
        occurred_at_ms: old.occurred_at_ms,
        change,
    };
    record.validate().then_some(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_old_packs() {
        let realm_id = RealmId([1; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let object = StoredObject {
            node_id,
            group_id: None,
            bucket: "datasets-x".into(),
            key: "git-packs/x.pack".into(),
            version_id: Ulid::from(1),
            size: 42,
            sha256: "ab".repeat(32),
            blake3: [3; 32],
        };
        let old = Record {
            event_id: Ulid::from(5),
            realm_id,
            group_id: Ulid::from(6),
            document_id: Ulid::from(7),
            placement: PlacementRef::NIL,
            user_id: UserId::nil(realm_id),
            node_id,
            occurred_at_ms: 1,
            change: Change::Objects {
                pack: Some(Box::new(object)),
                refs: vec![RefUpdate {
                    name: "refs/heads/main".into(),
                    old: crate::git::ZERO_OID.into(),
                    new: "c".repeat(40),
                }],
                lfs: Vec::new(),
                revision: None,
                digest: None,
                made: Vec::new(),
            },
        };
        let bytes = postcard::to_allocvec(&old).expect("old record encodes");
        let record = GitRecord::decode(&bytes).expect("old record converts");
        let GitChange::Objects { pack, .. } = &record.change else {
            panic!("objects stay objects");
        };
        let expected = GitPack {
            sha256: "ab".repeat(32),
            size: 42,
        };
        assert_eq!(pack.as_ref(), Some(&expected));
        let current = postcard::to_allocvec(&record).expect("record encodes");
        assert_eq!(GitRecord::decode(&current), Some(record));
    }
}
