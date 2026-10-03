//! Re-encodes blob versions from before materialized versions named their encoding class.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{BackendRef, BlobVersion, BlobVersionState};
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::{NodeId, UserId};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::SystemTime;

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub(super) struct LegacyVersion {
    created_at: SystemTime,
    created_by: UserId,
    state: State,
    metadata: HashMap<String, String>,
    published_by: Option<NodeId>,
    placement_policies: Vec<PlacementPolicyRef>,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
enum State {
    Materialized {
        blob_hash: [u8; 32],
        backend: BackendRef,
        source: Option<VersionSourceBinding>,
    },
    Reference {
        source: VersionSourceBinding,
        cached_metadata: SourceMetadata,
        last_refresh: SystemTime,
        advance_count: u16,
    },
    Deleted,
}

impl From<LegacyVersion> for BlobVersion {
    fn from(old: LegacyVersion) -> Self {
        let state = match old.state {
            State::Materialized {
                blob_hash,
                backend,
                source,
            } => BlobVersionState::Materialized {
                blob_hash,
                backend,
                encoding: EncodingClass::Raw,
                source,
            },
            State::Reference {
                source,
                cached_metadata,
                last_refresh,
                advance_count,
            } => BlobVersionState::Reference {
                source,
                cached_metadata,
                last_refresh,
                advance_count,
            },
            State::Deleted => BlobVersionState::Deleted,
        };
        BlobVersion {
            created_at: old.created_at,
            created_by: old.created_by,
            state,
            metadata: old.metadata,
            published_by: old.published_by,
            placement_policies: old.placement_policies,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, write};
    use super::*;
    use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
    use aruna_core::structs::identity::realm::RealmId;

    #[test]
    fn adds_raw_encoding() {
        let old = LegacyVersion {
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::nil(RealmId([1; 32])),
            state: State::Materialized {
                blob_hash: [4; 32],
                backend: BackendRef::node_default(),
                source: None,
            },
            metadata: HashMap::from([("a".to_string(), "b".to_string())]),
            published_by: None,
            placement_policies: Vec::new(),
        };
        let deleted = BlobVersion::deleted(SystemTime::UNIX_EPOCH, UserId::default());
        let rows = vec![
            (
                b"old".as_slice(),
                postcard::to_allocvec(&old).expect("encodes"),
            ),
            (b"new".as_slice(), deleted.to_bytes().expect("encodes")),
        ];
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        write(&path, BLOB_VERSIONS_KEYSPACE, rows);

        let output = migrate_output(database).expect("migration runs");

        assert_eq!(output.versions_rewritten, 1);
        let rows = read(&path, BLOB_VERSIONS_KEYSPACE);
        let version = BlobVersion::from_bytes(&rows[b"old".as_slice()]).expect("decodes");
        assert_eq!(version.metadata["a"], "b");
        let key = version.location_key().expect("materialized");
        assert_eq!(key.encoding, EncodingClass::Raw);
        assert_eq!(key.blake3_hash, [4; 32]);
        let again = migrate_output(database).expect("migration repeats");
        assert_eq!(again.versions_rewritten, 0);
    }
}
