//! Re-encodes bucket records from before buckets carried a compression setting.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{BucketCorsConfiguration, BucketInfo};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::routing::StorageRoutingRule;
use serde::Deserialize;
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub(super) struct LegacyBucket {
    group_id: Ulid,
    created_at: SystemTime,
    created_by: UserId,
    cors_configuration: Option<BucketCorsConfiguration>,
    storage_routing: Vec<StorageRoutingRule>,
    placement_policies: Vec<PlacementPolicyRef>,
    placement_policy_generation: u64,
}

impl From<LegacyBucket> for BucketInfo {
    fn from(old: LegacyBucket) -> Self {
        BucketInfo {
            group_id: old.group_id,
            created_at: old.created_at,
            created_by: old.created_by,
            cors_configuration: old.cors_configuration,
            storage_routing: old.storage_routing,
            placement_policies: old.placement_policies,
            placement_policy_generation: old.placement_policy_generation,
            compression: Compression::Off,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, write};
    use super::*;
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;

    #[test]
    fn adds_compression_off() {
        let old = LegacyBucket {
            group_id: Ulid::from(4),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 3,
        };
        let bytes = postcard::to_allocvec(&old).expect("old record encodes");
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        write(&path, S3_BUCKET_KEYSPACE, vec![(b"data", bytes)]);

        let output = migrate_output(database).expect("migration runs");

        assert_eq!(output.buckets_rewritten, 1);
        let rows = read(&path, S3_BUCKET_KEYSPACE);
        let bucket = BucketInfo::from_bytes(&rows[b"data".as_slice()]).expect("decodes");
        assert_eq!(bucket.compression, Compression::Off);
        assert_eq!(bucket.placement_policy_generation, 3);
        let again = migrate_output(database).expect("migration repeats");
        assert_eq!(again.buckets_rewritten, 0);
    }
}
