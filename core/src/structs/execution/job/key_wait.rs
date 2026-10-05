//! Bucket keys a parked job waits for, and the rows that wake it after unlock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::structs::storage::encryption::BucketKeyRef;

/// Prefix of the per-job wait list.
const JOB_WAIT_PREFIX: u8 = b'j';
/// Prefix of the wake index: key reference, then job id.
const KEY_WAIT_PREFIX: u8 = b'k';

/// One bucket key generation a job needs; `node_id` is the node that holds the content.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyWait {
    pub node_id: NodeId,
    pub bucket: String,
    pub group_id: Option<GroupId>,
    pub key: BucketKeyRef,
}

impl KeyWait {
    /// Public status entry: `{node_id, bucket, group_id?}`.
    pub fn to_public_json(&self) -> serde_json::Value {
        let mut value = serde_json::json!({
            "node_id": self.node_id.to_string(),
            "bucket": self.bucket,
        });
        if let Some(group_id) = self.group_id {
            value["group_id"] = serde_json::Value::String(group_id.to_string());
        }
        value
    }
}

/// Key of the wait list of one job.
pub fn job_wait_key(job_id: JobId) -> Key {
    let mut bytes = Vec::with_capacity(17);
    bytes.push(JOB_WAIT_PREFIX);
    bytes.extend_from_slice(&job_id.to_bytes());
    ByteView::from(bytes)
}

/// Prefix of every wake row of one bucket; generations follow in order.
pub fn bucket_wait_prefix(bucket_id: Ulid) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(17);
    bytes.push(KEY_WAIT_PREFIX);
    bytes.extend_from_slice(&bucket_id.to_bytes());
    bytes
}

/// Wake row of one job waiting for one key generation.
pub fn key_wait_key(key: BucketKeyRef, job_id: JobId) -> Key {
    let mut bytes = Vec::with_capacity(41);
    bytes.push(KEY_WAIT_PREFIX);
    bytes.extend_from_slice(&key.key());
    bytes.extend_from_slice(&job_id.to_bytes());
    ByteView::from(bytes)
}

/// The job id at the end of a wake row key.
pub fn parse_wait_key(key: &[u8]) -> Option<(BucketKeyRef, JobId)> {
    if key.len() != 41 || key[0] != KEY_WAIT_PREFIX {
        return None;
    }
    let bucket_id = Ulid::from_bytes(key[1..17].try_into().ok()?);
    let generation = u64::from_be_bytes(key[17..25].try_into().ok()?);
    let job_id = JobId::from_bytes(key[25..41].try_into().ok()?);
    Some((BucketKeyRef::new(bucket_id, generation), job_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wait_key_roundtrips() {
        let key = BucketKeyRef::new(Ulid::from_parts(7, 9), 3);
        let job_id = JobId::from_bytes([4; 16]);
        let row = key_wait_key(key, job_id);
        assert!(row.starts_with(bucket_wait_prefix(key.bucket_id)));
        assert_eq!(parse_wait_key(&row), Some((key, job_id)));
        assert_eq!(parse_wait_key(&job_wait_key(job_id)), None);
    }
}
