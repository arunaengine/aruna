//! Publishes a distributed bucket's versions from the coordinator of their range to the members
//! that hold it. A queued job names only the member; what it publishes comes from the record
//! this node holds when it sends, so an ended coordinatorship publishes nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionSnapshot, DistributionView, Publication,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// The member of a distributed bucket a queued job publishes to.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct MemberTarget {
    pub set_id: Ulid,
    pub member_id: Ulid,
}

/// This node's record of a set, if it holds one.
pub(crate) async fn read_record(
    context: &DriverContext,
    set_id: Ulid,
) -> Result<Option<DistributionView>, String> {
    let read = StorageEffect::Read {
        key_space: DISTRIBUTION_KEYSPACE.to_string(),
        key: set_id.to_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| DistributionView::from_bytes(value.as_ref()))
            .transpose()
            .map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected record answer {other:?}")),
    }
}

/// The binding of a local bucket and this node's record of its set; `None` for an ordinary
/// bucket, a missing bucket or a record that has not arrived.
pub(crate) async fn bound_record(
    context: &DriverContext,
    bucket: &str,
) -> Result<Option<(DistributionBinding, DistributionSnapshot)>, String> {
    let binding = match drive(GetBucketOperation::new(bucket.to_string()), context).await {
        Ok(info) => info.distribution,
        Err(GetBucketError::NotFound) => None,
        Err(error) => return Err(error.to_string()),
    };
    let Some(binding) = binding else {
        return Ok(None);
    };
    let Some(view) = read_record(context, binding.set_id).await? else {
        return Ok(None);
    };
    Ok(Some((binding, view.head.snapshot)))
}

/// What this member publishes of `key` to `target` now, and the target's node: `None` once
/// this member does not coordinate the key's range, or the target no longer holds it.
pub(crate) fn member_publication(
    binding: &DistributionBinding,
    snapshot: &DistributionSnapshot,
    key: &str,
    target: &MemberTarget,
) -> Option<(Publication, NodeId)> {
    let local = snapshot.member(binding.member_id)?;
    if binding.set_id != target.set_id || local.incarnation != Some(binding.incarnation) {
        return None;
    }
    let (range, deliveries) = snapshot.key_deliveries(binding.member_id, key)?;
    let delivery = deliveries
        .iter()
        .find(|delivery| delivery.member_id == target.member_id)?;
    Some((snapshot.publication(range, delivery), delivery.node_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_record::{binding, member, replicated};

    #[test]
    fn publishes_while_coordinating() {
        let (snapshot, _) = replicated(true, 1);
        let (coordinator, replica) = (binding(&snapshot, 1, 5), binding(&snapshot, 2, 5));
        let target = MemberTarget {
            set_id: snapshot.set_id(),
            member_id: member(2),
        };
        let (publication, node_id) =
            member_publication(&coordinator, &snapshot, "a", &target).unwrap();
        assert!(publication.bytes, "a copy replica keeps the bytes");
        assert_eq!(node_id, snapshot.member(member(2)).unwrap().node_id);
        assert!(
            member_publication(&replica, &snapshot, "a", &target).is_none(),
            "a replica publishes nothing"
        );
        assert!(member_publication(&coordinator, &snapshot, "x", &target).is_none());
        let mut replaced = coordinator.clone();
        replaced.incarnation += 1;
        assert!(member_publication(&replaced, &snapshot, "a", &target).is_none());
    }
}
