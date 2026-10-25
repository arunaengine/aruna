// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_record::{binding, record, secret};

#[test]
fn coordinator_stamps_epoch() {
    let (snapshot, bytes) = record(true);
    let own = binding(&snapshot, 1, 5);
    assert_eq!(
        write_epoch(&own, "a", Some(&bytes), None),
        Ok(OriginalEpoch(1))
    );
    let Err(GuardError::NotCoordinator {
        node_id, range_id, ..
    }) = write_epoch(&own, "x", Some(&bytes), None)
    else {
        panic!("the upper range belongs to member 2");
    };
    assert_eq!(node_id, secret(2).public());
    assert_eq!(range_id, snapshot.config.ranges[1].range_id);
}

#[test]
fn unready_sets_refuse() {
    let (snapshot, bytes) = record(false);
    let own = binding(&snapshot, 1, 5);
    assert_eq!(
        write_epoch(&own, "a", Some(&bytes), None),
        Err(GuardError::Route(RouteError::Pending(snapshot.set_id())))
    );
    assert_eq!(
        write_epoch(&own, "a", None, None),
        Err(GuardError::RecordMissing(snapshot.set_id()))
    );
}

#[test]
fn replaced_bucket_refused() {
    let (snapshot, bytes) = record(true);
    // A recreated local bucket of the name has another incarnation than the admitted one.
    let replaced = binding(&snapshot, 1, 6);
    assert_eq!(
        write_epoch(&replaced, "a", Some(&bytes), None),
        Err(GuardError::Unbound(snapshot.set_id()))
    );
    let stranger = binding(&snapshot, 9, 5);
    assert_eq!(
        write_epoch(&stranger, "a", Some(&bytes), None),
        Err(GuardError::Route(RouteError::NotMember(snapshot.set_id())))
    );
}

#[test]
fn fence_refuses_writes() {
    use aruna_core::structs::storage::distribution::fence::{LocalFence, RangeFence};
    let (snapshot, bytes) = record(true);
    let own = binding(&snapshot, 1, 5);
    let range_id = snapshot.config.ranges[0].range_id;
    let mut fence = LocalFence::default();
    fence.fence_range(RangeFence {
        range_id,
        transfer_id: Ulid::from_bytes([3; 16]),
        epoch: 1,
        since_ms: 0,
        uploads_open: false,
    });
    let fence = fence.to_bytes().unwrap();
    // The record still names this member, but its own fence takes precedence.
    assert_eq!(
        write_epoch(&own, "a", Some(&bytes), Some(&fence)),
        Err(GuardError::Route(RouteError::Moving(range_id)))
    );
}

#[test]
fn transaction_rechecks_authority() {
    use aruna_core::structs::storage::distribution::fence::LocalFence;
    let (snapshot, bytes) = record(true);
    let own = binding(&snapshot, 1, 5);
    let authority = write_authority(&own, "a", Some(&bytes), None).unwrap();
    assert_eq!(check_authority(&authority, "a", Some(&bytes), None), Ok(()));
    // A deletion fence set after the write was routed refuses its publication.
    let deleting = LocalFence {
        deletion: Some([1; 32]),
        ..LocalFence::default()
    };
    let deleting = deleting.to_bytes().unwrap();
    assert_eq!(
        check_authority(&authority, "a", Some(&bytes), Some(&deleting)),
        Err(GuardError::Route(RouteError::Deleting(snapshot.set_id())))
    );
    // A write routed under another epoch is stale once the record moved on.
    let stale = WriteAuthority {
        epoch: OriginalEpoch(2),
        ..authority
    };
    assert_eq!(
        check_authority(&stale, "a", Some(&bytes), None),
        Err(GuardError::Route(RouteError::StaleRange(stale.range_id)))
    );
}

#[test]
fn completion_passes_grace() {
    use crate::distribution::test_record::authority_rows;
    use aruna_core::structs::storage::distribution::fence::{LocalFence, RangeFence};
    let (snapshot, bytes) = record(true);
    let own = binding(&snapshot, 1, 5);
    let range_id = snapshot.config.ranges[0].range_id;
    let mut fence = LocalFence::default();
    fence.fence_range(RangeFence {
        range_id,
        transfer_id: Ulid::from_bytes([3; 16]),
        epoch: 1,
        since_ms: 0,
        uploads_open: true,
    });
    let open = fence.to_bytes().unwrap();
    let rows = || authority_rows(Some(bytes.clone()), Some(open.clone()));
    let moving = || GuardError::Route(RouteError::Moving(range_id));
    // New writes stop at the fence; completions of open uploads pass until the grace ends.
    assert_eq!(record_authority(&own, "a", rows()), Err(moving()));
    let authority = record_completion(&own, "a", rows()).unwrap();
    assert!(authority.completion);
    assert_eq!(
        check_authority(&authority, "a", Some(&bytes), Some(&open)),
        Ok(())
    );
    fence.close_uploads(range_id);
    let closed = fence.to_bytes().unwrap();
    assert_eq!(
        check_authority(&authority, "a", Some(&bytes), Some(&closed)),
        Err(moving())
    );
}

#[test]
fn moved_keys_skipped() {
    // Jobs skip keys of another coordinator or a moving range, and retry a pending set.
    let (snapshot, bytes) = record(true);
    let own = binding(&snapshot, 1, 5);
    let forwarded = write_epoch(&own, "x", Some(&bytes), None).unwrap_err();
    assert!(forwarded.moved_away());
    let range_id = snapshot.config.ranges[0].range_id;
    assert!(GuardError::Route(RouteError::Moving(range_id)).moved_away());
    assert!(!GuardError::Route(RouteError::Pending(snapshot.set_id())).moved_away());
    assert!(!GuardError::RecordMissing(snapshot.set_id()).moved_away());
}

#[test]
fn reads_held_only() {
    let (snapshot, bytes) = record(true);
    let own = binding(&snapshot, 1, 5);
    assert_eq!(
        read_target(&own, "a", false, Some(&bytes)),
        Ok(ReadRoute::Local { catching_up: false })
    );
    assert_eq!(
        read_target(&own, "x", false, Some(&bytes)),
        Ok(ReadRoute::Holder {
            range_id: snapshot.config.ranges[1].range_id,
            node_id: secret(2).public(),
        })
    );
    let replaced = binding(&snapshot, 1, 6);
    assert_eq!(
        read_target(&replaced, "a", false, Some(&bytes)),
        Err(GuardError::Unbound(snapshot.set_id()))
    );
}

#[tokio::test]
async fn uploads_follow_authority() {
    use crate::distribution::test_record::store_record;
    use crate::distribution::test_store::{context, open};
    let (_dir, handle) = open();
    let context = context(&handle);
    let (snapshot, bytes) = record(true);
    store_record(&handle, snapshot.set_id(), bytes).await;
    let own = binding(&snapshot, 1, 5);
    let upload = Ulid::from_bytes([4; 16]);
    let tag = upload_tag(&context, &own, "a", upload).await.unwrap();
    assert_eq!(tag.range_id, snapshot.config.ranges[0].range_id);
    assert_eq!(upload_route(&context, &own, "a", &tag).await, Ok(()));
    assert!(matches!(
        upload_tag(&context, &own, "x", upload).await,
        Err(GuardError::NotCoordinator { .. })
    ));
    let moved = UploadTag { epoch: 2, ..tag };
    assert_eq!(
        upload_route(&context, &own, "a", &moved).await,
        Err(GuardError::Route(RouteError::StaleRange(tag.range_id)))
    );
    assert_eq!(
        delegated_range(&context, &own, tag.range_id, "a").await,
        Ok(())
    );
    let upper = snapshot.config.ranges[1].range_id;
    assert_eq!(
        delegated_range(&context, &own, upper, "a").await,
        Err(GuardError::Route(RouteError::StaleRange(upper)))
    );
}

#[tokio::test]
async fn replicas_take_reads() {
    use crate::distribution::test_record::{replicated, store_record};
    use crate::distribution::test_store::{context, open};
    let (_dir, handle) = open();
    let context = context(&handle);
    let (snapshot, bytes) = replicated(true, 1);
    store_record(&handle, snapshot.set_id(), bytes).await;
    let replica = binding(&snapshot, 2, 5);
    let lower = snapshot.config.ranges[0].range_id;
    assert_eq!(delegated_read(&context, &replica, lower, "a").await, Ok(()));
    assert!(matches!(
        delegated_range(&context, &replica, lower, "a").await,
        Err(GuardError::NotCoordinator { .. })
    ));
    let (_dir, handle) = open();
    let context = crate::distribution::test_store::context(&handle);
    let (snapshot, bytes) = record(true);
    store_record(&handle, snapshot.set_id(), bytes).await;
    let stranger = binding(&snapshot, 2, 5);
    let lower = snapshot.config.ranges[0].range_id;
    assert_eq!(
        delegated_read(&context, &stranger, lower, "a").await,
        Err(GuardError::Route(RouteError::StaleRange(lower)))
    );
}
