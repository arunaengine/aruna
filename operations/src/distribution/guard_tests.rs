// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_record::{binding, record, secret};

#[test]
fn coordinator_stamps_epoch() {
    let (snapshot, bytes) = record(true);
    let own = binding(&snapshot, 1, 5);
    assert_eq!(write_epoch(&own, "a", Some(&bytes)), Ok(OriginalEpoch(1)));
    let Err(GuardError::NotCoordinator {
        node_id, range_id, ..
    }) = write_epoch(&own, "x", Some(&bytes))
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
        write_epoch(&own, "a", Some(&bytes)),
        Err(GuardError::Route(RouteError::Pending(snapshot.set_id())))
    );
    assert_eq!(
        write_epoch(&own, "a", None),
        Err(GuardError::RecordMissing(snapshot.set_id()))
    );
}

#[test]
fn replaced_bucket_refused() {
    let (snapshot, bytes) = record(true);
    // A recreated local bucket of the name has another incarnation than the admitted one.
    let replaced = binding(&snapshot, 1, 6);
    assert_eq!(
        write_epoch(&replaced, "a", Some(&bytes)),
        Err(GuardError::Unbound(snapshot.set_id()))
    );
    let stranger = binding(&snapshot, 9, 5);
    assert_eq!(
        write_epoch(&stranger, "a", Some(&bytes)),
        Err(GuardError::Route(RouteError::NotMember(snapshot.set_id())))
    );
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
