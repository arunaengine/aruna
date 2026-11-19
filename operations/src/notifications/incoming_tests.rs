//! Tests inbound notification delivery, watch events and peer checks between two nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::notifications::checks::{
    validate_inbound_batch, validate_inbound_kind, validate_watch_event, validate_watch_events,
    verify_node_origin,
};
use crate::notifications::outbox::OUTBOX_BATCH_SIZE;
use crate::notifications::protocol::EVENT_BATCH_SIZE;
use crate::tests::notifications::{data_group_id, data_node_id};
use crate::tests::notifications::{seeded_record, upload_event};
use aruna_core::UserId;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, watch_resource_path,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::time::unix_timestamp_millis;
use ulid::Ulid;

#[test]
fn join_payload_validates() {
    let realm_id = RealmId::from_bytes([7; 32]);
    let actor_user_id = UserId::local(Ulid::from_bytes([1; 16]), realm_id);
    let group_id = Ulid::from_bytes([2; 16]);
    let valid = NotificationKind::GroupJoinRequested {
        group_id,
        request_id: Ulid::from_bytes([3; 16]),
        actor_user_id,
    };
    assert!(validate_inbound_kind(&valid, realm_id).is_ok());
    // A federated requester of another realm may be named on an admin-bound record.
    assert!(validate_inbound_kind(&valid, RealmId::from_bytes([8; 32])).is_ok());
    let invalid = NotificationKind::GroupJoinRequested {
        group_id,
        request_id: Ulid::nil(),
        actor_user_id,
    };
    assert!(validate_inbound_kind(&invalid, realm_id).is_err());
}

#[test]
fn pending_member_validates() {
    // A local holder hears about a waiting member of any realm, but never about the nil user.
    let realm_id = RealmId::from_bytes([7; 32]);
    let pending = |member_user_id| NotificationKind::BucketKeyPending {
        bucket: "sealed".to_string(),
        node_id: data_node_id(),
        group_id: data_group_id(),
        member_user_id,
    };
    let foreign = UserId::new(Ulid::from_bytes([1; 16]), RealmId::from_bytes([8; 32]));
    assert!(validate_inbound_kind(&pending(foreign), realm_id).is_ok());
    assert!(validate_inbound_kind(&pending(UserId::nil(realm_id)), realm_id).is_err());
}

#[test]
fn transient_batch_cap() {
    let realm_id = RealmId::from_bytes([75u8; 32]);
    let recipient = UserId::new(Ulid::generate(), realm_id);
    let mut records: Vec<_> = (0..=OUTBOX_BATCH_SIZE)
        .map(|index| {
            let mut record = seeded_record(recipient, (index % 255 + 1) as u8);
            record.class = NotificationClass::Transient;
            record
        })
        .collect();

    let error = validate_inbound_batch(&records, unix_timestamp_millis())
        .expect_err("transient-only batch must use the total cap");
    assert!(error.contains("notification batch count"));
    records.truncate(OUTBOX_BATCH_SIZE);
    assert!(validate_inbound_batch(&records, unix_timestamp_millis()).is_ok());
}

#[test]
fn metadata_path_accepted() {
    let realm_id = RealmId::from_bytes([79u8; 32]);
    let actor = UserId::new(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    let document_id = Ulid::generate();
    let mut event = WatchEvent {
        event_id: Ulid::generate(),
        realm_id,
        kind: WatchEventKind::MetadataCreated,
        path: format!("meta/{group_id}/datasets/project/runs/run-42"),
        actor,
        occurred_at_ms: 1_000,
        detail: WatchEventDetail::MetadataCreated {
            group_id,
            document_id,
        },
    };

    assert!(validate_watch_event(&event, realm_id, 1_000).is_ok());
    // A federated actor is accepted; the event itself must still belong to this realm.
    event.actor = UserId::new(Ulid::generate(), RealmId::from_bytes([78u8; 32]));
    assert!(validate_watch_event(&event, realm_id, 1_000).is_ok());
    assert!(validate_watch_event(&event, RealmId::from_bytes([78u8; 32]), 1_000).is_err());

    event.path = format!("meta/{group_id}/datasets/project/");
    assert_eq!(
        validate_watch_event(&event, realm_id, 1_000),
        Err("watch event metadata path is not canonical".to_string())
    );

    event.path = format!("meta/{}/datasets/project", Ulid::generate());
    assert_eq!(
        validate_watch_event(&event, realm_id, 1_000),
        Err("watch event metadata path does not match detail".to_string())
    );
}

#[test]
fn watch_batch_caps() {
    let realm_id = RealmId::from_bytes([80u8; 32]);
    let actor = UserId::new(Ulid::generate(), realm_id);
    let events = vec![upload_event(realm_id, actor, "bucket/object"); EVENT_BATCH_SIZE + 1];

    assert!(
        validate_watch_events(&events, realm_id, 1_700_000_000_000)
            .expect_err("oversized watch batch must be rejected")
            .contains("exceeds cap")
    );
}

#[test]
fn data_identity_required() {
    let realm_id = RealmId::from_bytes([78u8; 32]);
    let actor = UserId::new(Ulid::generate(), realm_id);
    let mut event = upload_event(realm_id, actor, "bucket/object");

    assert!(
        validate_watch_event(&event, realm_id, event.occurred_at_ms).is_ok(),
        "canonical detail matches its path"
    );

    if let WatchEventDetail::DataUploaded { node_id, .. } = &mut event.detail {
        *node_id = iroh::SecretKey::from_bytes(&[32u8; 32]).public();
    } else {
        panic!("canonical data detail expected");
    }
    assert_eq!(
        validate_watch_event(&event, realm_id, event.occurred_at_ms),
        Err("watch event data path does not match detail".to_string())
    );
}

#[test]
fn validates_sync_watch() {
    let realm_id = RealmId::from_bytes([77u8; 32]);
    let actor = UserId::new(Ulid::generate(), realm_id);
    let group_id = data_group_id();
    let node_id = data_node_id();
    let mut event = WatchEvent {
        event_id: Ulid::generate(),
        realm_id,
        kind: WatchEventKind::SyncCompleted,
        path: watch_resource_path(group_id, node_id, "bucket", "prefix/"),
        actor,
        occurred_at_ms: 1_000,
        detail: WatchEventDetail::SyncCompleted {
            group_id,
            node_id,
            bucket: "bucket".to_string(),
            relationship_id: Ulid::generate(),
            versions_synced: 2,
        },
    };

    assert!(validate_watch_event(&event, realm_id, 1_000).is_ok());
    if let WatchEventDetail::SyncCompleted {
        relationship_id, ..
    } = &mut event.detail
    {
        *relationship_id = Ulid::nil();
    }
    assert!(validate_watch_event(&event, realm_id, 1_000).is_err());
}

#[test]
fn restart_notice_origin() {
    let origin = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let other = iroh::SecretKey::from_bytes(&[4; 32]).public();
    let recipient = UserId::new(Ulid::from_bytes([5; 16]), RealmId::from_bytes([1; 32]));
    let kind = NotificationKind::BucketRestartLocked {
        bucket: "raw".to_string(),
        node_id: origin,
        group_id: Ulid::from_bytes([6; 16]),
    };
    let records = vec![NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        kind,
        1,
    )];
    assert!(verify_node_origin(&records, origin).is_ok());
    assert!(verify_node_origin(&records, other).is_err());
    // Only the bucket's node may purge the notices about it.
    let deleted = NotificationKind::BucketDeleted {
        bucket: "raw".to_string(),
        node_id: origin,
        group_id: Ulid::from_bytes([6; 16]),
    };
    let records = vec![NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        deleted,
        1,
    )];
    assert!(verify_node_origin(&records, origin).is_ok());
    assert!(verify_node_origin(&records, other).is_err());
}
