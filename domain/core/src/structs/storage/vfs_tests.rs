//! Tests the `nfs_state` key and record codecs, folder roots and head watch keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    BucketScope, DetachReason, DirectoryWitness, FileIdentity, HandleIdentity, HandleKind,
    LiveHandle, NfsMountBinding, NfsMountTarget, NfsStageRecord, ReceiptChange, RetireReason,
    StageBase, StageDisposition, StageOwner, StageScope, VfsBootState, VfsHandleRow, VfsKey,
    VfsReceipt, VfsRecord, VfsRecordError, VfsWatchKey, decode_handle_id, decode_token,
    encode_handle_id, encode_token, folder_contains,
};
use crate::structs::storage::blob::{BackendRef, BlobHeadKey};
use crate::user::id::UserId;
use ulid::Ulid;

fn scope(bucket: &str) -> BucketScope {
    BucketScope::new(bucket, 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10)
}

fn id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

#[test]
fn keys_round_trip() {
    let keys = [
        (VfsKey::Boot, 0x00),
        (VfsKey::Host(id(1)), 0x01),
        (VfsKey::Mount(id(2)), 0x02),
        (VfsKey::Handle(scope("lab"), 7), 0x03),
        (VfsKey::Name(scope("lab"), "runs/a b.txt".to_string()), 0x04),
        (VfsKey::Cookie(scope("lab"), 9), 0x05),
        (VfsKey::LiveStage(scope("lab"), 7), 0x06),
        (VfsKey::RetainedStage(scope("lab"), id(3)), 0x06),
        (VfsKey::Receipt(id(4)), 0x07),
    ];
    for (key, tag) in keys {
        let bytes = key.to_bytes().unwrap();
        assert_eq!(bytes, key.to_bytes().unwrap(), "encoding is deterministic");
        assert_eq!(bytes[0], tag);
        assert_eq!(VfsKey::from_bytes(&bytes).unwrap(), key);
    }
    assert!(matches!(
        VfsKey::Handle(BucketScope::new("a\0b", 1), 1).to_bytes(),
        Err(VfsRecordError::Bucket)
    ));
    assert!(matches!(
        VfsKey::from_bytes(&[0x03, b'l', 0]),
        Err(VfsRecordError::Malformed)
    ));
    assert!(matches!(
        VfsKey::from_bytes(&[0x09]),
        Err(VfsRecordError::Malformed)
    ));
}

#[test]
fn keys_order_numerically() {
    let handle = |bucket, id| VfsKey::Handle(scope(bucket), id).to_bytes().unwrap();
    assert!(handle("lab", 2) < handle("lab", 256));
    assert!(handle("lab", 256) < handle("lab", u64::MAX));
    // One bucket's rows never sit between another bucket's rows, even when the names share a start.
    let lab = VfsKey::scope_prefix(super::HANDLE_TAG, &scope("lab")).unwrap();
    assert!(handle("lab", u64::MAX).starts_with(&lab));
    assert!(!handle("labs", 0).starts_with(&lab));
    assert!(handle("lab", u64::MAX) < handle("labs", 0));
    let live = VfsKey::stage_prefix(&scope("lab"), super::LIVE_STAGE).unwrap();
    let retained = VfsKey::RetainedStage(scope("lab"), id(0))
        .to_bytes()
        .unwrap();
    assert!(!retained.starts_with(&live));
    assert!(
        VfsKey::LiveStage(scope("lab"), u64::MAX)
            .to_bytes()
            .unwrap()
            < retained
    );
}

#[test]
fn records_round_trip() {
    let boot = VfsBootState::default();
    assert_eq!(
        VfsBootState::from_bytes(&boot.to_bytes().unwrap()).unwrap(),
        boot
    );
    let owner = UserId::default();
    let binding = NfsMountBinding {
        mount_id: id(1),
        host_id: id(2),
        owner,
        access_key: "AKIA".to_string(),
        group_id: id(3),
        target: NfsMountTarget::for_folder("lab-data", 5, "team").unwrap(),
        read_only: false,
        allowed_networks: vec!["10.0.0.0/8".to_string()],
        display_uid: 1000,
        display_gid: 1000,
        mode: 0o755,
        created_at_ms: 11,
    };
    let bytes = binding.to_bytes().unwrap();
    assert_eq!(bytes, binding.to_bytes().unwrap());
    assert_eq!(NfsMountBinding::from_bytes(&bytes).unwrap(), binding);
    let rows = [
        VfsHandleRow::Live(LiveHandle {
            key: "team/a".to_string(),
            identity: HandleIdentity::File(FileIdentity::Committed { life_id: id(4) }),
            mode: 0o644,
            attribute_change: 3,
            cookie_id: 12,
            detached: Some(DetachReason::Unlinked),
        }),
        VfsHandleRow::Live(LiveHandle {
            key: "team/d/".to_string(),
            identity: HandleIdentity::Directory(DirectoryWitness::Unpublished {
                key: "team/d/x".to_string(),
                token: id(5),
            }),
            mode: 0o755,
            attribute_change: 4,
            cookie_id: 13,
            detached: None,
        }),
        VfsHandleRow::Tombstone {
            reason: RetireReason::Expired,
            retired_at_ms: 99,
        },
    ];
    for row in rows {
        assert_eq!(
            VfsHandleRow::from_bytes(&row.to_bytes().unwrap()).unwrap(),
            row
        );
    }
    let stage = NfsStageRecord {
        stage_id: id(6),
        handle_id: 7,
        owner: StageOwner {
            user: owner,
            access_key: "AKIA".to_string(),
            mount_id: id(1),
            scope: StageScope::User,
        },
        key: "team/a".to_string(),
        generation: 2,
        checkpoint: "stage-2".to_string(),
        logical_size: 4096,
        base: StageBase::Unpublished {
            token: id(5),
            predecessor: None,
        },
        source_copies: vec![BackendRef::node_default()],
        source_policies: Vec::new(),
        destination_copies: vec![BackendRef::node_default()],
        destination_policies: Vec::new(),
        disposition: StageDisposition::Retained {
            reason: DetachReason::LostLife,
        },
        quota_retry: None,
        foreign_life: Some(id(8)),
        updated_at_ms: 21,
    };
    assert_eq!(
        NfsStageRecord::from_bytes(&stage.to_bytes().unwrap()).unwrap(),
        stage
    );
    let receipt = VfsReceipt {
        attempt_id: id(9),
        scope: scope("lab-data"),
        created_at_ms: 30,
        change: ReceiptChange::Remove {
            key: "team/a".to_string(),
            marker_version: Some(id(10)),
            handle_id: Some(7),
        },
    };
    assert_eq!(
        VfsReceipt::from_bytes(&receipt.to_bytes().unwrap()).unwrap(),
        receipt
    );
    assert_eq!(decode_handle_id(&encode_handle_id(258)).unwrap(), 258);
    assert_eq!(
        HandleIdentity::File(FileIdentity::Unpublished {
            token: id(1),
            predecessor: Some(id(2)),
        })
        .kind(),
        HandleKind::File
    );
}

#[test]
fn boot_reserves_ids() {
    let boot = VfsBootState::default().next_boot().unwrap();
    assert_eq!(boot.boot_generation, 1);
    let (first, boot) = boot.reserve_handles(4).unwrap();
    assert_eq!(first, super::FIRST_HANDLE);
    let (second, boot) = boot.reserve_handles(1).unwrap();
    assert_eq!(second, first + 4, "a reserved id is never handed out twice");
    let (cookie, _) = boot.reserve_cookies(1).unwrap();
    assert_eq!(cookie, super::FIRST_COOKIE);
    let full = VfsBootState {
        next_handle_id: u64::MAX,
        ..boot
    };
    assert!(matches!(
        full.reserve_handles(1),
        Err(VfsRecordError::Exhausted("handle"))
    ));
}

#[test]
fn folder_roots_canonical() {
    let root = |prefix| match NfsMountTarget::for_folder("lab-data", 1, prefix) {
        Ok(NfsMountTarget::Folder { prefix, .. }) => Ok(prefix),
        Ok(other) => panic!("unexpected target {other:?}"),
        Err(error) => Err(error),
    };
    assert_eq!(root("team").unwrap(), "team/");
    assert_eq!(root("team/").unwrap(), "team/");
    assert_eq!(root("/team/sub").unwrap(), "team/sub/");
    assert!(matches!(root(""), Err(VfsRecordError::EmptyFolder)));
    assert!(matches!(root("/"), Err(VfsRecordError::EmptyFolder)));
    assert!(matches!(root("team//"), Err(VfsRecordError::Location(_))));
    assert!(matches!(root("a/../b"), Err(VfsRecordError::Location(_))));
    assert!(matches!(root(".."), Err(VfsRecordError::Location(_))));
    let target = NfsMountTarget::for_folder("lab-data", 1, "team").unwrap();
    assert!(target.contains("lab-data", "team/a"));
    assert!(target.contains("lab-data", "team/"));
    assert!(!target.contains("lab-data", "team-old/a"));
    assert!(!target.contains("lab-data", "team"));
    assert!(!target.contains("other-data", "team/a"));
    assert!(
        !folder_contains("team", "team/a"),
        "a prefix without its slash holds nothing"
    );
    assert!(!folder_contains("team//", "team//a"));
    let job = NfsMountTarget::JobInputView {
        job_id: crate::structs::execution::job::identity::JobId::from_bytes([1u8; 16]),
    };
    assert!(!job.contains("lab-data", "team/a"));
}

#[test]
fn watch_keys_disjoint() {
    let watch = VfsWatchKey::new("lab", "team/a");
    let bytes = watch.to_bytes().unwrap();
    assert_eq!(bytes, [&[0u8][..], b"lab", &[0u8], b"team/a"].concat());
    assert!(VfsWatchKey::is_watch_key(&bytes));
    assert_eq!(VfsWatchKey::from_bytes(&bytes).unwrap(), watch);
    let head = BlobHeadKey::new("lab", "team/a").to_bytes().unwrap();
    assert!(!VfsWatchKey::is_watch_key(&head));
    assert!(!VfsWatchKey::is_watch_key(&[]));
    assert_eq!(
        VfsWatchKey::of_head(&BlobHeadKey::new("lab", "team/a")),
        watch
    );
    assert!(matches!(
        VfsWatchKey::from_bytes(&head),
        Err(VfsRecordError::Malformed)
    ));
    let prefix = VfsWatchKey::bucket_prefix("lab").unwrap();
    assert!(bytes.starts_with(&prefix));
    let other = VfsWatchKey::new("labs", "x").to_bytes().unwrap();
    assert!(!other.starts_with(&prefix));
    assert_eq!(decode_token(&encode_token(id(7))).unwrap(), id(7));
    assert!(decode_token(&[1, 2, 3]).is_err());
}
