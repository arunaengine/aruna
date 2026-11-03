//! Tests the share link records, the owner index and the access log.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::share::{ShareAction, ShareMode, ShareTarget, ShareUses};
use aruna_storage::storage::FjallStorage;

fn context(dir: &tempfile::TempDir) -> DriverContext {
    DriverContext {
        storage_handle: FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

async fn save(context: &DriverContext, link: ShareLink) {
    let Ok(aruna_core::effects::Effect::Storage(effect)) = save_effect(&link) else {
        panic!("a link saves with one storage batch");
    };
    let event = context.storage_handle.send_storage_effect(effect).await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

fn link(owner: u8, id: u8) -> ShareLink {
    let realm = RealmId::from_bytes([1; 32]);
    ShareLink {
        id: Ulid::from_bytes([id; 16]),
        group_id: Ulid::from_bytes([2; 16]),
        target: ShareTarget::Bucket {
            bucket: "data".into(),
        },
        mode: ShareMode::Read,
        created_by: UserId::new(Ulid::from_bytes([owner; 16]), realm),
        created_at: 1,
        label: String::new(),
        expires_at: None,
        password: None,
        secret: [id; 32],
        signing_key: [id; 32],
        revoked_at: None,
    }
}

#[tokio::test]
async fn lists_own_links() {
    let dir = tempfile::tempdir().unwrap();
    let context = context(&dir);
    for (owner, id) in [(3, 10), (3, 11), (4, 12)] {
        save(&context, link(owner, id)).await;
    }
    let mut revoked = link(3, 10);
    revoked.revoked_at = Some(5);
    save(&context, revoked.clone()).await;
    let mine = drive(ListSharesOperation::new(link(3, 0).created_by), &context)
        .await
        .unwrap();
    assert_eq!(mine, vec![revoked.clone(), link(3, 11)]);
    let read = drive(GetShareOperation::new(revoked.id), &context)
        .await
        .unwrap();
    assert_eq!(read, Some(revoked));
    let missing = drive(GetShareOperation::new(Ulid::from_bytes([99; 16])), &context)
        .await
        .unwrap();
    assert_eq!(missing, None);
    // Another user reads someone else's link as missing.
    let foreign = GetShareOperation::for_owner(Ulid::from_bytes([12; 16]), link(3, 0).created_by);
    assert_eq!(drive(foreign, &context).await.unwrap(), None);
}

#[tokio::test]
async fn log_pages_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let context = context(&dir);
    let share = Ulid::from_bytes([7; 16]);
    let other = Ulid::from_bytes([8; 16]);
    let event = |at| ShareEvent {
        at,
        action: ShareAction::Download,
        key: Some("a.txt".into()),
        bytes: Some(3),
        client: Some("192.0.2.0/24".into()),
    };
    for (index, id) in [share, share, share, other].into_iter().enumerate() {
        let entry = Ulid::from_parts(index as u64 + 1, 0);
        drive(
            LogShareOperation::new(id, entry, event(index as u64)),
            &context,
        )
        .await
        .unwrap();
    }
    let (first, next) = drive(ShareEventsOperation::new(share, None, 2), &context)
        .await
        .unwrap();
    assert_eq!(first.iter().map(|(_, e)| e.at).collect::<Vec<_>>(), [0, 1]);
    let (rest, end) = drive(ShareEventsOperation::new(share, next, 2), &context)
        .await
        .unwrap();
    assert_eq!(rest.iter().map(|(_, e)| e.at).collect::<Vec<_>>(), [2]);
    assert_eq!(end, None);
    // Each entry counts in the link's use row; the other link keeps its own.
    let uses = drive(
        ReadUsesOperation::new(vec![share, other, Ulid::nil()]),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(uses[0].downloads, 3);
    assert_eq!(uses[0].last_access_at, Some(2));
    assert_eq!(uses[1].downloads, 1);
    assert_eq!(uses[2], ShareUses::default());
}

#[tokio::test]
async fn concurrent_logs_count() {
    let dir = tempfile::tempdir().unwrap();
    let context = context(&dir);
    let share = Ulid::from_bytes([7; 16]);
    let event = ShareEvent {
        at: 1,
        action: ShareAction::Upload,
        key: None,
        bytes: None,
        client: None,
    };
    let log = |entry: u64| LogShareOperation::new(share, Ulid::from_parts(entry, 0), event.clone());
    let runs: Vec<_> = (1..=8).map(|entry| drive(log(entry), &context)).collect();
    for result in futures_util::future::join_all(runs).await {
        result.unwrap();
    }
    let uses = drive(ReadUsesOperation::new(vec![share]), &context)
        .await
        .unwrap();
    assert_eq!(uses[0].uploads, 8);
}
