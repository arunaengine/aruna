//! Tests key re-mapping by pattern: paged previews, conflicts, access and resumed job pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
use super::*;
use crate::s3::bucket::rename::tests::{
    BUCKET, access, get, group, head_key, object, realm, setup,
};
use crate::tests::records::node;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use ulid::Ulid;

fn plan() -> RemapPlan {
    RemapPlan {
        from: String::new(),
        pattern: "*.faa.gz".to_string(),
        template: "proteins/{name}".to_string(),
        subfolders: false,
    }
}

async fn load(context: &DriverContext) -> BucketAccess {
    let auth = access(context).await;
    BucketAccess::load(context, &auth, (realm(), group(), node(4)), BUCKET)
        .await
        .unwrap()
}

fn scope(access: &BucketAccess) -> RemapScope<'_> {
    RemapScope {
        realm_id: realm(),
        group_id: group(),
        node_id: node(4),
        bucket: BUCKET,
        access,
    }
}

/// Stores `count` heads named `f{index}.faa.gz` and a few other keys in one write.
async fn many(context: &DriverContext, count: usize) {
    let head = CurrentVersionPointer::new(Ulid::from_parts(10, 1))
        .to_bytes()
        .unwrap();
    let names = (0..count)
        .map(|index| format!("f{index:05}.faa.gz"))
        .chain(["notes.txt".to_string(), "sub/g.faa.gz".to_string()]);
    let writes = names
        .map(|key| {
            let value = head.clone().into();
            (BLOB_HEAD_KEYSPACE.to_string(), head_key(&key).into(), value)
        })
        .collect();
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

#[tokio::test]
async fn pages_every_key() {
    let (_directory, context) = setup().await;
    many(&context, 2500).await;
    let access = load(&context).await;
    let scope = scope(&access);
    let (mut after, mut seen) = (None, Vec::new());
    loop {
        let page = preview_page(&context, &plan(), &scope, (None, after, MAX_PAGE))
            .await
            .unwrap();
        assert_eq!((page.total, page.complete), (2500, true));
        seen.extend(page.entries.iter().map(|entry| entry.from.clone()));
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    let wanted: Vec<_> = (0..2500)
        .map(|index| format!("f{index:05}.faa.gz"))
        .collect();
    assert_eq!(seen, wanted);
    // A filter narrows the totals and the entries alike.
    let page = preview_page(&context, &plan(), &scope, (Some("f0001"), None, 5))
        .await
        .unwrap();
    assert_eq!(page.total, 10);
    assert_eq!(page.entries.len(), 5);
    assert_eq!(page.entries[0].to, "proteins/f00010.faa.gz");
}

#[tokio::test]
async fn reports_collisions() {
    let (_directory, context) = setup().await;
    let (v1, v2, v3) = (
        Ulid::from_parts(10, 1),
        Ulid::from_parts(11, 1),
        Ulid::from_parts(12, 1),
    );
    object(&context, "a.faa.gz", &[v1], false).await;
    object(&context, "b.faa.gz", &[v2], false).await;
    object(&context, "proteins/b.faa.gz", &[v3], false).await;
    object(&context, "secret.faa.gz", &[v3], false).await;
    let access = load(&context).await;
    let scope = scope(&access);

    let page = preview_page(&context, &plan(), &scope, (None, None, DEFAULT_PAGE))
        .await
        .unwrap();
    let reasons: Vec<_> = page
        .entries
        .iter()
        .map(|entry| (entry.from.as_str(), entry.conflict.is_some()))
        .collect();
    assert_eq!(
        reasons,
        [
            ("a.faa.gz", false),
            ("b.faa.gz", true),
            ("secret.faa.gz", false)
        ]
    );

    // The job moves the free key and keeps the taken one; nothing is overwritten.
    let step = job_page(&context, &plan(), &scope, (None, u64::MAX))
        .await
        .unwrap();
    assert_eq!((step.done, step.skipped, step.finished), (2, 1, true));
    assert!(
        get(&context, BLOB_HEAD_KEYSPACE, head_key("a.faa.gz"))
            .await
            .is_none()
    );
    assert!(
        get(&context, BLOB_HEAD_KEYSPACE, head_key("b.faa.gz"))
            .await
            .is_some()
    );
    let kept = get(&context, BLOB_HEAD_KEYSPACE, head_key("proteins/b.faa.gz"))
        .await
        .unwrap();
    assert_eq!(
        CurrentVersionPointer::from_bytes(&kept).unwrap().version_id,
        v3
    );
}

#[tokio::test]
async fn skips_denied_keys() {
    let (_directory, context) = setup().await;
    let id = Ulid::from_parts(10, 1);
    object(&context, "secret/a.faa.gz", &[id], false).await;
    object(&context, "secret/b.faa.gz", &[id], false).await;
    let access = load(&context).await;
    let scope = scope(&access);
    let plan = RemapPlan {
        from: "secret/".to_string(),
        ..plan()
    };
    // Keys the caller may not read are neither counted nor shown, nor named by a cursor.
    let page = preview_page(&context, &plan, &scope, (None, None, 1))
        .await
        .unwrap();
    assert_eq!((page.total, page.entries.len(), page.next), (0, 0, None));
    let step = job_page(&context, &plan, &scope, (None, u64::MAX))
        .await
        .unwrap();
    assert_eq!((step.done, step.skipped), (0, 2));
    assert!(
        get(&context, BLOB_HEAD_KEYSPACE, head_key("secret/a.faa.gz"))
            .await
            .is_some()
    );
}

#[tokio::test]
async fn resumes_after_cursor() {
    let (_directory, context) = setup().await;
    many(&context, 600).await;
    let access = load(&context).await;
    let scope = scope(&access);
    let first = job_page(&context, &plan(), &scope, (None, u64::MAX))
        .await
        .unwrap();
    assert_eq!((first.done, first.finished), (JOB_KEYS as u64, false));
    // An interrupted job starts again from its stored cursor and redoes nothing.
    let (mut cursor, mut done) = (first.cursor, first.done);
    loop {
        let step = job_page(&context, &plan(), &scope, (cursor, u64::MAX))
            .await
            .unwrap();
        (cursor, done) = (step.cursor, done + step.done);
        if step.finished {
            break;
        }
    }
    assert_eq!(done, 600);
    let page = preview_page(&context, &plan(), &scope, (None, None, MAX_PAGE))
        .await
        .unwrap();
    assert_eq!(page.total, 0);
    assert!(
        get(
            &context,
            BLOB_HEAD_KEYSPACE,
            head_key("proteins/f00599.faa.gz")
        )
        .await
        .is_some()
    );
    // Keys written after the start stay.
    object(&context, "z.faa.gz", &[Ulid::from_parts(10, 1)], false).await;
    let late = job_page(&context, &plan(), &scope, (None, 0))
        .await
        .unwrap();
    assert_eq!((late.done, late.skipped), (0, 1));
}
