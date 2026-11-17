//! Call and result tests of the remote key wait rows, plus one run against real storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::types::{Key, Value};
use aruna_core::{NodeId, structs::execution::job::JobId};
use ulid::Ulid;

use super::{OwedWake, OwedWakesOperation, QueueWakesOperation, WaitRowOperation};
use crate::driver::{DriverContext, drive};
use crate::jobs::store::JobMutationError;
use crate::jobs::store::key_wait::WAKE_PAGE;

fn waiter() -> NodeId {
    iroh::SecretKey::from_bytes(&[2; 32]).public()
}

fn key(generation: u64) -> BucketKeyRef {
    BucketKeyRef::new(Ulid::from_bytes([8; 16]), generation)
}

fn job() -> JobId {
    JobId::from_bytes([5; 16])
}

fn auth() -> AuthContext {
    AuthContext {
        user_id: aruna_core::UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32])),
        realm_id: RealmId([1; 32]),
        path_restrictions: None,
        session: None,
    }
}

#[tokio::test]
async fn wake_until_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let context = DriverContext {
        storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    for generation in [1, 2] {
        let register = WaitRowOperation::register(key(generation), waiter(), job(), &auth());
        drive(register, &context).await.unwrap();
    }

    assert!(
        !drive(QueueWakesOperation::new(key(1)), &context)
            .await
            .unwrap()
    );
    let owed = OwedWake {
        waiter: waiter(),
        job_id: job(),
        key: key(1),
        auth: auth(),
    };
    let page = drive(OwedWakesOperation::new(None), &context)
        .await
        .unwrap();
    assert_eq!(page, (vec![owed], None));
    // A repeated unlock finds nothing left to queue for this key.
    assert!(
        !drive(QueueWakesOperation::new(key(1)), &context)
            .await
            .unwrap()
    );
    let page = drive(OwedWakesOperation::new(None), &context)
        .await
        .unwrap();
    assert_eq!(page.0.len(), 1);

    let acknowledge = WaitRowOperation::acknowledge(waiter(), job(), key(1));
    drive(acknowledge, &context).await.unwrap();
    let page = drive(OwedWakesOperation::new(None), &context)
        .await
        .unwrap();
    assert!(page.0.is_empty());
}

#[test]
fn row_deletes_once() {
    let mut row = WaitRowOperation::acknowledge(waiter(), job(), key(1));
    assert!(matches!(row.first(), Ok(Next::Call(IoCall::Delete(rows))) if rows.len() == 1));
    assert_eq!(row.next(IoResult::Done), Ok(Next::Done(())));
    assert!(matches!(row.first(), Err(JobMutationError::Storage(_))));
    assert!(matches!(
        row.next(IoResult::Woke),
        Err(JobMutationError::Storage(_))
    ));
}

#[test]
fn queue_rewrites_page() {
    let mut queue = QueueWakesOperation::new(key(1));
    assert!(queue.transaction());
    assert!(matches!(
        queue.first(),
        Ok(Next::Call(IoCall::Page { limit, after: None, .. })) if limit == WAKE_PAGE
    ));
    let mut row = vec![b'r'];
    row.extend_from_slice(&key(1).key());
    row.extend_from_slice(waiter().as_bytes());
    row.extend_from_slice(&job().to_bytes());
    let value = Value::from(postcard::to_allocvec(&auth()).unwrap().as_slice());
    let page = IoResult::Page {
        rows: vec![(Key::from(row.as_slice()), value)],
        next: None,
    };
    assert!(matches!(
        queue.next(page),
        Ok(Next::Call(IoCall::Write(writes))) if writes.len() == 1
    ));
    assert!(matches!(
        queue.next(IoResult::Done),
        Ok(Next::Call(IoCall::Delete(deletes))) if deletes.len() == 1
    ));
    assert_eq!(queue.next(IoResult::Done), Ok(Next::Done(false)));
}

#[test]
fn queue_names_conflict() {
    // A lost commit is a conflict the runner retries; any other failure is not.
    let conflict = JobMutationError::from(IoError::Conflict);
    assert!(QueueWakesOperation::is_conflict(&conflict));
    let unfinished = JobMutationError::from(IoError::Unfinished);
    assert!(!QueueWakesOperation::is_conflict(&unfinished));
}

#[test]
fn owed_names_cursor() {
    let rows: Vec<(Key, Value)> = (0..WAKE_PAGE as u8)
        .map(|seed| (Key::from(vec![b'd', seed]), Value::from(&[0u8; 0][..])))
        .collect();
    let mut owed = OwedWakesOperation::new(None);
    owed.first().expect("owed page");
    let page = IoResult::Page {
        rows: rows.clone(),
        next: None,
    };
    // Undecodable rows are skipped, yet a full page still names the next cursor.
    let next = rows.last().map(|(row, _)| row.clone());
    assert_eq!(owed.next(page), Ok(Next::Done((Vec::new(), next))));
    assert!(matches!(
        OwedWakesOperation::new(None).next(IoResult::Done),
        Err(JobMutationError::Storage(_))
    ));
}
