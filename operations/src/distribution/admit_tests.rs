// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::keyspaces::RANGE_INDEX_KEYSPACE;
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_core::structs::storage::distribution::accounting::slot_key;
use aruna_core::structs::storage::format::Compression;
use aruna_storage::storage::StorageHandle;

use super::*;
use crate::distribution::test_store::{get, open, range, send};

const BUCKET: &str = "spire";

fn secret() -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[4; 32])
}

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

fn set_id() -> Ulid {
    Ulid::from_bytes([1; 16])
}

fn member_id() -> Ulid {
    Ulid::from_bytes([2; 16])
}

fn ranges() -> Vec<KeyRange> {
    vec![range(10, "", Some("m")), range(11, "m", None)]
}

fn config() -> AdmitConfig {
    AdmitConfig {
        realm_id: realm(),
        node_id: secret().public(),
        bucket: BUCKET.to_string(),
        set_id: set_id(),
        member_id: member_id(),
        intent: [7; 32],
        ranges: ranges(),
    }
}

/// Writes the local bucket bound to `member` with `bound` as its incarnation, created as
/// incarnation `created`.
async fn bucket(handle: &StorageHandle, member: Ulid, bound: u128, created: u128) {
    let info = BucketInfo {
        group_id: Ulid::from_bytes([3; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::local(Ulid::from_bytes([5; 16]), realm()),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: Some(DistributionBinding {
            set_id: set_id(),
            member_id: member,
            incarnation: bound,
            revision: Default::default(),
        }),
    };
    let key: Key = BUCKET.as_bytes().into();
    let writes = vec![
        (
            S3_BUCKET_KEYSPACE.to_string(),
            key.clone(),
            info.to_bytes().unwrap().into(),
        ),
        (
            BUCKET_CREATION_KEYSPACE.to_string(),
            key,
            created.to_be_bytes().to_vec().into(),
        ),
    ];
    let effect = Effect::Storage(StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    });
    assert!(matches!(
        send(handle, effect).await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

/// Runs the admission against real storage; `sign` false answers the signing request with an
/// unavailable signer.
async fn run(handle: &StorageHandle, sign: bool) -> Result<SignedStatement, AdmitError> {
    let mut operation = AdmitOperation::new(config());
    let mut queue: VecDeque<Effect> = operation.start().into_iter().collect();
    while let Some(effect) = queue.pop_front() {
        let event = match effect {
            Effect::Net(NetEffect::StatementSign(claim)) => {
                assert_eq!(claim.signer, secret().public());
                let reply = match sign {
                    true => StatementSignEvent::Signed(Box::new(claim.statement.sign(&secret()))),
                    false => StatementSignEvent::Unavailable("offline".to_string()),
                };
                Event::Net(NetEvent::StatementSign(reply))
            }
            effect => send(handle, effect).await,
        };
        queue.extend(operation.step(event));
    }
    operation.finalize()
}

async fn opened(handle: &StorageHandle, upper: Option<&str>) -> bool {
    get(handle, RANGE_INDEX_KEYSPACE, slot_key(set_id(), upper))
        .await
        .is_some()
}

#[tokio::test]
async fn opens_ranges_once() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    let first = run(&handle, true).await.unwrap();
    assert!(first.verified());
    assert_eq!(first.statement.incarnation, 42);
    let expected = Statement::admit(realm(), set_id(), member_id(), 42, [7; 32]);
    assert_eq!(first.statement, expected);
    assert!(opened(&handle, Some("m")).await);
    assert!(opened(&handle, None).await);
    // A retry finds the ranges open and signs the very same statement again.
    assert_eq!(run(&handle, true).await.unwrap(), first);
}

#[tokio::test]
async fn foreign_bucket_refused() {
    let (_dir, handle) = open();
    bucket(&handle, Ulid::from_bytes([8; 16]), 42, 42).await;
    assert_eq!(
        run(&handle, true).await,
        Err(AdmitError::BindingChanged(BUCKET.to_string()))
    );
    assert!(!opened(&handle, None).await);
}

#[tokio::test]
async fn replaced_bucket_refused() {
    let (_dir, handle) = open();
    // The binding names an incarnation that is not this bucket's own creation.
    bucket(&handle, member_id(), 42, 43).await;
    assert_eq!(
        run(&handle, true).await,
        Err(AdmitError::BindingChanged(BUCKET.to_string()))
    );
}

#[tokio::test]
async fn unsigned_opens_nothing() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    assert!(matches!(
        run(&handle, false).await,
        Err(AdmitError::SignUnavailable(_))
    ));
    assert!(!opened(&handle, None).await);
}

#[tokio::test]
async fn rebuilding_usage_refused() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    let marker = Effect::Storage(StorageEffect::Write {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        key: USAGE_REBUILD_KEY.to_vec().into(),
        value: Vec::new().into(),
        txn_id: None,
    });
    send(&handle, marker).await;
    assert_eq!(run(&handle, true).await, Err(AdmitError::UsageIncomplete));
    assert!(
        !opened(&handle, None).await,
        "no range opens while usage counters are incomplete"
    );
}
