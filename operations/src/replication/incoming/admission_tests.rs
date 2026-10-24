//! Tests that a member bucket takes only its coordinators' publications, admitted against the
//! record it holds, and that no other bucket takes a publication.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::pure_tests::{
    bucket_read, load_routing, make_bucket_info, make_manifest, test_realm_id,
};
use super::*;
use crate::distribution::test_record::{binding, member, replicated, secret};
use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
use aruna_core::structs::storage::distribution::Publication;
use aruna_core::structs::storage::replication::ReplicationItemKind;
use aruna_core::structs::storage::routing::GroupRoutingInputs;

/// The lower range's delete marker for member 2 from member 1, and the record bytes.
fn published() -> (VersionReplicationManifest, Vec<u8>, BucketInfo) {
    let (snapshot, bytes) = replicated(true, 1);
    let range = &snapshot.config.ranges[0];
    let (_, deliveries) = snapshot.key_deliveries(member(1), "a").unwrap();
    let publication: Publication = snapshot.publication(range, &deliveries[0]);
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.key = "a".to_string();
    manifest.writer_auth_context = None;
    manifest.distribution = Some(publication);
    let mut bucket = make_bucket_info(Ulid::from_bytes([4; 16]));
    bucket.distribution = Some(binding(&snapshot, 2, 5));
    (manifest, bytes, bucket)
}

fn receiver(manifest: VersionReplicationManifest, sender: u8) -> IncomingVersionOperation {
    IncomingVersionOperation::new(
        Ulid::from_bytes([1; 16]),
        secret(2).public(),
        test_realm_id(),
        manifest,
    )
    .with_publisher_node(secret(sender).public())
}

fn rejected(operation: &IncomingVersionOperation) -> bool {
    matches!(
        operation.negotiation_result,
        Some(ReplicationNegotiationResult::Rejected(_))
    )
}

/// Runs the negotiation up to the record read and answers it with `record`.
fn admit(
    mut operation: IncomingVersionOperation,
    bucket: &BucketInfo,
    record: Vec<u8>,
) -> IncomingVersionOperation {
    operation.start();
    operation.step(bucket_read(Some(bucket.to_bytes().unwrap().into())));
    let effects = load_routing(&mut operation, GroupRoutingInputs::default());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, txn_id: None, .. })]
            if key_space == DISTRIBUTION_KEYSPACE
    ));
    operation.step(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: Some(record.into()),
    }));
    operation
}

#[test]
fn admits_coordinator() {
    let (manifest, record, bucket) = published();
    let operation = admit(receiver(manifest, 1), &bucket, record);
    assert_eq!(operation.state, IncomingVersionState::ReadExistingVersion);
}

#[test]
fn refuses_other_sender() {
    let (manifest, record, bucket) = published();
    let operation = admit(receiver(manifest, 3), &bucket, record);
    assert!(
        rejected(&operation),
        "only the range's coordinator publishes"
    );
}

#[test]
fn refuses_unbound_destination() {
    let (manifest, _, mut bucket) = published();
    bucket.distribution = None;
    let mut operation = receiver(manifest, 1);
    operation.start();
    operation.step(bucket_read(Some(bucket.to_bytes().unwrap().into())));
    assert!(rejected(&operation));
}

#[test]
fn refuses_foreign_input() {
    // An ordinary replica never lands in a member bucket, whoever writes it.
    let (mut manifest, _, bucket) = published();
    manifest.distribution = None;
    manifest.writer_auth_context = Some(manifest.auth_context.clone());
    let mut operation = receiver(manifest, 1);
    operation.start();
    operation.step(bucket_read(Some(bucket.to_bytes().unwrap().into())));
    assert!(rejected(&operation));
}

#[test]
fn never_creates_member() {
    let (manifest, _, _) = published();
    let mut operation = receiver(manifest, 1);
    operation.start();
    operation.step(bucket_read(None));
    assert!(
        rejected(&operation),
        "a missing member bucket is not created by a publication"
    );
}
