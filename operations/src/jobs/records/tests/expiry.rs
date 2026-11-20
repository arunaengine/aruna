//! Tests that a family without an outcome ends with an expiry cancel once its work settled.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::FAMILY_RECORD_KEYSPACE;
use aruna_core::structs::execution::job::family::{
    CancelAuthority, ExpiryReason, JobFamilyRecord, JobRecordEnvelope, JobRecordKind,
    PhysicalExecutionState,
};
use aruna_core::structs::execution::job::record_keys::record_key;
use aruna_core::structs::execution::job::record_rows::to_bytes;
use aruna_core::types::{Key, Value};
use aruna_drivers::driver::{DriverContext, drive};

use super::adapter::fixture;
use super::prune::{index, pass, write};
use crate::jobs::records::family::LoadFamilyOperation;
use crate::jobs::records::reduce::reduce_family;
use crate::tests::records::Family;

/// Stores `records` of `family` with an index row due long ago.
async fn seed(context: &DriverContext, family: &Family, records: &[JobRecordEnvelope]) {
    let mut writes: Vec<(String, Key, Value)> = (records.iter())
        .map(|envelope| {
            let bytes = to_bytes(envelope).expect("record encodes");
            let key = record_key(&envelope.key());
            (
                FAMILY_RECORD_KEYSPACE.to_string(),
                key,
                Value::from(bytes.as_slice()),
            )
        })
        .collect();
    writes.extend(index(family, 2_000));
    write(context, writes).await;
}

/// The expiry reasons of the family's stored cancels.
async fn expiries(context: &DriverContext, family: &Family) -> Vec<ExpiryReason> {
    let read = LoadFamilyOperation::kind(family.family(), JobRecordKind::Cancel);
    let cancels = drive(read, context).await.expect("cancels read");
    (cancels.iter())
        .filter_map(|envelope| match &envelope.record {
            JobFamilyRecord::Cancel(cancel) => match cancel.authority {
                CancelAuthority::Expiry(reason) => Some(reason),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

// An undecided family whose execution stopped expires after retention with one cancel that the
// projection shows; a family with an unstopped execution gets none. The pass runs at real time.
#[tokio::test]
async fn expires_undecided() {
    let (_dir, context, net, stopped) = fixture().await;
    let running = Family::with_local([2u8; 32], Some(net.node_id()));
    let records = stopped.stopped_with(PhysicalExecutionState::Error);
    seed(&context, &stopped, &records).await;
    let open = running.run(1, 0, PhysicalExecutionState::Error);
    seed(&context, &running, &open).await;

    pass(&context).await;

    assert_eq!(
        expiries(&context, &stopped).await,
        [ExpiryReason::Retention]
    );
    assert!(expiries(&context, &running).await.is_empty());
    let stored = drive(LoadFamilyOperation::family(stopped.family()), &context).await;
    let projection = reduce_family(stopped.family(), &stored.expect("records read"));
    let projection = projection.expect("reduces").expect("projection");
    assert_eq!(projection.expiry_reason, Some(ExpiryReason::Retention));
    net.shutdown().await;
}
