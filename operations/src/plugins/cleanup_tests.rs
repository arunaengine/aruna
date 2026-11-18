//! Tests the sweep: expired gap counts go on both sides, a removed install's replay rows go,
//! and a removing install's waiting events free their queue slots.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::drive;
use aruna_core::plugin_runs::{GapReason, SourceKind, gap_key, reported_key};
use aruna_core::time::unix_timestamp_millis;

use super::*;
#[allow(unused_imports)]
pub(crate) use crate::plugins::flow_support::cleanup::count;
use crate::plugins::install::tests::Fixture;
use crate::plugins::matcher::tests::config;
use crate::plugins::rows::encode;
use crate::plugins::rows::tests::page_result;
use crate::plugins::store::Txn;
use crate::plugins::test_support::installed;
use aruna_core::calls::IoCall;

#[tokio::test]
async fn expired_gaps() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let config = config(&fixture).await;
    let now = unix_timestamp_millis();
    let gap = |latest_ms| {
        let mut row = GapRow::new(SourceKind::DataUploaded, GapReason::Full);
        row.add(latest_ms);
        encode(&row).unwrap()
    };
    let (kind, reason) = (SourceKind::DataUploaded, GapReason::Full);
    let sender = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
    let rows = vec![
        (gap_key(install.install_id, kind, reason), gap(now)),
        (
            reported_key(sender, install.install_id, kind, reason),
            gap(now),
        ),
    ];
    let txn = Txn::direct(&fixture.ctx);
    txn.write(PLUGIN_GAPS_KEYSPACE, rows).await.unwrap();
    assert_eq!(drive(sweep_events(&config, now), &fixture.ctx).await, Ok(0));
    let later = now + DEDUP_RETENTION_MS + 1;
    assert_eq!(
        drive(sweep_events(&config, later), &fixture.ctx).await,
        Ok(2)
    );
    assert_eq!(count(&fixture, PLUGIN_GAPS_KEYSPACE).await, 0);
}

#[test]
fn sweep_by_standing() {
    let (removing, kept) = (Ulid::from_parts(1, 1), Ulid::from_parts(1, 2));
    let mut plan = SweepEvents {
        installs: BTreeMap::from([(removing, Standing::Removing), (kept, Standing::Kept)]),
        now_ms: 9,
        phase: SweepPhase::Scanning(Table::Replays, Table::Replays.scan()),
        deletes: Vec::new(),
        dropped: 0,
    };
    let replay = |install: Ulid| ([&[b'r'][..], &install.to_bytes()].concat(), Vec::new());
    assert!(matches!(
        plan.first().unwrap(),
        Next::Call(IoCall::Page { .. })
    ));
    let rows = vec![replay(removing), replay(kept)];
    let gone = vec![(PLUGIN_REPLAYS_KEYSPACE, replay(removing).0)];
    assert_eq!(
        plan.next(page_result(rows, None)).unwrap(),
        Next::Call(delete_call(gone))
    );
    assert!(matches!(
        plan.next(IoResult::Done).unwrap(),
        Next::Call(IoCall::Page { .. })
    ));
    // A removing install's waiting row leaves its queue slot first, then the waiting set.
    let waiting = vec![(removing.to_bytes().to_vec(), Vec::new())];
    let queue = vec![(PLUGIN_QUEUE_KEYSPACE, removing.to_bytes().to_vec())];
    let waits = vec![(PLUGIN_WAITING_KEYSPACE, removing.to_bytes().to_vec())];
    assert_eq!(
        plan.next(page_result(waiting, None)).unwrap(),
        Next::Call(delete_call(queue))
    );
    assert_eq!(
        plan.next(IoResult::Done).unwrap(),
        Next::Call(delete_call(waits))
    );
    for _ in 0..2 {
        assert!(matches!(
            plan.next(IoResult::Done).unwrap(),
            Next::Call(IoCall::Page { .. })
        ));
        let empty = plan.next(page_result(Vec::new(), None)).unwrap();
        assert_eq!(empty, Next::Call(delete_call(Vec::new())));
    }
    assert_eq!(plan.next(IoResult::Done).unwrap(), Next::Done(2));
    assert!(plan.next(IoResult::Done).is_err());
}
