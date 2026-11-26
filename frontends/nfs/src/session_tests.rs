//! Tests of the session table: client records, CREATE_SESSION sequencing, slot replay rules,
//! negotiated limits and host-bound sessions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::{IpAddr, Ipv4Addr};

use super::*;
use crate::backend::HostIdentity;

const LIMITS: ServerLimits = ServerLimits {
    max_request: 1 << 20,
    max_response: 1 << 20,
};
const SESSION: [u8; 16] = [7; 16];

fn host() -> Ulid {
    Ulid::from_parts(9, 1)
}

fn principal() -> RpcPrincipal {
    RpcPrincipal::Sys { uid: 0, gid: 0 }
}

fn sender(principal: &RpcPrincipal, connection: u64) -> Sender<'_> {
    Sender {
        host_id: host(),
        connection,
        principal,
    }
}

fn proposal() -> ChannelAttributes {
    ChannelAttributes {
        header_pad: 0,
        max_request: 1 << 20,
        max_response: 1 << 20,
        max_cached: 8192,
        max_operations: 16,
        max_requests: 4,
    }
}

fn create(client_id: u64, sequence: u32) -> CreateRequest {
    CreateRequest {
        client_id,
        sequence,
        session_id: SESSION,
        fore: proposal(),
        back: proposal(),
        limits: LIMITS,
    }
}

/// A table with one confirmed client and its session `SESSION`.
fn opened(now: Instant) -> (SessionTable, u64) {
    let mut table = SessionTable::new(1);
    let caller = principal();
    let exchange = table
        .exchange(
            sender(&caller, 1),
            Bytes::from_static(b"owner"),
            [1; 8],
            now,
        )
        .expect("exchange");
    table
        .create_session(
            sender(&caller, 1),
            create(exchange.client_id, exchange.sequence),
            now,
        )
        .expect("create");
    (table, exchange.client_id)
}

fn slot(sequence: u32, digest: u8) -> SlotRequest {
    SlotRequest {
        session: SESSION,
        slot: 0,
        sequence,
        highest: 0,
        digest: [digest; 32],
        shape: RequestShape {
            size: 100,
            operations: 2,
        },
    }
}

fn grant() -> ReplayGrant {
    ReplayGrant {
        host: HostIdentity {
            host_id: host(),
            owner: crate::mock::user(),
        },
        peer: IpAddr::V4(Ipv4Addr::LOCALHOST),
        principal: principal(),
        mounts: Vec::new(),
        reads: false,
        writes: true,
    }
}

// New, retried, falsely retried, misordered and uncached requests on one slot.
#[tokio::test]
async fn slot_sequencing_rules() {
    let now = Instant::now();
    let (mut table, _) = opened(now);
    let caller = principal();
    assert!(matches!(
        table.begin(sender(&caller, 1), slot(1, 1), now),
        SlotDecision::Execute { .. }
    ));
    let cached = SlotReply::Cached {
        digest: [1; 32],
        bytes: Bytes::from_static(b"reply"),
        grant: grant(),
    };
    table.finish(&SESSION, 0, cached);
    match table.begin(sender(&caller, 1), slot(1, 1), now) {
        SlotDecision::Replay { bytes, .. } => assert_eq!(&bytes[..], b"reply"),
        other => panic!("a retry replays, got {other:?}"),
    }
    let decision = table.begin(sender(&caller, 1), slot(1, 2), now);
    assert!(matches!(decision, SlotDecision::Fail(NfsStat4::FalseRetry)));
    let decision = table.begin(sender(&caller, 1), slot(3, 3), now);
    assert!(matches!(
        decision,
        SlotDecision::Fail(NfsStat4::SequenceMisordered)
    ));
    assert!(matches!(
        table.begin(sender(&caller, 1), slot(2, 4), now),
        SlotDecision::Execute { .. }
    ));
    let decision = table.begin(sender(&caller, 1), slot(2, 4), now);
    assert!(
        matches!(decision, SlotDecision::Fail(NfsStat4::Delay)),
        "a busy slot"
    );
    table.finish(&SESSION, 0, SlotReply::Uncached { digest: [4; 32] });
    let decision = table.begin(sender(&caller, 1), slot(2, 4), now);
    assert!(matches!(
        decision,
        SlotDecision::Fail(NfsStat4::RetryUncached)
    ));
}

#[tokio::test]
async fn create_session_replays() {
    let now = Instant::now();
    let mut table = SessionTable::new(1);
    let caller = principal();
    let exchange = table
        .exchange(
            sender(&caller, 1),
            Bytes::from_static(b"owner"),
            [1; 8],
            now,
        )
        .expect("exchange");
    assert!(!exchange.confirmed);
    let (first, _) = table
        .create_session(sender(&caller, 1), create(exchange.client_id, 1), now)
        .expect("create");
    let (again, _) = table
        .create_session(sender(&caller, 1), create(exchange.client_id, 1), now)
        .expect("replay");
    assert_eq!(first, again);
    assert_eq!(first.fore.max_requests, 4);
    assert_eq!(first.back.max_requests, 1);
    let misordered = table.create_session(sender(&caller, 1), create(exchange.client_id, 5), now);
    assert_eq!(misordered.unwrap_err(), NfsStat4::SequenceMisordered);
    let confirmed = table
        .exchange(
            sender(&caller, 1),
            Bytes::from_static(b"owner"),
            [1; 8],
            now,
        )
        .expect("exchange");
    assert!(confirmed.confirmed);
    assert_eq!(confirmed.client_id, exchange.client_id);
}

// Sessions belong to one enrolled host; another host neither uses nor binds them.
#[tokio::test]
async fn sessions_host_bound() {
    let now = Instant::now();
    let (mut table, _) = opened(now);
    let caller = principal();
    let stranger = Sender {
        host_id: Ulid::from_parts(9, 2),
        connection: 5,
        principal: &caller,
    };
    assert!(matches!(
        table.begin(stranger, slot(1, 1), now),
        SlotDecision::Fail(NfsStat4::BadSession)
    ));
    assert_eq!(table.bind(stranger, &SESSION), Err(NfsStat4::BadSession));
    assert_eq!(
        table.destroy_session(stranger, &SESSION),
        Err(NfsStat4::BadSession)
    );
    assert_eq!(table.bind(sender(&caller, 2), &SESSION), Ok(()));
}

#[tokio::test]
async fn limits_checked_first() {
    let now = Instant::now();
    let (mut table, _) = opened(now);
    let caller = principal();
    let mut large = slot(1, 1);
    large.shape.size = 2 << 20;
    let decision = table.begin(sender(&caller, 1), large, now);
    assert!(matches!(
        decision,
        SlotDecision::Fail(NfsStat4::RequestTooBig)
    ));
    let mut long = slot(1, 1);
    long.shape.operations = 17;
    let decision = table.begin(sender(&caller, 1), long, now);
    assert!(matches!(
        decision,
        SlotDecision::Fail(NfsStat4::TooManyOperations)
    ));
    let mut outside = slot(1, 1);
    outside.slot = 4;
    outside.highest = 4;
    let decision = table.begin(sender(&caller, 1), outside, now);
    assert!(matches!(decision, SlotDecision::Fail(NfsStat4::BadSlot)));
}

// A restarted server knows neither the old session nor the old client.
#[tokio::test]
async fn restart_forgets_sessions() {
    let now = Instant::now();
    let (_, client_id) = opened(now);
    let mut restarted = SessionTable::new(2);
    let caller = principal();
    assert!(matches!(
        restarted.begin(sender(&caller, 1), slot(1, 1), now),
        SlotDecision::Fail(NfsStat4::BadSession)
    ));
    let stale = restarted.create_session(sender(&caller, 1), create(client_id, 2), now);
    assert_eq!(stale.unwrap_err(), NfsStat4::StaleClient);
}

#[tokio::test]
async fn destroy_client_waits() {
    let now = Instant::now();
    let (mut table, client_id) = opened(now);
    let caller = principal();
    assert_eq!(
        table.destroy_client(sender(&caller, 1), client_id),
        Err(NfsStat4::ClientBusy)
    );
    table
        .destroy_session(sender(&caller, 1), &SESSION)
        .expect("destroy session");
    assert_eq!(table.destroy_client(sender(&caller, 1), client_id), Ok(()));
    assert!(table.client(client_id).is_none());
    assert_eq!(
        table.reclaim_complete(client_id),
        Err(NfsStat4::StaleClient)
    );
}

// Lease expiry depends only on the injected time.
#[tokio::test]
async fn lease_expiry_listed() {
    let now = Instant::now();
    let (mut table, client_id) = opened(now);
    let lease = Duration::from_secs(90);
    assert!(table.expired(now + lease, lease).is_empty());
    assert_eq!(table.expired(now + lease * 2, lease), vec![client_id]);
    assert_eq!(
        table.forget_idle(now + lease * 9, lease * 8),
        vec![client_id]
    );
    assert!(table.client(client_id).is_none());
}
