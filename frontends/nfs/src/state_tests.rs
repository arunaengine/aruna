//! Tests of the stateid table: seqid ordering, revocation status retention, scoped admin
//! revocation, lock bookkeeping and the cleanup revocation leaves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use bytes::Bytes;

use super::*;
use crate::backend::MountExport;
use crate::mock::{Fixture, MockBackend};

fn fixture() -> Fixture {
    MockBackend::with_host([1; 32]).1
}

fn caller(fixture: &Fixture, export: &MountExport) -> Caller {
    Caller {
        host: fixture.host,
        peer: IpAddr::V4(Ipv4Addr::LOCALHOST),
        export: Arc::new(export.clone()),
    }
}

fn owner(client_id: u64, name: &'static [u8]) -> OwnerKey {
    OwnerKey {
        client_id,
        owner: Bytes::from_static(name),
    }
}

fn object(export: &MountExport, object_id: u64) -> ObjectRef {
    ObjectRef {
        mount_id: export.mount_id,
        object_id,
    }
}

#[test]
fn seqid_ordering_checked() {
    let fixture = fixture();
    let mut table = StateTable::new(3);
    let file = object(&fixture.export, 5);
    let first = table.record_open(
        owner(1, b"o"),
        file,
        caller(&fixture, &fixture.export),
        (1, 0),
    );
    assert_eq!(first.seqid, 1);
    let upgraded = table.record_open(
        owner(1, b"o"),
        file,
        caller(&fixture, &fixture.export),
        (2, 0),
    );
    assert_eq!(upgraded.other, first.other);
    assert_eq!(upgraded.seqid, 2);
    assert_eq!(table.open_modes(&owner(1, b"o"), file), Some((3, 0)));
    assert_eq!(table.validate(&first, 1).err(), Some(NfsStat4::OldStateid));
    let ahead = Stateid {
        seqid: 3,
        other: first.other,
    };
    assert_eq!(table.validate(&ahead, 1).err(), Some(NfsStat4::BadStateid));
    let any = Stateid {
        seqid: 0,
        other: first.other,
    };
    assert!(table.validate(&any, 1).is_ok(), "seqid zero skips ordering");
    assert_eq!(
        table.validate(&upgraded, 2).err(),
        Some(NfsStat4::BadStateid)
    );
    let restarted = StateTable::new(4);
    assert_eq!(
        restarted.validate(&upgraded, 1).err(),
        Some(NfsStat4::BadStateid)
    );
}

// Expired state keeps answering EXPIRED until the client frees it.
#[test]
fn revoked_status_retained() {
    let fixture = fixture();
    let mut table = StateTable::new(3);
    let file = object(&fixture.export, 5);
    let open = table.record_open(
        owner(1, b"o"),
        file,
        caller(&fixture, &fixture.export),
        (3, 0),
    );
    let cleanups = table.revoke(|entry| entry.client_id == 1, Revoked::Expired);
    assert_eq!(cleanups.len(), 1);
    assert!(matches!(cleanups[0], Cleanup::Close { .. }));
    assert_eq!(table.test(&open, 1), NfsStat4::Expired);
    assert_eq!(table.status_flags(1), sequence_flag::EXPIRED_ALL);
    assert_eq!(table.open_modes(&owner(1, b"o"), file), None);
    assert_eq!(table.free(&open, 1), Ok(()));
    assert_eq!(table.test(&open, 1), NfsStat4::BadStateid);
    assert_eq!(table.status_flags(1), 0);
}

// Revoking one binding leaves the state of another binding of the same client alive.
#[test]
fn mount_revocation_scoped() {
    let fixture = fixture();
    let mut table = StateTable::new(3);
    let first = table.record_open(
        owner(1, b"o"),
        object(&fixture.export, 5),
        caller(&fixture, &fixture.export),
        (1, 0),
    );
    let second = table.record_open(
        owner(1, b"o"),
        object(&fixture.other, 5),
        caller(&fixture, &fixture.other),
        (1, 0),
    );
    let mount = fixture.export.mount_id;
    table.revoke(|entry| entry.object.mount_id == mount, Revoked::Admin);
    assert_eq!(table.test(&first, 1), NfsStat4::AdminRevoked);
    assert_eq!(table.test(&second, 1), NfsStat4::Ok);
    assert_eq!(table.status_flags(1), sequence_flag::ADMIN_REVOKED);
}

#[test]
fn lock_state_bookkeeping() {
    let fixture = fixture();
    let mut table = StateTable::new(3);
    let file = object(&fixture.export, 5);
    let open = table.record_open(
        owner(1, b"o"),
        file,
        caller(&fixture, &fixture.export),
        (3, 0),
    );
    let lock = table.lock_state(
        open.other,
        owner(1, b"l"),
        file,
        caller(&fixture, &fixture.export),
    );
    assert_eq!(
        table.lock_state(
            open.other,
            owner(1, b"l"),
            file,
            caller(&fixture, &fixture.export)
        ),
        lock
    );
    let held = table.mark_lock(&lock, true).expect("lock");
    assert_eq!(held.seqid, 1);
    assert!(table.has_locks(&open.other));
    assert_eq!(table.free(&held, 1), Err(NfsStat4::LocksHeld));
    assert_eq!(table.free(&open, 1), Err(NfsStat4::LocksHeld));
    let cleanups = table.revoke(|_| true, Revoked::Admin);
    assert!(
        matches!(cleanups[0], Cleanup::Unlock { .. }),
        "locks are released first"
    );
    assert!(matches!(cleanups[1], Cleanup::Close { .. }));
    let released = table.lock_state(
        open.other,
        owner(1, b"l"),
        file,
        caller(&fixture, &fixture.export),
    );
    assert_ne!(released, lock, "revoked state no longer serves its owner");
}

#[test]
fn close_drops_dependents() {
    let fixture = fixture();
    let mut table = StateTable::new(3);
    let file = object(&fixture.export, 5);
    let open = table.record_open(
        owner(1, b"o"),
        file,
        caller(&fixture, &fixture.export),
        (3, 0),
    );
    let lock = table.lock_state(
        open.other,
        owner(1, b"l"),
        file,
        caller(&fixture, &fixture.export),
    );
    table.close(&open.other);
    assert!(table.get(&lock).is_none());
    assert!(table.get(&open.other).is_none());
    assert_eq!(table.purge_client(1).len(), 0);
}
