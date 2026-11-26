//! Committed renames and removals in one put transaction: the moved handle, stage and receipt
//! rows, the steps that lead to the joint write, and the settled outcome.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::handles_tests::{asked, child, context, granted, head, id, scope, user, vfs};
use super::super::recovery::ReceiptOutcome;
use super::super::{VfsError, head_key, state_key};
use super::*;
use crate::s3::object::put::PutObjectInput;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
use aruna_core::structs::storage::format::StoredFormat;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::structs::storage::vfs::{
    StageDisposition, StageOwner, StageScope, VfsRecord, encode_handle_id,
};
use std::collections::HashMap;

/// A prepared write of `key` in the test bucket.
pub(in crate::vfs) fn prepared(key: &str) -> PutObjectConfig {
    PutObjectConfig {
        user_id: user(),
        group_id: id(3),
        realm_id: context().realm_id,
        node_id: context().node_id,
        request: PutObjectInput {
            bucket: "bucket".to_string(),
            key: key.to_string(),
            content_length: Some(0),
            body: None,
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: None,
        quota_ceiling: None,
        routing: RoutingSnapshot::single(id(3)),
    }
}

fn identity(kind: HandleKind) -> HandleIdentity {
    match kind {
        HandleKind::File => HandleIdentity::File(FileIdentity::Committed { life_id: id(4) }),
        HandleKind::Directory => {
            HandleIdentity::Directory(DirectoryWitness::Marker { life_id: id(4) })
        }
    }
}

fn moving(kind: HandleKind) -> RenameInput {
    RenameInput {
        context: context(),
        from: "p/a".to_string(),
        to: "q/b".to_string(),
        kind,
        identity: identity(kind),
        handle_id: Some(4),
        instance_id: Some(1),
        replace: false,
        attempt_id: id(20),
        stage: None,
    }
}

fn handle_row(kind: HandleKind) -> Value {
    let live = LiveHandle {
        key: "p/a".to_string(),
        identity: identity(kind),
        mode: 0o644,
        attribute_change: 1,
        cookie_id: 9,
        detached: None,
    };
    Value::from(VfsHandleRow::Live(live).to_bytes().unwrap())
}

/// The rows a move reads: a live source, a free destination named by handle 5, both reverse
/// rows and the source handle.
fn rows(kind: HandleKind) -> Vec<Option<Value>> {
    let name = |handle_id| Some(Value::from(encode_handle_id(handle_id).to_vec()));
    vec![
        Some(head(2, Some(4))),
        None,
        None,
        None,
        None,
        None,
        name(4),
        name(5),
        Some(handle_row(kind)),
    ]
}

fn dirty_stage() -> NfsStageRecord {
    NfsStageRecord {
        stage_id: id(12),
        handle_id: 4,
        owner: StageOwner {
            user: user(),
            access_key: "KEY1".to_string(),
            mount_id: id(13),
            scope: StageScope::User,
        },
        key: "p/a".to_string(),
        generation: 2,
        checkpoint: "c-2".to_string(),
        logical_size: 5,
        base: StageBase::Committed {
            version_id: id(2),
            life_id: id(4),
        },
        source_copies: Vec::new(),
        source_policies: Vec::new(),
        destination_copies: Vec::new(),
        destination_policies: Vec::new(),
        disposition: StageDisposition::Live,
        quota_retry: None,
        foreign_life: None,
        updated_at_ms: 1,
    }
}

fn written(plan: &MovePlan, key: &VfsKey) -> Option<Value> {
    let key = state_key(key).unwrap();
    let row = plan.writes.iter().find(|(_, row, _)| *row == key);
    row.map(|row| row.2.clone())
}

fn stored() -> PutObjectResult {
    PutObjectResult {
        location: BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path".to_string(),
            ulid: id(40),
            format: StoredFormat::default(),
            created_by: user(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        },
        version_id: id(20),
        replayed: false,
    }
}

fn put_done(result: Result<PutObjectResult, PutObjectError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(result))
}

#[test]
fn committed_file_plan() {
    // The moved handle, its reverse rows, the dirty stage and the receipt name the new life.
    let mut input = moving(HandleKind::File);
    input.stage = Some(dirty_stage());
    let moved = committed_move(
        &input,
        &scope(),
        &rows(HandleKind::File),
        (id(30), id(31)),
        70,
    );
    let moved = moved.unwrap();
    assert_eq!(moved.displaced, Some(5));
    let plan = &moved.rows;
    let handle = written(plan, &VfsKey::Handle(scope(), 4)).unwrap();
    let VfsHandleRow::Live(handle) = VfsHandleRow::from_bytes(&handle).unwrap() else {
        panic!("the moved handle stays live");
    };
    let life = HandleIdentity::File(FileIdentity::Committed { life_id: id(31) });
    assert_eq!((handle.key.as_str(), handle.identity), ("q/b", life));
    let name = written(plan, &VfsKey::Name(scope(), "q/b".to_string()));
    assert_eq!(name, Some(Value::from(encode_handle_id(4).to_vec())));
    let source = state_key(&VfsKey::Name(scope(), "p/a".to_string())).unwrap();
    assert!(plan.deletes.iter().any(|(_, key)| *key == source));
    let stage = written(plan, &VfsKey::LiveStage(scope(), 4)).unwrap();
    let stage = NfsStageRecord::from_bytes(&stage).unwrap();
    let base = StageBase::Committed {
        version_id: id(30),
        life_id: id(31),
    };
    assert_eq!(
        (stage.key.as_str(), stage.base, stage.generation),
        ("q/b", base, 2)
    );
    let receipt = written(plan, &VfsKey::Receipt(id(20))).unwrap();
    let receipt = VfsReceipt::from_bytes(&receipt).unwrap();
    assert_eq!(
        receipt.change,
        ReceiptChange::FilePublish {
            key: "q/b".to_string(),
            version_id: id(30),
            life_id: id(31),
            handle_id: Some(4),
            stage_id: Some(id(12)),
        }
    );
    // The stage generation and both reverse rows are guarded as read.
    let guarded = |key: &VfsKey| {
        let key = state_key(key).unwrap();
        plan.guards.iter().any(|(_, row, _)| *row == key)
    };
    assert!(guarded(&VfsKey::LiveStage(scope(), 4)));
    assert!(guarded(&VfsKey::Name(scope(), "q/b".to_string())));
}

#[test]
fn marker_directory_plan() {
    // An empty marker directory moves its handle under the destination marker's life.
    let input = moving(HandleKind::Directory);
    let moved = committed_move(
        &input,
        &scope(),
        &rows(HandleKind::Directory),
        (id(30), id(31)),
        70,
    );
    let plan = moved.unwrap().rows;
    let handle = written(&plan, &VfsKey::Handle(scope(), 4)).unwrap();
    let VfsHandleRow::Live(handle) = VfsHandleRow::from_bytes(&handle).unwrap() else {
        panic!("the moved handle stays live");
    };
    let marker = DirectoryWitness::Marker { life_id: id(31) };
    assert_eq!(handle.identity, HandleIdentity::Directory(marker));
    let receipt = written(&plan, &VfsKey::Receipt(id(20))).unwrap();
    let receipt = VfsReceipt::from_bytes(&receipt).unwrap();
    assert!(matches!(
        receipt.change,
        ReceiptChange::DirectoryRename { .. }
    ));
}

/// Runs a committed move up to its rows read, through the parent marker admission.
fn admitted(kind: HandleKind) -> VfsRenameOperation {
    let target = match kind {
        HandleKind::File => "q/b".to_string(),
        HandleKind::Directory => "q/b/".to_string(),
    };
    let mut operation = VfsRenameOperation::new(moving(kind))
        .with_destination_put(prepared(&target))
        .with_parent_marker(prepared("p/"));
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    operation.next(granted()).unwrap();
    // The marker write is admitted on its own before the permit.
    let turn = operation.next(granted());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let turn = operation.next(granted());
    assert!(matches!(asked(&turn), VfsEffect::AcquirePermit { .. }));
    let turn = operation.next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    operation
}

fn staged_none() -> IoResult {
    vfs(VfsEvent::StagedList {
        entries: Vec::new(),
        sequence: 1,
    })
}

#[test]
fn committed_file_moves() {
    // A committed file moves as one put; the settled move detaches the displaced instance.
    let mut operation = admitted(HandleKind::File);
    let turn = operation.next(IoResult::Values(rows(HandleKind::File)));
    assert!(matches!(asked(&turn), VfsEffect::ListStaged { prefix, .. } if prefix == "q/b/"));
    let turn = operation.next(staged_none());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(
        operation.next(put_done(Ok(stored()))),
        Ok(Next::Call(IoCall::Sync))
    );
    let turn = operation.next(IoResult::Done);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let settled = ReceiptOutcome::Applied {
        life_id: Some(id(31)),
    };
    let Ok(Next::Then { calls, .. }) = operation.next(child(settled)) else {
        panic!("the move settles with its releases");
    };
    let detach = vfs_call(VfsEffect::DetachInstance {
        scope: scope(),
        handle_id: 5,
        instance_id: None,
        reason: DetachReason::Displaced,
    });
    assert_eq!(calls.first(), Some(&detach));
}

#[test]
fn moved_source_refused() {
    // A changed source or destination refuses the whole move; nothing waits on a receipt.
    let mut operation = admitted(HandleKind::File);
    operation
        .next(IoResult::Values(rows(HandleKind::File)))
        .unwrap();
    operation.next(staged_none()).unwrap();
    let turn = operation.next(put_done(Err(PutObjectError::HeadChanged)));
    assert_eq!(turn, Err(VfsError::Conflict));
    let mut operation = admitted(HandleKind::File);
    operation
        .next(IoResult::Values(rows(HandleKind::File)))
        .unwrap();
    operation.next(staged_none()).unwrap();
    let turn = operation.next(put_done(Err(PutObjectError::ObjectExists)));
    assert_eq!(turn, Err(VfsError::Exists));
}

#[test]
fn marker_directory_moves() {
    // An empty marker directory is checked for staged and live children, then moves as one put.
    let mut operation = admitted(HandleKind::Directory);
    let turn = operation.next(IoResult::Values(rows(HandleKind::Directory)));
    assert!(matches!(asked(&turn), VfsEffect::ListStaged { prefix, .. } if prefix == "q/b/"));
    let turn = operation.next(staged_none());
    assert!(matches!(asked(&turn), VfsEffect::ListStaged { prefix, .. } if prefix == "p/a/"));
    let turn = operation.next(staged_none());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
    let own = head_key("bucket", "p/a/").unwrap();
    let page = IoResult::Page {
        rows: vec![(own, head(2, Some(4)))],
        next: None,
    };
    let turn = operation.next(page);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
}

#[test]
fn nonempty_directory_stays() {
    // A live hidden S3 child keeps a directory in place.
    let mut operation = admitted(HandleKind::Directory);
    operation
        .next(IoResult::Values(rows(HandleKind::Directory)))
        .unwrap();
    operation.next(staged_none()).unwrap();
    operation.next(staged_none()).unwrap();
    let child_key = head_key("bucket", "p/a/x").unwrap();
    let page = IoResult::Page {
        rows: vec![(child_key, head(6, Some(7)))],
        next: None,
    };
    assert_eq!(operation.next(page), Err(VfsError::NotEmpty));
}

#[test]
fn denied_marker_skipped() {
    // A parent marker the caller may not write is left out; the move goes on.
    let mut operation = VfsRenameOperation::new(moving(HandleKind::File))
        .with_destination_put(prepared("q/b"))
        .with_parent_marker(prepared("p/"));
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    operation.next(granted()).unwrap();
    operation.next(granted()).unwrap();
    let denied = IoResult::Child(ChildOutcome::new(Err::<
        super::super::policy::VfsGrant,
        VfsError,
    >(VfsError::Denied)));
    let turn = operation.next(denied);
    assert!(matches!(asked(&turn), VfsEffect::AcquirePermit { .. }));
    assert_eq!(operation.marker, None);
}

#[test]
fn last_child_removed() {
    // Removing a committed file with an admitted parent marker is one put of the marker.
    let input = RemoveInput {
        context: context(),
        key: "p/a".to_string(),
        kind: HandleKind::File,
        handle_id: None,
        identity: identity(HandleKind::File),
        instance_id: None,
        attempt_id: id(21),
    };
    let mut operation = VfsRemoveOperation::new(input).with_parent_marker(prepared("p/"));
    operation.first().unwrap();
    let turn = operation.next(granted());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let turn = operation.next(granted());
    assert!(matches!(asked(&turn), VfsEffect::AcquirePermit { .. }));
    let turn = operation.next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let turn = operation.next(IoResult::Values(vec![None, Some(head(2, Some(4))), None]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert!(operation.joint);
    let turn = operation.next(put_done(Err(PutObjectError::FenceTaken)));
    assert_eq!(turn, Err(VfsError::Conflict));
}
