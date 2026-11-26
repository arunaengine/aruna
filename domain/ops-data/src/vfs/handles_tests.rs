//! Witness selection and continuity, handle identity checks, lazy handle allocation and
//! listing cookies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::handles::*;
use super::policy::{VfsAccessContext, VfsGrant};
use super::{VfsError, state_key};
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::credential_encryption::EncryptedS3Secret;
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{CurrentVersionPointer, UserAccess};
use aruna_core::structs::storage::vfs::{
    BucketScope, DirectoryWitness, FileIdentity, HandleIdentity, LiveHandle, NfsMountTarget,
    RetireReason, VfsBootState, VfsHandleRow, VfsKey, VfsRecord, encode_handle_id,
};
use aruna_core::types::Value;
use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::{InstanceSummary, StagedEntry, VfsEffect, VfsEvent, VfsProtocol};
use std::time::SystemTime;
use ulid::Ulid;

pub(super) fn id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

pub(super) fn user() -> UserId {
    UserId::new(id(1), RealmId::from_bytes([1; 32]))
}

pub(super) fn scope() -> BucketScope {
    BucketScope::new("bucket", 7)
}

pub(super) fn context() -> VfsAccessContext {
    VfsAccessContext {
        realm_id: RealmId::from_bytes([1; 32]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        access_key: "KEY1".to_string(),
        protocol: VfsProtocol::Nfs,
        target: NfsMountTarget::for_bucket("bucket", 7).unwrap(),
        read_only: false,
        now_ms: 50,
    }
}

/// A grant as the admission child answers it for the test bucket.
pub(super) fn grant() -> VfsGrant {
    let access = UserAccess {
        access_key: "KEY1".to_string(),
        user_identity: user(),
        group_id: id(3),
        secret: EncryptedS3Secret::empty(),
        expiry: SystemTime::UNIX_EPOCH,
        path_restrictions: None,
        issued_by: [2; 32],
        revoked_at: None,
    };
    VfsGrant {
        access,
        owner: user(),
        bucket: aruna_test_harness::s3::bucket_info(id(3), user()),
        incarnation: 7,
        encryption: Default::default(),
        path: "/realm/g/group/data/node/bucket".to_string(),
        context: AuthContext::new(user(), RealmId::from_bytes([1; 32])),
        scope: None,
    }
}

pub(super) fn granted() -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<VfsGrant, VfsError>(grant())))
}

pub(super) fn vfs(event: VfsEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Vfs(Box::new(event))))
}

pub(super) fn child<T>(value: T) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(Ok::<T, VfsError>(value)))
}

pub(super) fn asked(turn: &Result<Next<impl std::fmt::Debug>, VfsError>) -> VfsEffect {
    match turn {
        Ok(Next::Call(IoCall::Effect(effect))) => match effect.as_ref() {
            Effect::Vfs(effect) => (**effect).clone(),
            other => panic!("expected a file server effect, got {other:?}"),
        },
        other => panic!("expected an effect call, got {other:?}"),
    }
}

pub(super) fn head(version: u8, life: Option<u8>) -> Value {
    let pointer = CurrentVersionPointer::new(id(version)).with_life(life.map(id));
    Value::from(pointer.to_bytes().unwrap())
}

fn live(key: &str, identity: HandleIdentity) -> LiveHandle {
    LiveHandle {
        key: key.to_string(),
        identity,
        mode: 0o644,
        attribute_change: 1,
        cookie_id: 9,
        detached: None,
    }
}

#[test]
fn witness_preference_order() {
    let mut facts = WitnessFacts {
        marker: None,
        descendants: vec![("d/b".to_string(), id(5)), ("d/a".to_string(), id(4))],
        unpublished: vec![("d/new".to_string(), id(6))],
    };
    // A live descendant by exact key and life comes before an unpublished one.
    let witness = select_witness(&facts);
    let descendant = DirectoryWitness::Descendant {
        key: "d/a".to_string(),
        life_id: id(4),
    };
    assert_eq!(witness, Some(descendant));
    facts.marker = Some(id(9));
    let marker = DirectoryWitness::Marker { life_id: id(9) };
    assert_eq!(select_witness(&facts), Some(marker));
    facts = WitnessFacts {
        unpublished: vec![("d/new".to_string(), id(6))],
        ..WitnessFacts::default()
    };
    let staged = DirectoryWitness::Unpublished {
        key: "d/new".to_string(),
        token: id(6),
    };
    assert_eq!(select_witness(&facts), Some(staged));
    assert_eq!(select_witness(&WitnessFacts::default()), None);
}

#[test]
fn witness_advances_dependents() {
    let marker = DirectoryWitness::Marker { life_id: id(1) };
    let child = DirectoryWitness::Descendant {
        key: "d/a".to_string(),
        life_id: id(2),
    };
    let survivor = DirectoryWitness::Descendant {
        key: "d/b".to_string(),
        life_id: id(3),
    };
    // A marker never rests on a child; a descendant witness moves to the chosen replacement.
    assert_eq!(advance_witness(&marker, "d/a", None), Some(marker.clone()));
    assert_eq!(
        advance_witness(&child, "d/a", Some(&survivor)),
        Some(survivor.clone())
    );
    assert_eq!(advance_witness(&child, "d/z", None), Some(child.clone()));
    // Without a replacement the old witness is never swapped for a different proof.
    assert_eq!(advance_witness(&child, "d/a", None), None);
    assert!(snapshot_of("d", &marker).is_some());
}

#[test]
fn identity_proofs_hold() {
    let committed = HandleIdentity::File(FileIdentity::Committed { life_id: id(4) });
    let pointer = |life| CurrentVersionPointer::new(id(2)).with_life(life);
    assert!(identity_holds(
        &committed,
        Some(&pointer(Some(id(4)))),
        None
    ));
    // Recreation under the same name gives a new life; the old handle does not attach.
    assert!(!identity_holds(
        &committed,
        Some(&pointer(Some(id(5)))),
        None
    ));
    let unpublished = HandleIdentity::File(FileIdentity::Unpublished {
        token: id(7),
        predecessor: Some(id(2)),
    });
    assert!(identity_holds(
        &unpublished,
        Some(&pointer(None)),
        Some(id(7))
    ));
    // A put then a version delete back to the old marker loses the token.
    assert!(!identity_holds(&unpublished, Some(&pointer(None)), None));
    assert!(!identity_holds(
        &unpublished,
        Some(&pointer(Some(id(4)))),
        Some(id(7))
    ));
    assert_eq!(
        proof_key(
            "d",
            &HandleIdentity::Directory(DirectoryWitness::Marker { life_id: id(1) })
        ),
        "d/"
    );
}

#[test]
fn tombstones_name_errors() {
    let stale = VfsHandleRow::Tombstone {
        reason: RetireReason::Stale,
        retired_at_ms: 1,
    };
    let expired = VfsHandleRow::Tombstone {
        reason: RetireReason::Expired,
        retired_at_ms: 1,
    };
    assert_eq!(row_state(Some(stale)), Err(VfsError::StaleHandle));
    assert_eq!(row_state(Some(expired)), Err(VfsError::ExpiredHandle));
    // An authenticated id whose row was collected is an expired volatile identity.
    assert_eq!(row_state(None), Err(VfsError::ExpiredHandle));
}

#[test]
fn detached_serves_holders() {
    let summary = InstanceSummary {
        instance_id: 1,
        key: "a".to_string(),
        kind: aruna_core::structs::storage::vfs::HandleKind::File,
        handle_id: Some(3),
        identity: HandleIdentity::File(FileIdentity::Committed { life_id: id(4) }),
        logical_size: 0,
        base_limit: 0,
        dirty: false,
        delete_pending: false,
        disposition_owner: None,
        open_count: 1,
        writer_count: 0,
        detached: Some(aruna_core::structs::storage::vfs::DetachReason::Unlinked),
        stage_id: None,
        generation: 0,
        change: 0,
    };
    assert!(detached_allows(&summary, HandleUse::ExistingOpen));
    for usage in [
        HandleUse::NewOpen,
        HandleUse::ClaimHandle,
        HandleUse::Anonymous,
    ] {
        assert!(!detached_allows(&summary, usage));
    }
}

fn claim() -> HandleClaim {
    HandleClaim {
        scope: scope(),
        key: "a.txt".to_string(),
        identity: HandleIdentity::File(FileIdentity::Committed { life_id: id(4) }),
        mode: 0o644,
    }
}

#[test]
fn claim_allocates_durably() {
    let mut operation = ClaimHandleOperation::new(claim(), 11, 50);
    assert_eq!(operation.transaction(), TransactionMode::Write);
    let Ok(Next::Call(IoCall::Read(reads))) = operation.first() else {
        panic!("the forward, boot and identity rows are read first");
    };
    assert_eq!(reads.len(), 4);
    let boot = VfsBootState::default().to_bytes().unwrap();
    let rows = vec![None, Some(Value::from(boot)), Some(head(2, Some(4))), None];
    let turn = operation.next(IoResult::Values(rows));
    let Ok(Next::Call(IoCall::Write(writes))) = &turn else {
        panic!("expected the allocation rows, got {turn:?}");
    };
    // The boot high-water marks, the handle and its reverse row commit together.
    let stored = VfsBootState::from_bytes(&writes[0].2).unwrap();
    assert_eq!((stored.next_handle_id, stored.next_cookie_id), (2, 4));
    let handle = VfsHandleRow::from_bytes(&writes[1].2).unwrap();
    let VfsHandleRow::Live(live) = handle else {
        panic!("a live handle row");
    };
    assert_eq!((live.cookie_id, live.attribute_change), (3, 11));
    let name = state_key(&VfsKey::Name(scope(), "a.txt".to_string())).unwrap();
    assert_eq!(writes[2].1, name);
    let allocated = operation.next(IoResult::Done).unwrap();
    let expected = HandleAllocation {
        handle_id: 1,
        cookie_id: 3,
        created: true,
    };
    assert_eq!(allocated, Next::Done(expected));
}

#[test]
fn claim_finds_existing() {
    // A row another caller committed for the same identity is returned as it is.
    let mut operation = ClaimHandleOperation::new(claim(), 11, 50);
    operation.first().unwrap();
    let name = Value::from(encode_handle_id(5).to_vec());
    let rows = vec![Some(name), None, Some(head(2, Some(4))), None];
    operation.next(IoResult::Values(rows)).unwrap();
    let row = VfsHandleRow::Live(live("a.txt", claim().identity));
    let row = Value::from(row.to_bytes().unwrap());
    let found = operation.next(IoResult::Values(vec![Some(row)])).unwrap();
    let expected = HandleAllocation {
        handle_id: 5,
        cookie_id: 9,
        created: false,
    };
    assert_eq!(found, Next::Done(expected));
}

#[test]
fn claim_retires_stale() {
    // A reverse row bound to an older life is tombstoned; a new id names the new life.
    let mut operation = ClaimHandleOperation::new(claim(), 11, 50);
    operation.first().unwrap();
    let name = Value::from(encode_handle_id(5).to_vec());
    let rows = vec![Some(name), None, Some(head(2, Some(4))), None];
    operation.next(IoResult::Values(rows)).unwrap();
    let old = HandleIdentity::File(FileIdentity::Committed { life_id: id(8) });
    let row = Value::from(VfsHandleRow::Live(live("a.txt", old)).to_bytes().unwrap());
    let turn = operation.next(IoResult::Values(vec![Some(row)]));
    let Ok(Next::Call(IoCall::Write(writes))) = &turn else {
        panic!("expected the allocation rows, got {turn:?}");
    };
    let stale = state_key(&VfsKey::Handle(scope(), 5)).unwrap();
    let tombstone = writes.iter().find(|(_, key, _)| *key == stale).unwrap();
    let tombstone = VfsHandleRow::from_bytes(&tombstone.2).unwrap();
    assert!(matches!(
        tombstone,
        VfsHandleRow::Tombstone {
            reason: RetireReason::Stale,
            retired_at_ms: 50
        }
    ));
}

#[test]
fn claim_refuses_change() {
    let mut operation = ClaimHandleOperation::new(claim(), 11, 50);
    operation.first().unwrap();
    let rows = vec![None, None, Some(head(2, Some(9))), None];
    assert_eq!(
        operation.next(IoResult::Values(rows)),
        Err(VfsError::StaleHandle)
    );
}

#[test]
fn allocation_syncs_first() {
    let mut operation = AllocateHandleOperation::new(claim());
    assert_eq!(
        asked(&operation.first()),
        VfsEffect::AcquirePermit { scope: scope() }
    );
    let turn = operation.next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }));
    assert!(matches!(asked(&turn), VfsEffect::NextChange { .. }));
    let turn = operation.next(vfs(VfsEvent::ChangeValue { value: 11 }));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let allocation = HandleAllocation {
        handle_id: 1,
        cookie_id: 3,
        created: true,
    };
    // The id leaves only after a sync, and the permit goes after the outcome.
    assert_eq!(
        operation.next(child(allocation)),
        Ok(Next::Call(IoCall::Sync))
    );
    let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
        panic!("expected the outcome with the permit release");
    };
    assert_eq!(output, allocation);
    assert_eq!(calls.len(), 1);
    assert!(operation.abort().is_empty());
}

#[test]
fn retire_keeps_position() {
    let handle = live("p/a.txt", claim().identity);
    let name = Value::from(encode_handle_id(5).to_vec());
    let (writes, deletes) =
        retire_rows(&scope(), 5, &handle, RetireReason::Stale, 60, Some(&name)).unwrap();
    assert_eq!(writes.len(), 2);
    let position = aruna_core::structs::storage::vfs::CookiePosition::from_bytes(&writes[1].2);
    let position = position.unwrap();
    assert_eq!(
        (position.directory_key.as_str(), position.entry_key.as_str()),
        ("p", "p/a.txt")
    );
    assert_eq!(deletes.len(), 1);
    // A reverse row that names another handle stays.
    let other = Value::from(encode_handle_id(6).to_vec());
    let (_, deletes) =
        retire_rows(&scope(), 5, &handle, RetireReason::Stale, 60, Some(&other)).unwrap();
    assert!(deletes.is_empty());
}

fn listing(resume: ListResume) -> VfsListOperation {
    VfsListOperation::new(ListRequest {
        context: context(),
        directory: "d".to_string(),
        witness: None,
        resume,
        max_entries: 10,
        deadline_ms: None,
    })
}

#[test]
fn reserved_cookies_refused() {
    for cookie in [1, 2] {
        let mut operation = listing(ListResume::Cookie {
            cookie,
            name: "a".to_string(),
        });
        operation.first().unwrap();
        assert_eq!(operation.next(granted()), Err(VfsError::BadCookie));
    }
}

#[test]
fn displaced_cookie_resumes() {
    // A cookie whose name moved resumes at its old position in this directory only.
    let mut operation = listing(ListResume::Cookie {
        cookie: 12,
        name: "gone".to_string(),
    });
    operation.first().unwrap();
    let turn = operation.next(granted());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let position = aruna_core::structs::storage::vfs::CookiePosition {
        directory_key: "d".to_string(),
        entry_key: "d/gone".to_string(),
        retired_at_ms: 1,
    };
    let position = Value::from(position.to_bytes().unwrap());
    let turn = operation.next(IoResult::Values(vec![None, Some(position)]));
    assert!(matches!(asked(&turn), VfsEffect::ListStaged { .. }));

    let mut elsewhere = listing(ListResume::Cookie {
        cookie: 12,
        name: "gone".to_string(),
    });
    elsewhere.first().unwrap();
    elsewhere.next(granted()).unwrap();
    let position = aruna_core::structs::storage::vfs::CookiePosition {
        directory_key: "other".to_string(),
        entry_key: "other/gone".to_string(),
        retired_at_ms: 1,
    };
    let position = Value::from(position.to_bytes().unwrap());
    let turn = elsewhere.next(IoResult::Values(vec![None, Some(position)]));
    assert_eq!(turn, Err(VfsError::BadCookie));
}

#[test]
fn staged_names_merge() {
    // Staged names below the directory list in exact order with the page; a deeper staged
    // name makes its first component a directory.
    let mut operation = listing(ListResume::Start);
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    let staged = |key: &str| StagedEntry {
        key: key.to_string(),
        instance_id: 1,
        handle_id: Some(4),
        kind: aruna_core::structs::storage::vfs::HandleKind::File,
        size: 3,
        dirty: true,
        delete_pending: false,
        unpublished_token: Some(id(6)),
        change: 1,
    };
    let entries = vec![
        staged("d/new.txt"),
        staged("d/sub/deep.txt"),
        staged("d/con\u{7}"),
    ];
    let turn = operation.next(vfs(VfsEvent::StagedList {
        entries,
        sequence: 4,
    }));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let page = crate::s3::object::visible::VisiblePage::<crate::s3::object::list::ListedObject> {
        prefixes: vec!["d/old/".to_string()],
        ..Default::default()
    };
    type Failure = crate::s3::object::visible::VisibleError<
        VfsError,
        crate::s3::object::list::ListBucketError,
    >;
    let answer = IoResult::Child(ChildOutcome::new(Ok::<_, Failure>(page)));
    let Ok(Next::Done(listed)) = operation.next(answer) else {
        panic!("expected the listing");
    };
    let names: Vec<_> = listed
        .entries
        .iter()
        .map(|entry| entry.name.as_str())
        .collect();
    assert_eq!(names, ["new.txt", "old", "sub"]);
    assert_eq!(listed.next, ListNext::End);
    assert_eq!(listed.sequence, 4);
    assert!(listed.entries[0].staged && listed.entries[0].handle_id == Some(4));
}
