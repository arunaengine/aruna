//! Tests run reads: the unrestricted submitter, run crate readers and everyone else.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};

use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::structs::execution::job::{RunCrateStatus, RunLink};
use aruna_core::structs::identity::auth::{Actor, PathRestriction, Role};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument};

use super::*;
use crate::driver::{DriverContext, drive};
use crate::jobs::store::test_calls::put_crate_outcome;

async fn read_run(
    context: &DriverContext,
    realm_id: RealmId,
    auth: &AuthContext,
    run_id: JobId,
) -> Result<RunView, RunReadError> {
    drive(ReadRunOperation::new(realm_id, auth, run_id), context).await
}

fn link(created_by: UserId, run_crate: Option<Ulid>) -> RunLink {
    RunLink {
        family: None,
        created_by,
        group_id: Ulid::from_bytes([7u8; 16]),
        logical_job_id: Some(JobId::from_bytes([8u8; 16])),
        run_crate,
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
}

fn linked(found: Option<RunLink>) -> IoResult {
    child(Ok::<_, JobMutationError>(found))
}

// An unknown run reads as absent, and the submitter goes on to the crate status.
#[test]
fn submitter_sees_status() {
    let realm_id = RealmId([1u8; 32]);
    let owner = user(1, realm_id);
    let run_id = JobId::from_bytes([2u8; 16]);
    let mut read = ReadRunOperation::new(realm_id, &auth(owner, realm_id), run_id);
    assert!(matches!(read.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(read.next(linked(None)), Err(RunReadError::NotFound));

    let mut read = ReadRunOperation::new(realm_id, &auth(owner, realm_id), run_id);
    let found = linked(Some(link(owner, None)));
    assert!(matches!(read.next(found), Ok(Next::Call(IoCall::Child(_)))));
    let status = child(Ok::<_, JobMutationError>(Some(RunCrateStatus::Pending)));
    let Next::Done(view) = read.next(status).unwrap() else {
        panic!("expected the view");
    };
    assert!(view.logical_job_id.is_some() && view.run_crate_status.is_some());
}

// Another user needs a run crate and READ on it, and sees no submitter details.
#[test]
fn others_need_read() {
    let realm_id = RealmId([1u8; 32]);
    let (owner, other) = (user(1, realm_id), user(3, realm_id));
    let run_id = JobId::from_bytes([2u8; 16]);
    let start = || ReadRunOperation::new(realm_id, &auth(other, realm_id), run_id);
    let mut read = start();
    assert_eq!(
        read.next(linked(Some(link(owner, None)))),
        Err(RunReadError::NotFound)
    );
    let found = || linked(Some(link(owner, Some(Ulid::from_bytes([9u8; 16])))));
    let mut read = start();
    read.next(found()).unwrap();
    let allowed = child(Ok::<_, AuthorizeError>(()));
    let Next::Done(view) = read.next(allowed).unwrap() else {
        panic!("expected the view");
    };
    assert!(view.logical_job_id.is_none() && view.run_crate_status.is_none());
    let mut read = start();
    read.next(found()).unwrap();
    let denied = child(Err::<(), _>(AuthorizeError::PermissionDenied));
    assert_eq!(read.next(denied), Err(RunReadError::NotFound));
}

fn user(seed: u8, realm_id: RealmId) -> UserId {
    UserId::local(Ulid::from_bytes([seed; 16]), realm_id)
}

fn auth(user_id: UserId, realm_id: RealmId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

#[tokio::test]
async fn run_read_rights() {
    let temp = tempfile::tempdir().unwrap();
    let context = DriverContext {
        storage_handle: aruna_storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let realm_id = RealmId::from_bytes([1; 32]);
    let (submitter, reader, other) = (user(2, realm_id), user(3, realm_id), user(4, realm_id));
    let group_id = Ulid::from_bytes([5; 16]);
    let run_id = JobId::from_bytes([6; 16]);
    let document_id = Ulid::from_bytes([7; 16]);
    let crate_path = MetadataRegistryRecord::permission_path_for(
        &realm_id,
        group_id,
        &format!("runs/{run_id}"),
        document_id,
    );
    let role_id = Ulid::from_bytes([8; 16]);
    let group = GroupAuthorizationDocument {
        group_id,
        policies: Vec::new(),
        roles: HashMap::from([(
            role_id,
            Role {
                role_id,
                name: "reader".to_string(),
                permissions: HashMap::from([(crate_path.clone(), Permission::READ)]),
                assigned_users: HashSet::from([reader]),
            },
        )]),
    };
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[9; 32]).public(),
        user_id: submitter,
        realm_id,
    };
    let realm = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    let record = Group {
        display_name: "g".to_string(),
        group_id,
        realm_id,
        roles: HashSet::new(),
        owner: submitter,
    };
    let writes = [
        (
            DocumentTarget::Group { group_id },
            record.to_bytes(&actor).unwrap(),
        ),
        (
            DocumentTarget::RealmConfig { realm_id },
            config.to_bytes(&actor).unwrap(),
        ),
        (
            DocumentTarget::GroupAuthorization { group_id },
            group.to_bytes(&actor).unwrap(),
        ),
        (
            DocumentTarget::RealmAuthorization { realm_id },
            realm.to_bytes(&actor).unwrap(),
        ),
    ];
    for (target, value) in writes {
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: value.into(),
                txn_id: None,
            })
            .await;
    }
    let link = RunLink {
        family: None,
        logical_job_id: Some(JobId::from_bytes([10; 16])),
        group_id,
        created_by: submitter,
        run_crate: Some(document_id),
    };
    let status = RunCrateStatus::Written {
        resource: document_id.to_string(),
    };
    put_crate_outcome(&context, run_id, &status, &link)
        .await
        .unwrap();
    let read = |auth: AuthContext, run_id| {
        let context = &context;
        async move { read_run(context, realm_id, &auth, run_id).await }
    };

    let own = read(auth(submitter, realm_id), run_id).await.unwrap();
    assert_eq!(own.logical_job_id, link.logical_job_id);
    assert_eq!(own.run_crate, Some(document_id));
    assert_eq!(own.run_crate_status, Some(status.to_public_json()));
    let shown = read(auth(reader, realm_id), run_id).await.unwrap();
    assert_eq!(shown.logical_job_id, None);
    assert_eq!(shown.run_crate, Some(document_id));
    assert_eq!(shown.run_crate_status, None);
    assert!(matches!(
        read(auth(other, realm_id), run_id).await,
        Err(RunReadError::NotFound)
    ));
    // A restricted token of the submitter counts only when its paths cover the crate.
    let restricted = |pattern: &str| AuthContext {
        path_restrictions: Some(vec![PathRestriction {
            pattern: pattern.to_string(),
            permission: Permission::READ,
        }]),
        ..auth(submitter, realm_id)
    };
    let elsewhere = format!("/{realm_id}/g/{group_id}/meta/other/**");
    assert!(matches!(
        read(restricted(&elsewhere), run_id).await,
        Err(RunReadError::NotFound)
    ));
    // A denied crate tells the submitter why it is missing.
    let denied_run = JobId::from_bytes([12; 16]);
    let denied = RunCrateStatus::Denied {
        message: "metadata write access denied".to_string(),
    };
    let missing = RunLink {
        run_crate: None,
        ..link.clone()
    };
    put_crate_outcome(&context, denied_run, &denied, &missing)
        .await
        .unwrap();
    let own = read(auth(submitter, realm_id), denied_run).await.unwrap();
    assert_eq!(own.run_crate, None);
    let shown = own.run_crate_status.unwrap();
    assert_eq!(shown["status"], "denied");
    assert_eq!(shown["message"], "metadata write access denied");
    let unknown = JobId::from_bytes([11; 16]);
    assert!(matches!(
        read(auth(submitter, realm_id), unknown).await,
        Err(RunReadError::NotFound)
    ));
}
