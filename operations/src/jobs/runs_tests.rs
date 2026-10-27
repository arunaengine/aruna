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
use crate::jobs::store::put_crate_outcome;

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
        task_handle: None,
        compute_handle: None,
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
    put_crate_outcome(&context.storage_handle, run_id, &status, &link)
        .await
        .unwrap();
    let read = |auth: AuthContext, run_id| {
        let context = &context;
        async move { read_run(context, realm_id, &auth, run_id).await }
    };

    let own = read(auth(submitter, realm_id), run_id).await.unwrap();
    assert_eq!(own.logical_job_id, link.logical_job_id);
    assert_eq!(own.run_crate, Some(document_id));
    let shown = read(auth(reader, realm_id), run_id).await.unwrap();
    assert_eq!(shown.logical_job_id, None);
    assert_eq!(shown.run_crate, Some(document_id));
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
    let unknown = JobId::from_bytes([11; 16]);
    assert!(matches!(
        read(auth(submitter, realm_id), unknown).await,
        Err(RunReadError::NotFound)
    ));
}
