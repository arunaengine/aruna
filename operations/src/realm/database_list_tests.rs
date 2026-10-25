// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::UserId;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, DATABASE_REQUEST_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::database::DatabaseRequest;
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument};

use super::*;
use crate::distribution::test_record::{realm, record, store_record};
use crate::distribution::test_store::{context, open};

fn caller(user_id: UserId) -> Caller {
    Caller {
        auth: AuthContext {
            user_id,
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        },
        bearer: None,
    }
}

fn entry(set_id: Ulid, owner_group: Ulid) -> DatabaseEntry {
    DatabaseEntry {
        entry_id: Ulid::from_bytes([73; 16]),
        dataset_id: Ulid::from_bytes([74; 16]),
        target: DatabaseTarget::Distributed { set_id },
        owner_group,
    }
}

async fn write(context: &DriverContext, writes: Vec<(&str, Vec<u8>, Vec<u8>)>) {
    let writes = (writes.into_iter())
        .map(|(space, key, value)| (space.to_string(), key.into(), value.into()))
        .collect();
    let batch = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    assert!(matches!(
        context.storage_handle.send_storage_effect(batch).await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

#[tokio::test]
async fn unlinked_stays_unlinked() {
    // An entry once seen unlinked stays so for its version; a fresh mark is looked up again.
    let (_dir, handle) = open();
    let context = context(&handle);
    let node_id = iroh::SecretKey::from_bytes(&[72; 32]).public();
    let entry = entry(Ulid::from_bytes([75; 16]), Ulid::from_bytes([76; 16]));
    let dot = |origin_seq| AdminDocumentDot {
        event_id: Ulid::from_bytes([77; 16]),
        origin_node_id: node_id,
        origin_seq,
    };
    let row = postcard::to_allocvec(&Some(dot(1))).unwrap();
    let key = entry.entry_id.to_bytes().to_vec();
    write(&context, vec![(DATABASE_UNLINKED_KEYSPACE, key, row)]).await;
    let caller = caller(UserId::local(Ulid::from_bytes([78; 16]), realm()));
    let place = (realm(), node_id);
    let seen = version_links(&context, place, &caller, &entry, Some(dot(1))).await;
    assert_eq!(seen.unwrap(), Some(LinkedDataset::Unlinked));
    let fresh = version_links(&context, place, &caller, &entry, Some(dot(2))).await;
    assert!(
        matches!(fresh, Err(DatabaseServiceError::Unavailable(_))),
        "a fresh mark asks the metadata nodes again"
    );
}

#[tokio::test]
async fn approval_rechecks_owner() {
    // A request taken for one group is not approved once the bucket names another owner.
    use crate::realm::databases::approve_request;
    let (_dir, handle) = open();
    let context = context(&handle);
    let (snapshot, bytes) = record(true);
    store_record(&handle, snapshot.set_id(), bytes).await;
    let node_id = iroh::SecretKey::from_bytes(&[79; 32]).public();
    let group_id = Ulid::from_bytes([81; 16]);
    let owner = UserId::local(Ulid::from_bytes([82; 16]), realm());
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id: realm(),
    };
    let group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm(), group_id);
    let group = Group {
        display_name: "owners".to_string(),
        group_id,
        realm_id: realm(),
        roles: group_auth.roles.keys().copied().collect::<HashSet<_>>(),
        owner,
    };
    let request = DatabaseRequest {
        request_id: Ulid::from_bytes([83; 16]),
        entry: entry(snapshot.set_id(), group_id),
        predecessor: None,
        curator: UserId::local(Ulid::from_bytes([84; 16]), realm()),
        created_at_ms: 0,
    };
    let config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm());
    let realm_key = realm().as_bytes().to_vec();
    let group_key = group_id.to_bytes().to_vec();
    write(
        &context,
        vec![
            (
                REALM_CONFIG_KEYSPACE,
                realm_key.clone(),
                config.to_bytes(&actor).unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                realm_key,
                realm_auth.to_bytes(&actor).unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                group_key.clone(),
                group_auth.to_bytes(&actor).unwrap(),
            ),
            (GROUP_KEYSPACE, group_key, group.to_bytes(&actor).unwrap()),
            (
                DATABASE_REQUEST_KEYSPACE,
                request.request_id.to_bytes().to_vec(),
                request.to_bytes().unwrap(),
            ),
        ],
    )
    .await;
    let result = approve_request(
        &context,
        realm(),
        node_id,
        &caller(owner),
        request.request_id,
    )
    .await;
    assert!(
        matches!(&result, Err(DatabaseServiceError::BadRequest(message)) if message.contains("mark it again")),
        "{result:?}"
    );
}
