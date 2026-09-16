use super::{DeviceDraft, preview_draft};
use crate::error::ServerError;
use crate::metadata::ProfilePreviewRequest;
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_operations::device::publish_queue::{PublishEntry, PublishState};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::metadata::MetadataHandle;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use axum::extract::State;
use axum::{Extension, Json};
use std::sync::Arc;
use ulid::Ulid;

/// A user node enrolled for `owner`, holding only the realm configuration.
async fn user_node() -> (tempfile::TempDir, Arc<ServerState>, AuthContext) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let owner = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
    let metadata = MetadataHandle::new(
        dir.path().join("metadata"),
        node_id,
        storage.clone(),
        None,
        None,
        None,
    )
    .unwrap();
    let state = Arc::new(
        ServerState::new(
            Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: Some(metadata),
                task_handle: Some(TaskHandle::new()),
                compute_handle: None,
            }),
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await,
    );
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(node_id, RealmNodeKind::User { owner });
    let event = state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: realm_id.as_bytes().to_vec().into(),
            value: config.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    let auth = AuthContext {
        user_id: owner,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    (dir, state, auth)
}

#[tokio::test]
async fn preview_refuses_group() {
    // Owning the device does not prove READ on an arbitrary group's Profiles.
    let (_dir, state, auth) = user_node().await;

    let result = preview_draft(
        State(state),
        Extension(Some(auth)),
        Json(ProfilePreviewRequest {
            rocrate: serde_json::json!({
                "@context": "https://w3id.org/ro/crate/1.2/context",
                "@graph": []
            }),
            group_id: Some(Ulid::generate().to_string()),
            public: false,
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn preview_marks_unchecked() {
    let (_dir, state, auth) = user_node().await;
    let file = format!("https://w3id.org/aruna/data/{}", "09".repeat(32));
    let (_, Json(preview)) = preview_draft(
            State(state), Extension(Some(auth)), Json(ProfilePreviewRequest {
                rocrate: serde_json::json!({
                    "@context": "https://w3id.org/ro/crate/1.3/context",
                    "@graph": [
                        {"@id": "ro-crate-metadata.json", "@type": "CreativeWork",
                         "about": {"@id": "./"}, "conformsTo": {"@id": "https://w3id.org/ro/crate/1.3"}},
                        {"@id": "./", "@type": "Dataset", "name": "Draft", "description": "Draft",
                         "datePublished": "2026-09-08", "hasPart": {"@id": file}},
                        {"@id": file, "@type": "File"}
                    ]
                }),
                group_id: None, public: true,
            }),
        ).await.unwrap();
    assert_eq!(preview.restricted_files_complete, Some(false));
    assert!(preview.restricted_files.is_empty());
    assert_eq!(
        serde_json::to_value(preview).unwrap()["restricted_files_complete"],
        false
    );
}

fn entry() -> PublishEntry {
    PublishEntry::new(
        Ulid::generate(),
        UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32])),
        Ulid::generate(),
        "/notes".to_string(),
        true,
        "{}".to_string(),
    )
}

#[test]
fn maps_draft_states() {
    // The desktop reads the lifecycle from `status`, never from a flag mix.
    let document_id = Ulid::generate();
    let mut source = entry();
    let pending: DeviceDraft = source.clone().into();
    assert_eq!(pending.status, "pending");
    assert!(pending.document_id.is_none());

    source.state = PublishState::Publishing {
        document_id,
        due_at_ms: 0,
        attempts: 2,
    };
    let publishing: DeviceDraft = source.clone().into();
    assert_eq!(publishing.status, "publishing");
    assert_eq!(
        publishing.document_id.as_deref(),
        Some(document_id.to_string().as_str())
    );

    source.state = PublishState::Failed {
        reason: "denied".to_string(),
        retryable: false,
        document_id: None,
    };
    let failed: DeviceDraft = source.into();
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.retryable, Some(false));
}
