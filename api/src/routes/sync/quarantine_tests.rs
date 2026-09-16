use super::*;
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentSyncRevision, DocumentTarget,
};
use aruna_core::effects::StorageEffect;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::{
    SyncQuarantineEvidence, SyncQuarantineIdentity, SyncQuarantineInput, SyncQuarantineUsage,
    build_quarantine_entries, quarantine_usage_entry,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_storage::storage::FjallStorage;
use aruna_tasks::TaskHandle;
use ulid::Ulid;

struct Fixture {
    _dir: tempfile::TempDir,
    state: Arc<ServerState>,
    admin: AuthContext,
    realm_id: RealmId,
}

async fn setup() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    let realm_id = RealmId::from_bytes(
        ed25519_dalek::SigningKey::from_bytes(&[31u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
    let admin_id = UserId::local(Ulid::from_bytes([8u8; 16]), realm_id);
    let actor = Actor {
        node_id,
        user_id: admin_id,
        realm_id,
    };
    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: actor.clone(),
            realm_description: "quarantine".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
        context.as_ref(),
    )
    .await
    .unwrap();
    drive(
        ClaimInitialOperation::new(ClaimInitialInput { actor }),
        context.as_ref(),
    )
    .await
    .unwrap();
    let state = Arc::new(
        ServerState::new(
            context,
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await,
    );
    Fixture {
        _dir: dir,
        state,
        admin: AuthContext {
            user_id: admin_id,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        realm_id,
    }
}

fn event(index: u8) -> DocumentEvent {
    DocumentEvent::Delete {
        event_id: Ulid::from_bytes([index; 16]),
        target: DocumentTarget::RealmConfig {
            realm_id: RealmId([9; 32]),
        },
        change: DocumentChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_bytes([index; 16]),
                actor: NodeId::from_bytes(&[1u8; 32]).unwrap(),
                updated_at_ms: 1,
            },
            kind: DocumentChangeKind::Delete,
            placement: PlacementRef::NIL,
        },
    }
}

fn identity(index: u8) -> SyncQuarantineIdentity {
    SyncQuarantineIdentity::from_parts([7; 32], [8; 32], u64::from(index) + 1)
}

async fn seed_rows(fx: &Fixture, count: u8) {
    let ctx = fx.state.get_ctx();
    let mut usage = SyncQuarantineUsage::default();
    for index in 0..count {
        let write = build_quarantine_entries(
            SyncQuarantineInput {
                identity: identity(index),
                evidence: SyncQuarantineEvidence::from_event(&event(index)),
                reason: "unauthorized",
                quarantined_at_ms: 42,
                replaced_bytes: None,
            },
            usage,
            SyncQuarantineCapacity::default(),
        )
        .unwrap();
        usage = write.usage;
        ctx.storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes: vec![write.row, quarantine_usage_entry(usage).unwrap()],
                txn_id: None,
            })
            .await;
    }
}

#[tokio::test]
async fn admin_lists_records() {
    let fx = setup().await;
    seed_rows(&fx, 3).await;

    let (_, Json(page)) = list_quarantine(
        State(fx.state.clone()),
        Extension(Some(fx.admin.clone())),
        Query(QuarantineQuery {
            cursor: None,
            topic: None,
            limit: Some(2),
        }),
    )
    .await
    .unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.usage.records, 3);
    assert!(page.next_cursor.is_some());
    assert_eq!(page.records[0].family.as_deref(), Some("delete"));

    let (_, Json(inspected)) = inspect_quarantine(
        State(fx.state.clone()),
        Extension(Some(fx.admin.clone())),
        Path(page.records[0].id.clone()),
    )
    .await
    .unwrap();
    assert_eq!(inspected.record.id, page.records[0].id);
    assert!(inspected.event.is_some());
}

#[tokio::test]
async fn admin_acknowledges_row() {
    let fx = setup().await;
    seed_rows(&fx, 2).await;
    let (_, Json(page)) = list_quarantine(
        State(fx.state.clone()),
        Extension(Some(fx.admin.clone())),
        Query(QuarantineQuery::default()),
    )
    .await
    .unwrap();
    let id = page.records[0].id.clone();

    for _ in 0..2 {
        let (_, Json(record)) = acknowledge_quarantine(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Path(id.clone()),
        )
        .await
        .unwrap();
        assert!(record.acknowledged);
    }

    let (_, Json(pruned)) = prune_quarantine(
        State(fx.state.clone()),
        Extension(Some(fx.admin.clone())),
        Query(QuarantineQuery::default()),
    )
    .await
    .unwrap();
    assert_eq!(pruned.pruned, 1);
    assert_eq!(pruned.usage.records, 1);
    assert_eq!(
        pruned.usage.max_records,
        SyncQuarantineCapacity::default().max_records
    );
}

#[tokio::test]
async fn anonymous_is_rejected() {
    let fx = setup().await;
    for result in [
        list_quarantine(
            State(fx.state.clone()),
            Extension(None),
            Query(QuarantineQuery::default()),
        )
        .await
        .err(),
        prune_quarantine(
            State(fx.state.clone()),
            Extension(None),
            Query(QuarantineQuery::default()),
        )
        .await
        .err(),
    ] {
        assert!(matches!(result, Some(ServerError::Unauthorized)));
    }
    let inspected = inspect_quarantine(
        State(fx.state.clone()),
        Extension(None),
        Path("00".to_string()),
    )
    .await;
    assert!(matches!(inspected, Err(ServerError::Unauthorized)));
}

#[tokio::test]
async fn stranger_is_rejected() {
    let fx = setup().await;
    seed_rows(&fx, 1).await;
    let stranger = AuthContext {
        user_id: UserId::local(Ulid::from_bytes([77; 16]), fx.realm_id),
        realm_id: fx.realm_id,
        path_restrictions: None,
        session: None,
    };
    let listed = list_quarantine(
        State(fx.state.clone()),
        Extension(Some(stranger.clone())),
        Query(QuarantineQuery::default()),
    )
    .await;
    assert!(matches!(listed, Err(ServerError::Forbidden)));

    let acknowledged = acknowledge_quarantine(
        State(fx.state.clone()),
        Extension(Some(stranger)),
        Path("00".to_string()),
    )
    .await;
    assert!(matches!(acknowledged, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn restricted_token_rejected() {
    let fx = setup().await;
    let delegated = AuthContext {
        user_id: fx.admin.user_id,
        realm_id: fx.realm_id,
        path_restrictions: Some(Vec::new()),
        session: None,
    };
    let listed = list_quarantine(
        State(fx.state.clone()),
        Extension(Some(delegated)),
        Query(QuarantineQuery::default()),
    )
    .await;
    assert!(matches!(listed, Err(ServerError::Forbidden)));
}
