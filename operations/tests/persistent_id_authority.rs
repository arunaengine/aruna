// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
//! The PID authority on a realm sized above the replication factor.
//!
//! The mapping keyspace is deliberately not the registry row: the row is deleted
//! with the document while the mapping has to survive it to serve a permanent 410.
//! These tests prove that every node answers the same way regardless of which one
//! receives the request, that `Withdrawn` is terminal against a racing mint, and
//! that a node which cannot reach the authority reports unavailable rather than
//! inventing a local 404.

mod topology;

use aruna_core::StructuredId;
use aruna_core::structs::{MintPersistentIdSpec, PersistentIdStatus, PlacementRef, pid_dedup_key};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_local_document,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::load_metadata_record_by_document;
use aruna_operations::jobs::service::submit_mint_pid;
use aruna_operations::jobs::store::find_dedup_job;
use aruna_operations::metadata::PersistentIdResolution;
use aruna_operations::metadata::api::MetadataApiError;
use aruna_operations::metadata::forward::{
    delete_metadata_document_routed, mint_pid_routed, resolve_pid_routed, withdraw_pid_routed,
};
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::persistent_id::read_mapping;
use ulid::Ulid;

use topology::{TestNode, TestResult, Topology, wait_for_convergence, wait_until};

const MANAGEMENT_NODES: usize = 5;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 3;
const JOB_RETENTION_MS: u64 = 60_000;

/// Mint from a node that holds nothing: the mapping must land on the document's
/// holders and never in the forwarder's own store, and every node must resolve it.
#[tokio::test]
async fn mint_routes_holder() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let (document_id, placement) = seed_document(&realm, group_id, "datasets/minted", true).await?;
    let forwarder = realm.non_holder(&placement);
    let holders = realm.assert_not_holder(forwarder.node_id(), &placement);

    let (mapping, minted) = mint_routed(&realm, forwarder, document_id).await?;
    assert!(minted);
    assert_eq!(mapping.status, PersistentIdStatus::Active);

    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("mint reaches every holder", node.node_id(), || {
            mapping_active(node, document_id)
        })
        .await?;
    }
    assert!(
        read_mapping(forwarder.context.as_ref(), document_id)
            .await?
            .is_none(),
        "the forwarder must not mint into its own store"
    );

    for node in realm.nodes.iter() {
        assert_eq!(
            resolve_until_answer(&realm, node, document_id).await?,
            PersistentIdResolution::Redirect,
            "every node resolves a minted, visible document the same way"
        );
    }

    realm.shutdown().await;
    Ok(())
}

/// POST acceptance, then a DELETE of the PID before the worker runs, then the
/// worker's mint: the terminal state and every node's landing answer stay
/// withdrawn. A tombstone is written even though nothing was ever minted.
#[tokio::test]
async fn withdraw_precedes_mint() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let (document_id, placement) = seed_document(&realm, group_id, "datasets/raced", true).await?;
    let forwarder = realm.non_holder(&placement);

    // The submitted job is accepted here; the worker's mint runs after the
    // withdrawal below, exactly as the race orders it.
    let submitted = submit_mint_pid(
        realm.node(0).context.as_ref(),
        MintPersistentIdSpec {
            document_id,
            minted_by: realm.user_id,
        },
        realm.node(0).node_id(),
        JOB_RETENTION_MS,
    )
    .await?;
    assert!(submitted.created);

    let withdrawn = withdraw_routed(&realm, forwarder, document_id).await?;
    assert_eq!(withdrawn.status, PersistentIdStatus::Withdrawn);
    assert_eq!(withdrawn.minted_at_ms, None);

    let (mapping, minted) = mint_routed(&realm, forwarder, document_id).await?;
    assert!(!minted, "a mint may never replace a tombstone");
    assert_eq!(mapping.status, PersistentIdStatus::Withdrawn);

    for holder in realm.holders(&placement) {
        let node = realm.find(holder);
        wait_until("the tombstone reaches every holder", node.node_id(), || {
            mapping_withdrawn(node, document_id)
        })
        .await?;
    }
    for node in realm.nodes.iter() {
        assert!(
            matches!(
                resolve_until_answer(&realm, node, document_id).await?,
                PersistentIdResolution::Gone { .. }
            ),
            "a withdrawn PID is a permanent 410 on every node"
        );
    }

    realm.shutdown().await;
    Ok(())
}

/// Deleting the document withdraws its mapping on the holder that removed the
/// registry row, and the mapping outlives that row so the PID stays a 410.
#[tokio::test]
async fn delete_withdraws_mapping() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let (document_id, placement) = seed_document(&realm, group_id, "datasets/doomed", true).await?;
    let forwarder = realm.non_holder(&placement);
    let holder = realm.holder(&placement);

    mint_routed(&realm, forwarder, document_id).await?;
    wait_until("mint reaches the holder", holder.node_id(), || {
        mapping_active(holder, document_id)
    })
    .await?;

    let record = registry_record(holder, document_id)
        .await
        .expect("the holder carries the registry row");
    delete_metadata_document_routed(
        &holder.context,
        realm.actor(holder),
        Some(&record),
        document_id,
        Some(realm.bearer_token()),
    )
    .await?;

    let mapping = read_mapping(holder.context.as_ref(), document_id)
        .await?
        .expect("the mapping survives the registry row it can no longer be derived from");
    assert_eq!(mapping.status, PersistentIdStatus::Withdrawn);
    assert!(mapping.minted_at_ms.is_some());
    assert!(registry_record(holder, document_id).await.is_none());
    assert!(matches!(
        resolve_until_answer(&realm, holder, document_id).await?,
        PersistentIdResolution::Gone { .. }
    ));

    realm.shutdown().await;
    Ok(())
}

/// The landing path is unauthenticated, so it may not become an existence oracle:
/// a minted PID for a document that is not anonymously readable resolves as absent,
/// while an anonymously readable one redirects.
#[tokio::test]
async fn resolve_gates_visibility() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let (private_id, private_placement) =
        seed_document(&realm, group_id, "datasets/private", false).await?;
    let (public_id, public_placement) =
        seed_document(&realm, group_id, "datasets/public", true).await?;

    let private_holder = realm.holder(&private_placement);
    let public_holder = realm.holder(&public_placement);
    assert!(mint_routed(&realm, private_holder, private_id).await?.1);
    assert!(mint_routed(&realm, public_holder, public_id).await?.1);

    assert_eq!(
        resolve_until_answer(&realm, private_holder, private_id).await?,
        PersistentIdResolution::Missing,
        "a private document's PID must not resolve anonymously"
    );
    assert_eq!(
        resolve_until_answer(&realm, public_holder, public_id).await?,
        PersistentIdResolution::Redirect
    );

    realm.shutdown().await;
    Ok(())
}

/// With every holder down, a non-holder must report the authority unreachable.
/// Answering 404 from its own empty store would turn a live PID into a dead one.
#[tokio::test]
async fn holder_loss_unavailable() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let (document_id, placement) =
        seed_document(&realm, group_id, "datasets/offline", true).await?;
    let forwarder = realm.non_holder(&placement);
    let holders = realm.assert_not_holder(forwarder.node_id(), &placement);

    mint_routed(&realm, forwarder, document_id).await?;
    for holder in &holders {
        realm.find(*holder).net.shutdown().await;
    }

    assert!(
        matches!(
            resolve_pid_routed(&forwarder.context, realm.realm_id, document_id).await,
            Err(MetadataApiError::ServiceUnavailable)
        ),
        "an unreachable authority is unavailable, never a local 404"
    );

    realm.shutdown().await;
    Ok(())
}

/// Two authorized users minting the same document produce one job identity: the
/// dedup row is keyed by the document, not by the submitting user.
#[tokio::test]
async fn mint_dedups_users() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let (document_id, _) = seed_document(&realm, group_id, "datasets/shared", true).await?;
    let ingress = realm.node(0);
    let other = aruna_core::UserId::local(Ulid::generate(), realm.realm_id);

    let first = submit_mint_pid(
        ingress.context.as_ref(),
        MintPersistentIdSpec {
            document_id,
            minted_by: realm.user_id,
        },
        ingress.node_id(),
        JOB_RETENTION_MS,
    )
    .await?;
    let second = submit_mint_pid(
        ingress.context.as_ref(),
        MintPersistentIdSpec {
            document_id,
            minted_by: other,
        },
        ingress.node_id(),
        JOB_RETENTION_MS,
    )
    .await?;

    assert!(first.created);
    assert!(!second.created, "the second user joins the first job");
    assert_eq!(first.job_id, second.job_id);
    // Either user resolves the reservation, which is what makes it document-global.
    for user in [realm.user_id, other] {
        assert_eq!(
            find_dedup_job(
                &ingress.context.storage_handle,
                user,
                &pid_dedup_key(document_id),
                None,
            )
            .await?,
            Some(first.job_id)
        );
    }

    realm.shutdown().await;
    Ok(())
}

async fn mint_routed(
    realm: &Topology,
    node: &TestNode,
    document_id: Ulid,
) -> TestResult<(aruna_core::structs::PersistentIdMapping, bool)> {
    Ok(mint_pid_routed(
        &node.context,
        realm.realm_id,
        document_id,
        realm.user_id,
        aruna_core::util::unix_timestamp_millis(),
        Some(realm.bearer_token()),
    )
    .await?)
}

async fn withdraw_routed(
    realm: &Topology,
    node: &TestNode,
    document_id: Ulid,
) -> TestResult<aruna_core::structs::PersistentIdMapping> {
    Ok(withdraw_pid_routed(
        &node.context,
        realm.realm_id,
        document_id,
        aruna_core::util::unix_timestamp_millis(),
        Some(realm.bearer_token()),
    )
    .await?)
}

/// A routed resolve, retried while the fan-out reports it unavailable: the first
/// request pays a cold metadata dial inside the fan-out's per-peer slot.
async fn resolve_until_answer(
    realm: &Topology,
    node: &TestNode,
    document_id: Ulid,
) -> TestResult<PersistentIdResolution> {
    let answer = std::cell::RefCell::new(None);
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "no routed resolve reached the authority",
        || async {
            match resolve_pid_routed(&node.context, realm.realm_id, document_id).await {
                Err(MetadataApiError::ServiceUnavailable) => Ok(1),
                other => {
                    *answer.borrow_mut() = Some(other?);
                    Ok(0)
                }
            }
        },
    )
    .await?;
    Ok(answer
        .into_inner()
        .ok_or("the routed resolve produced no answer")?)
}

async fn mapping_active(node: &TestNode, document_id: Ulid) -> bool {
    matches!(
        read_mapping(node.context.as_ref(), document_id).await,
        Ok(Some(mapping)) if mapping.status == PersistentIdStatus::Active
    )
}

async fn mapping_withdrawn(node: &TestNode, document_id: Ulid) -> bool {
    matches!(
        read_mapping(node.context.as_ref(), document_id).await,
        Ok(Some(mapping)) if mapping.status == PersistentIdStatus::Withdrawn
    )
}

/// Creates a document on a node that holds its bucket and waits for the registry
/// row to reach every sync-eligible node, which is what routing runs on.
async fn seed_document(
    realm: &Topology,
    group_id: Ulid,
    path: &str,
    public: bool,
) -> TestResult<(Ulid, PlacementRef)> {
    // The id must be a structured MetaResourceId: routing resolves the mapping's
    // placement from the id itself, never from the deletable registry row.
    let origin = realm.node(0);
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    realm
        .origin_placement(origin, group_id, document_id, path)
        .ok_or("a Management node holds buckets")?;
    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: realm.actor(origin),
            group_id,
            document_id,
            document_path: path.to_string(),
            public,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "PID Authority Dataset".to_string(),
                description: "Written on a realm above the replication factor".to_string(),
                date_published: "2026-01-01".to_string(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            },
        }),
        origin.context.as_ref(),
    )
    .await?;
    replay_metadata_event_log(origin.context.as_ref()).await?;
    let placement = created.record.placement;

    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "the seeded document's registry row never reached every sync-eligible node",
        || async {
            let mut pending = 0;
            for node in realm.nodes.iter().filter(|node| node.is_sync_eligible()) {
                if !registry_present(node.context.as_ref(), document_id).await {
                    pending += 1;
                }
            }
            Ok(pending)
        },
    )
    .await?;
    Ok((document_id, placement))
}

async fn registry_present(context: &DriverContext, document_id: Ulid) -> bool {
    load_metadata_record_by_document(context, document_id)
        .await
        .is_ok_and(|record| record.is_some())
}

async fn registry_record(
    node: &TestNode,
    document_id: Ulid,
) -> Option<aruna_core::structs::MetadataRegistryRecord> {
    load_metadata_record_by_document(node.context.as_ref(), document_id)
        .await
        .ok()
        .flatten()
}
