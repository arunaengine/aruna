// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
//! The PID authority on a realm sized above the replication factor.
//!
//! The mapping keyspace is deliberately not the registry row: the row is deleted
//! with the document while the mapping has to survive it to serve a permanent 410.
//! These tests prove that every node answers the same way regardless of which one
//! receives the request, that retirement is terminal against a racing mint, and
//! that a node which cannot reach the authority reports unavailable rather than
//! inventing a local 404.

mod topology;

use aruna_core::StructuredId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{METADATA_PENDING_PROJECTION_KEYSPACE, PERSISTENT_ID_MAPPING_KEYSPACE};
use aruna_core::storage_entries::metadata_pending_projection_key;
use aruna_core::structs::{
    JobId, MetadataRegistryRecord, MintPersistentIdSpec, PersistentIdMapping, PersistentIdRevision,
    PersistentIdStatus, PlacementRef, persistent_id_key, pid_dedup_key,
};
use aruna_operations::claim_initial_realm_admin::{
    ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_local_document,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::load_metadata_record_by_document;
use aruna_operations::jobs::service::{read_job_routed, submit_mint_pid};
use aruna_operations::jobs::store::{find_dedup_job, read_job_record};
use aruna_operations::jobs::submit::{SubmitJobError, SubmitJobResult};
use aruna_operations::metadata::PersistentIdResolution;
use aruna_operations::metadata::api::MetadataApiError;
use aruna_operations::metadata::forward::{
    MetadataWriteError, create_metadata_document_routed, delete_metadata_document_routed,
    mint_pid_routed, resolve_pid_routed, withdraw_pid_routed,
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

    // A retried mint whose first attempt lost its response reports no change, so
    // the flag is covered without I/O in `persistent_id::tests` instead.
    let (mapping, _) = mint_routed(&realm, forwarder, document_id).await?;
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
    drive(
        ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
            actor: realm.actor(realm.node(0)),
        }),
        realm.node(0).context.as_ref(),
    )
    .await?;
    let group_id = realm.seed_group().await?;
    let (document_id, placement) = seed_document(&realm, group_id, "datasets/raced", true).await?;
    let forwarder = realm.non_holder(&placement);

    // The submitted job is accepted here; the worker's mint runs after the
    // withdrawal below, exactly as the race orders it.
    submit_routed(&realm, realm.node(0), document_id, realm.user_id).await?;

    let withdrawn = withdraw_routed(&realm, forwarder, document_id).await?;
    assert_eq!(withdrawn.status, PersistentIdStatus::AdminWithdrawn);
    assert_eq!(withdrawn.minted_at_ms, None);

    let (mapping, minted) = mint_routed(&realm, forwarder, document_id).await?;
    assert!(!minted, "a mint may never replace a tombstone");
    assert_eq!(mapping.status, PersistentIdStatus::AdminWithdrawn);

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

/// Deleting the document withdraws its mapping inside the transaction that
/// removes the registry row, and the mapping outlives that row so the PID stays a
/// 410 — including on an authority that has only the delete, not the tombstone.
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
    delete_until_applied(&realm, holder, &record, document_id).await?;

    let mapping = read_mapping(holder.context.as_ref(), document_id)
        .await?
        .expect("the mapping survives the registry row it can no longer be derived from");
    assert_eq!(mapping.status, PersistentIdStatus::Tombstoned);
    assert!(mapping.minted_at_ms.is_some());
    assert!(registry_record(holder, document_id).await.is_none());
    assert!(matches!(
        resolve_until_answer(&realm, holder, document_id).await?,
        PersistentIdResolution::Gone { .. }
    ));

    // The create this node accepted is durable evidence the document existed, so
    // even an active mapping resolves Gone once the row is gone.
    write_mapping(holder, active_mapping(document_id, realm.user_id)).await?;
    assert!(matches!(
        resolve_until_answer(&realm, holder, document_id).await?,
        PersistentIdResolution::Gone { .. }
    ));

    realm.shutdown().await;
    Ok(())
}

/// A delete accepted by another replica must execute on the same rank-0 authority
/// that answers PID landing requests; otherwise the successful delete can race an
/// authoritative redirect until its tombstone happens to replicate.
#[tokio::test]
async fn replica_delete_routes_to_pid_authority() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let (document_id, placement) =
        seed_document(&realm, group_id, "datasets/replica-delete", true).await?;
    let holders = realm.holders(&placement);
    let authority = realm.find(holders[0]);
    let replica = realm.find(holders[1]);

    mint_routed(&realm, authority, document_id).await?;
    wait_until("mint reaches the replica", replica.node_id(), || {
        mapping_active(replica, document_id)
    })
    .await?;
    let record = registry_record(replica, document_id)
        .await
        .expect("the replica carries the registry row");
    delete_until_applied(&realm, replica, &record, document_id).await?;

    let mapping = read_mapping(authority.context.as_ref(), document_id)
        .await?
        .expect("the authority retains the withdrawn mapping");
    assert_eq!(mapping.status, PersistentIdStatus::Tombstoned);
    assert_eq!(mapping.revision.actor, authority.node_id());
    assert!(registry_record(authority, document_id).await.is_none());

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
            resolve_pid_routed(
                &forwarder.context,
                realm.realm_id,
                document_id,
                MetadataRegistryRecord::graph_iri_for(document_id),
            )
            .await,
            Err(MetadataApiError::ServiceUnavailable)
        ),
        "an unreachable authority is unavailable, never a local 404"
    );

    realm.shutdown().await;
    Ok(())
}

/// With the authority down but its replicas up, every other node must report the
/// PID unavailable. A surviving replica answering in its stead is exactly the
/// disagreement that turns a stale row into a redirect or a permanent 410.
#[tokio::test]
async fn authority_loss_unavailable() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let (document_id, placement) =
        seed_document(&realm, group_id, "datasets/partitioned", true).await?;
    let authority = realm.holder(&placement);
    let replicas: Vec<_> = realm
        .holders(&placement)
        .into_iter()
        .filter(|holder| *holder != authority.node_id())
        .collect();

    mint_routed(&realm, authority, document_id).await?;
    for replica in &replicas {
        let node = realm.find(*replica);
        wait_until("mint reaches every replica", node.node_id(), || {
            mapping_active(node, document_id)
        })
        .await?;
    }
    authority.net.shutdown().await;

    for node in realm
        .nodes
        .iter()
        .filter(|node| node.node_id() != authority.node_id())
    {
        assert!(
            matches!(
                resolve_pid_routed(
                    &node.context,
                    realm.realm_id,
                    document_id,
                    MetadataRegistryRecord::graph_iri_for(document_id),
                )
                .await,
                Err(MetadataApiError::ServiceUnavailable)
            ),
            "a replica may not answer for the authority"
        );
    }

    realm.shutdown().await;
    Ok(())
}

/// Two authorized users minting the same document through two ingress nodes get
/// one job on one owner. A node-local dedup row would let alternating ingress
/// open a job each, and the handle each caller receives must be readable by that
/// caller — an owner-scoped id its holder cannot inspect is not a handle.
#[tokio::test]
async fn mint_dedups_ingress() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let (document_id, placement) = seed_document(&realm, group_id, "datasets/shared", true).await?;
    let other = aruna_core::UserId::local(Ulid::generate(), realm.realm_id);
    realm.grant_group_user(group_id, other).await?;

    let authority = realm.holder(&placement);
    let ingress = realm.non_holder_ids(&placement);
    let (first_node, second_node) = (realm.find(ingress[0]), realm.find(ingress[1]));

    let first = submit_routed(&realm, first_node, document_id, realm.user_id).await?;
    let second = submit_routed(&realm, second_node, document_id, other).await?;

    // A retried submission joins its own lost attempt, so only the second
    // submitter's join carries the dedup contract.
    assert!(!second.created, "the second user joins the first job");
    assert_eq!(first.job_id, second.job_id);

    for node in realm.nodes.iter() {
        let stored = read_job_record(&node.context.storage_handle, first.job_id, None).await?;
        assert_eq!(
            stored.is_some(),
            node.node_id() == authority.node_id(),
            "only the authority may own the mint job"
        );
    }
    // Either user resolves the reservation, which is what makes it document-global.
    for user in [realm.user_id, other] {
        assert_eq!(
            find_dedup_job(
                &authority.context.storage_handle,
                user,
                &pid_dedup_key(document_id),
                None,
            )
            .await?,
            Some(first.job_id)
        );
    }

    let owner = read_job_routed(
        first_node.context.as_ref(),
        &realm.auth_context(),
        first.job_id,
        Some(realm.bearer_token()),
    )
    .await?;
    assert_eq!(owner.job.created_by, realm.user_id);
    let joiner = read_job_routed(
        second_node.context.as_ref(),
        &realm.auth_for(other),
        second.job_id,
        Some(realm.bearer_for(other)),
    )
    .await?;
    assert_eq!(
        joiner.job.created_by, other,
        "a joined handle is served to its own caller, never as the first submitter"
    );
    assert_eq!(joiner.job.job_id, first.job_id);

    realm.shutdown().await;
    Ok(())
}

/// A mapping row on a node that is not the authority carries no version anyone
/// can compare, so it may neither create a redirect nor a tombstone. Both
/// directions are checked: a fabricated Active row must not redirect an unminted
/// document, and a fabricated Withdrawn row must not retire a live one.
#[tokio::test]
async fn replica_mapping_ignored() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let (unminted_id, unminted_placement) =
        seed_document(&realm, group_id, "datasets/unminted", true).await?;
    let (minted_id, minted_placement) =
        seed_document(&realm, group_id, "datasets/minted-live", true).await?;

    for node_id in realm.non_holder_ids(&unminted_placement) {
        write_mapping(
            realm.find(node_id),
            active_mapping(unminted_id, realm.user_id),
        )
        .await?;
    }
    let authority = realm.holder(&minted_placement);
    assert!(mint_routed(&realm, authority, minted_id).await?.1);
    for node_id in realm.non_holder_ids(&minted_placement) {
        write_mapping(
            realm.find(node_id),
            withdrawn_mapping(minted_id, realm.user_id),
        )
        .await?;
    }

    for node in realm.nodes.iter() {
        assert_eq!(
            resolve_until_answer(&realm, node, unminted_id).await?,
            PersistentIdResolution::Missing,
            "a replica's mapping must not mint a redirect"
        );
        assert_eq!(
            resolve_until_answer(&realm, node, minted_id).await?,
            PersistentIdResolution::Redirect,
            "a replica's tombstone must not retire a live PID"
        );
    }

    realm.shutdown().await;
    Ok(())
}

/// An active mapping that reached the authority before the document did is not
/// evidence of deletion. Answering Gone from that state emits a permanent 410 for
/// a live document, so the authority reports unavailable until it can tell the
/// two apart.
#[tokio::test]
async fn premature_mapping_unavailable() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let path = "datasets/unprojected";
    let origin = realm.leading_node(group_id, path);
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    let placement = realm
        .origin_placement(origin, group_id, document_id, path)
        .ok_or("a Management node holds buckets")?;

    let authority = realm.holder(&placement);
    write_mapping(authority, active_mapping(document_id, realm.user_id)).await?;

    assert!(
        matches!(
            resolve_pid_routed(
                &authority.context,
                realm.realm_id,
                document_id,
                MetadataRegistryRecord::graph_iri_for(document_id),
            )
            .await,
            Err(MetadataApiError::ServiceUnavailable)
        ),
        "an unexplained active mapping is unavailable, never a false 410"
    );

    realm.shutdown().await;
    Ok(())
}

/// Administrative withdrawal is not a document-owner operation: a group member
/// with document WRITE is still forbidden without the realm PID-admin grant.
#[tokio::test]
async fn owner_cannot_withdraw() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let (document_id, placement) =
        seed_document(&realm, group_id, "datasets/guarded", true).await?;
    let owner = aruna_core::UserId::local(Ulid::generate(), realm.realm_id);
    realm.grant_group_user(group_id, owner).await?;
    let sender = realm.non_holder(&placement);

    let denied = withdraw_pid_routed(
        &sender.context,
        realm.realm_id,
        document_id,
        owner,
        "test denial".to_string(),
        aruna_core::util::unix_timestamp_millis(),
        Some(realm.bearer_for(owner)),
    )
    .await;
    assert!(
        matches!(denied, Err(MetadataApiError::Forbidden)),
        "document WRITE must not grant primary-PID withdrawal: {denied:?}"
    );

    let never_created = mint_local_document(
        &realm.config,
        &realm.actor(realm.node(0)),
        group_id,
        "datasets/ghost",
    )?
    .as_ulid();
    let absent = withdraw_pid_routed(
        &sender.context,
        realm.realm_id,
        never_created,
        owner,
        "test denial".to_string(),
        aruna_core::util::unix_timestamp_millis(),
        Some(realm.bearer_for(owner)),
    )
    .await;
    assert!(
        matches!(absent, Err(MetadataApiError::NotFound)),
        "an id with no registry row has no withdrawal to authorize: {absent:?}"
    );

    for node in realm.nodes.iter() {
        if let Some(mapping) = read_mapping(node.context.as_ref(), document_id).await? {
            assert!(
                !mapping.is_retired(),
                "unauthorized withdrawal may not retire the automatic intent"
            );
        }
        assert!(
            read_mapping(node.context.as_ref(), never_created)
                .await?
                .is_none()
        );
    }

    realm.shutdown().await;
    Ok(())
}

/// A holder that still owes a create's registry projection may not answer a
/// routed delete with absence: with every holder lagging, the harvest deletion
/// path would take that fan-out as proof the document never existed and go
/// terminal over a live document.
#[tokio::test]
async fn delete_waits_projection() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let path = "datasets/lagging";
    let origin = realm.leading_node(group_id, path);
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    let placement = realm
        .origin_placement(origin, group_id, document_id, path)
        .ok_or("a Management node holds buckets")?;
    let sender = realm.non_holder(&placement);

    // Every holder reachable and empty: absence is then a definitive answer.
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "the routed delete never reached every holder",
        || async {
            match routed_delete(&realm, sender, document_id).await {
                Err(MetadataWriteError::NotFound) => Ok(0),
                _ => Ok(1),
            }
        },
    )
    .await?;

    queue_projection(realm.holder(&placement), document_id).await?;
    let pending = routed_delete(&realm, sender, document_id).await;
    assert!(
        matches!(pending, Err(MetadataWriteError::Undeliverable(_))),
        "a queued projection makes the holder's answer retryable: {pending:?}"
    );

    realm.shutdown().await;
    Ok(())
}

async fn routed_delete(
    realm: &Topology,
    node: &TestNode,
    document_id: Ulid,
) -> Result<(), MetadataWriteError> {
    delete_metadata_document_routed(
        &node.context,
        realm.actor(node),
        None,
        document_id,
        Some(realm.bearer_token()),
    )
    .await
}

/// A routed delete, retried while the forward reports it undeliverable: a
/// response lost on a starved machine leaves the write possibly sent. The frozen
/// record routes the first attempt only, so a replay can still answer
/// `NotFound` once the row is gone from the current holders.
async fn delete_until_applied(
    realm: &Topology,
    node: &TestNode,
    record: &aruna_core::structs::MetadataRegistryRecord,
    document_id: Ulid,
) -> TestResult<()> {
    let attempted = std::cell::Cell::new(false);
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "no routed delete reached the authority",
        || async {
            let replay = attempted.replace(true);
            match delete_metadata_document_routed(
                &node.context,
                realm.actor(node),
                (!replay).then_some(record),
                document_id,
                Some(realm.bearer_token()),
            )
            .await
            {
                Err(MetadataWriteError::Undeliverable(_)) => Ok(1),
                Err(MetadataWriteError::NotFound) if replay => Ok(0),
                other => {
                    other?;
                    Ok(0)
                }
            }
        },
    )
    .await
}

fn revision(occurred_at_ms: u64) -> PersistentIdRevision {
    PersistentIdRevision {
        event_id: Ulid::generate(),
        actor: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
        occurred_at_ms,
    }
}

fn active_mapping(document_id: Ulid, minted_by: aruna_core::UserId) -> PersistentIdMapping {
    let mut mapping = PersistentIdMapping::requested(
        document_id,
        false,
        minted_by,
        JobId::from_bytes([7; 16]),
        true,
        "/documents/test".to_string(),
        revision(1),
    );
    assert!(mapping.activate(minted_by, revision(1)));
    mapping
}

fn withdrawn_mapping(document_id: Ulid, minted_by: aruna_core::UserId) -> PersistentIdMapping {
    let mut mapping = PersistentIdMapping::requested(
        document_id,
        false,
        minted_by,
        JobId::from_bytes([7; 16]),
        true,
        "/documents/test".to_string(),
        revision(1),
    );
    assert!(mapping.mark_tombstoned(revision(2)));
    mapping
}

/// Writes a mapping row straight into one node's store, which is what a delayed
/// or forged replication would leave behind.
async fn write_mapping(node: &TestNode, mapping: PersistentIdMapping) -> TestResult<()> {
    write_entry(
        node,
        PERSISTENT_ID_MAPPING_KEYSPACE,
        persistent_id_key(mapping.target),
        mapping.to_bytes()?,
    )
    .await
}

/// Marks a committed create as still unprojected on one node.
async fn queue_projection(node: &TestNode, document_id: Ulid) -> TestResult<()> {
    write_entry(
        node,
        METADATA_PENDING_PROJECTION_KEYSPACE,
        metadata_pending_projection_key(document_id, Ulid::generate()).to_vec(),
        Vec::new(),
    )
    .await
}

async fn write_entry(
    node: &TestNode,
    key_space: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) -> TestResult<()> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![(key_space.to_string(), key.into(), value.into())],
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        other => Err(format!("unexpected fixture write event: {other:?}").into()),
    }
}

/// A routed mint submission, retried while the authority reports it
/// unavailable: the document-global dedup key makes a replay join the job the
/// lost attempt created rather than open a second one.
async fn submit_routed(
    realm: &Topology,
    node: &TestNode,
    document_id: Ulid,
    minted_by: aruna_core::UserId,
) -> TestResult<SubmitJobResult> {
    let submitted = std::cell::RefCell::new(None);
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "no mint submission reached the authority",
        || async {
            match submit_mint_pid(
                &node.context,
                MintPersistentIdSpec {
                    document_id,
                    minted_by,
                },
                node.node_id(),
                JOB_RETENTION_MS,
                Some(realm.bearer_for(minted_by)),
            )
            .await
            {
                Err(SubmitJobError::PlacementUnavailable(_)) => Ok(1),
                other => {
                    *submitted.borrow_mut() = Some(other?);
                    Ok(0)
                }
            }
        },
    )
    .await?;
    Ok(submitted
        .into_inner()
        .ok_or("the mint submission produced no job")?)
}

/// A routed mint, retried while the fan-out reports it unavailable: the mint is
/// idempotent on the authority, so a replay after a lost response returns the
/// existing mapping instead of a second one.
async fn mint_routed(
    realm: &Topology,
    node: &TestNode,
    document_id: Ulid,
) -> TestResult<(aruna_core::structs::PersistentIdMapping, bool)> {
    let minted = std::cell::RefCell::new(None);
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "no routed mint reached the authority",
        || async {
            match mint_pid_routed(
                &node.context,
                realm.realm_id,
                document_id,
                realm.user_id,
                aruna_core::util::unix_timestamp_millis(),
                Some(realm.bearer_token()),
            )
            .await
            {
                Err(MetadataApiError::ServiceUnavailable) => Ok(1),
                other => {
                    *minted.borrow_mut() = Some(other?);
                    Ok(0)
                }
            }
        },
    )
    .await?;
    Ok(minted
        .into_inner()
        .ok_or("the routed mint produced no mapping")?)
}

/// A routed withdrawal, retried on the same terms as the mint: withdrawing an
/// already withdrawn mapping returns it unchanged.
async fn withdraw_routed(
    realm: &Topology,
    node: &TestNode,
    document_id: Ulid,
) -> TestResult<aruna_core::structs::PersistentIdMapping> {
    let withdrawn = std::cell::RefCell::new(None);
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "no routed withdrawal reached the authority",
        || async {
            match withdraw_pid_routed(
                &node.context,
                realm.realm_id,
                document_id,
                realm.user_id,
                "test administrator withdrawal".to_string(),
                aruna_core::util::unix_timestamp_millis(),
                Some(realm.bearer_token()),
            )
            .await
            {
                Err(MetadataApiError::ServiceUnavailable | MetadataApiError::Forbidden) => Ok(1),
                other => {
                    *withdrawn.borrow_mut() = Some(other?);
                    Ok(0)
                }
            }
        },
    )
    .await?;
    Ok(withdrawn
        .into_inner()
        .ok_or("the routed withdrawal produced no mapping")?)
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
            match resolve_pid_routed(
                &node.context,
                realm.realm_id,
                document_id,
                MetadataRegistryRecord::graph_iri_for(document_id),
            )
            .await
            {
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
        Ok(Some(mapping)) if mapping.status.is_retired()
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
    let origin = realm.leading_node(group_id, path);
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    realm
        .origin_placement(origin, group_id, document_id, path)
        .ok_or("a Management node holds buckets")?;
    let created = create_metadata_document_routed(
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
        origin.context.clone(),
        Some(realm.bearer_token()),
    )
    .await?;
    let placement = created.record.placement;
    let authority = realm.holder(&placement);
    replay_metadata_event_log(authority.context.as_ref()).await?;

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
    wait_until(
        "the automatic PID intent reaches its authority",
        authority.node_id(),
        || async {
            read_mapping(authority.context.as_ref(), document_id)
                .await
                .is_ok_and(|mapping| mapping.is_some())
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
