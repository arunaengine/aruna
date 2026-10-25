//! Tests device folder sync in both directions, plus conflicts and refused remote bindings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::topology;

use aruna_core::structs::storage::format::Compression;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REPLICATION_OBLIGATION_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::structs::{
    ActionKind, ActionOutcome, ActionScope, EntryState, FolderMode, FolderState, ORIGIN_DEVICE_TAG,
    RemoteBinding, SyncBase, SyncPull, SyncPullAck, SyncPullStatus, SyncRefusal,
};
use aruna_core::types::GroupId;
use aruna_operations::device::sync::ReconcileFolderError;
use aruna_operations::device::sync::actions::{ApplyActionInput, ExpectedEntry, apply_action};
use aruna_operations::device::sync::folders::{
    BindFolderInput, FolderError, RealmBucketInput, bind_folder, create_realm_bucket, list_entries,
    list_folders, list_transfers, read_bound, set_folder_state, unbind_folder,
};
use aruna_operations::device::sync::outbox::{drain_sync_outbox, local_origin};
use aruna_operations::device::sync::repository::{SyncUpload, UploadState, upload_entry};
use aruna_operations::device::sync::{reconcile_folder, reconcile_folders};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::metadata::device_pull::{accept_pull, pull_status};
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_operations::s3::bucket::delete::DeleteBucketOperation;
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::object::delete::{DeleteObjectInput, DeleteObjectOperation};
use aruna_operations::s3::object::get::{GetObjectInput, GetObjectOperation};
use aruna_operations::s3::object::head::{HeadObjectInput, HeadObjectOperation};
use aruna_operations::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_operations::s3::object::versions::{ListVersionsInput, ListVersionsOperation};
use aruna_operations::staging::offered_directory::{
    OfferDirectoryInput, OfferedDirectoryError, list_offers, offer_directory,
};
use futures_util::StreamExt;
use topology::{TestResult, Topology, hang_cap, wait_for_convergence, wait_until};
use ulid::Ulid;

const MANAGEMENT_NODES: usize = 2;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 1;
const REMOTE_BUCKET: &str = "lab-data";

fn body(bytes: &'static [u8]) -> BackendStream<Result<bytes::Bytes, StreamError>> {
    BackendStream::new(tokio_util::io::ReaderStream::new(bytes))
}

/// Waits until the realm has pulled every queued upload.
async fn await_uploads(realm: &Topology) -> TestResult<()> {
    let device = realm.user_node();
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "the device never published its queued uploads",
        || async {
            drain_sync_outbox(&device.context).await;
            Ok(list_transfers(&device.context).await?.len())
        },
    )
    .await
}

/// Waits until the realm node's replication drain has taken every live obligation.
async fn await_drained(context: &DriverContext) -> TestResult<()> {
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "a committed pull never woke the replication drain",
        || async {
            let scan = StorageEffect::Iter {
                key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1,
                txn_id: None,
            };
            match context.storage_handle.send_storage_effect(scan).await {
                Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values.len()),
                other => Err(format!("the obligation scan failed: {other:?}").into()),
            }
        },
    )
    .await
}

async fn create_bucket(
    context: &DriverContext,
    group_id: GroupId,
    user_id: UserId,
) -> TestResult<()> {
    drive(
        CreateBucketOperation::new(
            REMOTE_BUCKET.to_string(),
            BucketInfo {
                group_id,
                created_at: SystemTime::now(),
                created_by: user_id,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
            },
        ),
        context,
    )
    .await?;
    Ok(())
}

async fn put_object(
    realm: &Topology,
    context: &DriverContext,
    group_id: GroupId,
    key: &str,
    bytes: &'static [u8],
) -> TestResult<Ulid> {
    let node_id = context
        .net_handle
        .as_ref()
        .ok_or("the realm node needs a net handle")?
        .node_id();
    let result = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: realm.user_id,
            group_id,
            realm_id: realm.realm_id,
            node_id,
            request: PutObjectInput {
                bucket: REMOTE_BUCKET.to_string(),
                key: key.to_string(),
                content_length: Some(bytes.len() as u64),
                body: Some(body(bytes)),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        }),
        context,
    )
    .await?;
    Ok(result.version_id)
}

async fn read_object(
    realm: &Topology,
    context: &DriverContext,
    group_id: GroupId,
    key: &str,
) -> TestResult<Vec<u8>> {
    let node_id = context
        .net_handle
        .as_ref()
        .ok_or("the realm node needs a net handle")?
        .node_id();
    let result = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: REMOTE_BUCKET.to_string(),
            key: key.to_string(),
            version_id: None,
            range: None,
            group_id,
            user_identity: realm.user_id,
            realm_id: realm.user_id.realm_id,
            node_id,
        }),
        context,
    )
    .await?;
    let mut bytes = Vec::new();
    let mut blob = result.blob.0;
    while let Some(chunk) = blob.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

async fn delete_object(
    realm: &Topology,
    context: &DriverContext,
    group_id: GroupId,
    key: &str,
) -> TestResult<()> {
    let node_id = context
        .net_handle
        .as_ref()
        .ok_or("the realm node needs a net handle")?
        .node_id();
    drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: REMOTE_BUCKET.to_string(),
            key: key.to_string(),
            version_id: None,
            group_id,
            realm_id: realm.realm_id,
            node_id,
            deleted_by: realm.user_id,
        }),
        context,
    )
    .await?;
    Ok(())
}

async fn entry_of(
    context: &std::sync::Arc<DriverContext>,
    folder_id: Ulid,
    path: &str,
) -> TestResult<SyncBase> {
    let (entries, _) = list_entries(context, folder_id, None, None).await?;
    entries
        .into_iter()
        .find(|(relative, _)| relative == path)
        .map(|(_, base)| base)
        .ok_or_else(|| format!("the folder has no entry for {path}").into())
}

/// The device without its task runtime. A bind through it wakes no background pass, so only
/// the test's own passes decide.
fn manual_context(context: &DriverContext) -> std::sync::Arc<DriverContext> {
    std::sync::Arc::new(DriverContext {
        task_handle: None,
        ..context.clone()
    })
}

/// A create-enabled binding of `root` to the realm node's bucket.
fn create_input(realm: &Topology, root: &std::path::Path, group_id: GroupId) -> BindFolderInput {
    BindFolderInput {
        folder_id: Ulid::generate(),
        root: root.to_string_lossy().to_string(),
        group_id,
        remote: RemoteBinding {
            node_id: realm.node(0).node_id(),
            bucket: REMOTE_BUCKET.to_string(),
            prefix: String::new(),
        },
        create_bucket: true,
        mode: FolderMode::TwoWay,
        propagate_deletes: true,
        realm_id: realm.realm_id,
        user_id: realm.user_id,
    }
}

/// A folder bound to the realm node's bucket. No pass has observed it yet.
async fn bind(
    realm: &Topology,
    root: &std::path::Path,
    group_id: GroupId,
) -> TestResult<aruna_core::structs::SyncedFolder> {
    let device = realm.user_node();
    let server = realm.node(0);
    let folder = bind_folder(
        &manual_context(&device.context),
        BindFolderInput {
            folder_id: Ulid::generate(),
            root: root.to_string_lossy().to_string(),
            group_id,
            remote: RemoteBinding {
                node_id: server.node_id(),
                bucket: REMOTE_BUCKET.to_string(),
                prefix: String::new(),
            },
            create_bucket: false,
            mode: FolderMode::TwoWay,
            propagate_deletes: true,
            realm_id: realm.realm_id,
            user_id: realm.user_id,
        },
    )
    .await?;
    Ok(folder)
}

/// A local file becomes a realm version, a realm object becomes a local file,
/// and a realm deletion leaves the local file exactly where it is.
#[tokio::test]
async fn syncs_both_directions() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    create_bucket(&server.context, group_id, realm.user_id).await?;

    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("note.txt"), b"from the device")?;
    let folder = bind(&realm, root.path(), group_id).await?;

    let plan = reconcile_folder(&device.context, &folder).await?;
    assert_eq!(plan.uploads, 1);
    await_uploads(&realm).await?;
    assert_eq!(
        read_object(&realm, &server.context, group_id, "note.txt").await?,
        b"from the device"
    );

    put_object(
        &realm,
        &server.context,
        group_id,
        "shared.txt",
        b"from the realm",
    )
    .await?;
    reconcile_folder(&device.context, &folder).await?;
    assert_eq!(
        std::fs::read(root.path().join("shared.txt"))?,
        b"from the realm"
    );

    delete_object(&realm, &server.context, group_id, "note.txt").await?;
    reconcile_folder(&device.context, &folder).await?;
    assert_eq!(
        std::fs::read(root.path().join("note.txt"))?,
        b"from the device",
        "a realm deletion must never remove the owner's file"
    );
    assert!(matches!(
        entry_of(&device.context, folder.folder_id, "note.txt")
            .await?
            .entry,
        EntryState::RemoteDeleted { .. }
    ));
    Ok(())
}

/// A pulled version and a pulled delete both wake continuous sync on the realm
/// node, so neither waits for an unrelated write.
#[tokio::test]
async fn wakes_replication_drain() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    create_bucket(&server.context, group_id, realm.user_id).await?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("note.txt"), b"from the device")?;
    let folder = bind(&realm, root.path(), group_id).await?;

    reconcile_folder(&device.context, &folder).await?;
    await_uploads(&realm).await?;
    await_drained(&server.context).await?;

    std::fs::remove_file(root.path().join("note.txt"))?;
    assert_eq!(reconcile_folder(&device.context, &folder).await?.uploads, 1);
    await_uploads(&realm).await?;
    assert!(
        read_object(&realm, &server.context, group_id, "note.txt")
            .await
            .is_err(),
        "the local delete reached the realm"
    );
    await_drained(&server.context).await
}

/// Polls an accepted pull until the realm node answers for good.
async fn pull_done(
    context: &std::sync::Arc<DriverContext>,
    peer: aruna_core::NodeId,
    token: &AuthToken,
    pull: &SyncPull,
    mut status: SyncPullStatus,
) -> TestResult<SyncPullAck> {
    hang_cap("a sync pull never finished", async {
        loop {
            match status {
                SyncPullStatus::Done(ack) => return Ok(ack),
                SyncPullStatus::Pending { .. } => {}
                other => return Err(format!("the pull ended without a version: {other:?}").into()),
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            status = pull_status(context, peer, token.clone(), pull).await;
        }
    })
    .await
}

/// Two pulls of one device version released together run once, commit it once and answer
/// alike, as when a device asks again while its first pull still runs.
#[tokio::test]
async fn fences_overlapping_pulls() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    create_bucket(&server.context, group_id, realm.user_id).await?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("note.txt"), b"from the device")?;
    let folder = bind(&realm, root.path(), group_id).await?;
    reconcile_folder(&device.context, &folder).await?;
    let upload = list_transfers(&device.context)
        .await?
        .pop()
        .ok_or("the file is queued")?;
    let pull = SyncPull {
        source: VersionedObjectArn::new(
            realm.realm_id,
            device.node_id(),
            folder.local_bucket.clone(),
            upload.relative.clone(),
            upload
                .local_version
                .ok_or("the upload names its local version")?,
        )?,
        blake3: upload.blake3,
        size: upload.size,
        target_bucket: REMOTE_BUCKET.to_string(),
        target_key: folder.remote.remote_key(&upload.relative),
        deleted: false,
    };
    let token = AuthToken::internal(AuthContext {
        user_id: realm.user_id,
        realm_id: realm.realm_id,
        path_restrictions: None,
        session: None,
    });
    let peer = device.node_id();
    // A pull this node never accepted is unknown, never pending.
    assert_eq!(
        pull_status(&server.context, peer, token.clone(), &pull).await,
        SyncPullStatus::Unknown
    );
    let barrier = tokio::sync::Barrier::new(2);
    let pull_once = || async {
        barrier.wait().await;
        let accepted = accept_pull(&server.context, peer, token.clone(), pull.clone()).await;
        pull_done(&server.context, peer, &token, &pull, accepted).await
    };
    let (first, second) = tokio::join!(pull_once(), pull_once());
    let (first, second) = (first?, second?);
    assert_eq!(first.version_id, second.version_id);

    let versions = drive(
        ListVersionsOperation::new(ListVersionsInput {
            bucket: REMOTE_BUCKET.to_string(),
            prefix: Some(pull.target_key.clone()),
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: None,
        }),
        &server.context,
    )
    .await?;
    assert_eq!(
        versions.items.len(),
        1,
        "one device version, one realm version"
    );
    Ok(())
}

/// A pulled version names the device it came from, and only that device keeps which local
/// file it was.
#[tokio::test]
async fn records_pull_origin() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    create_bucket(&server.context, group_id, realm.user_id).await?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("note.txt"), b"from the device")?;
    let folder = bind(&realm, root.path(), group_id).await?;
    reconcile_folder(&device.context, &folder).await?;
    await_uploads(&realm).await?;

    let head = drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: REMOTE_BUCKET.to_string(),
            key: "note.txt".to_string(),
            version_id: None,
        }),
        &server.context,
    )
    .await?;
    let device_id = device.node_id().to_string();
    assert_eq!(head.metadata.get(ORIGIN_DEVICE_TAG), Some(&device_id));
    let root_path = root.path().to_string_lossy().to_string();
    assert!(
        head.metadata
            .values()
            .all(|value| !value.contains(&root_path)),
        "no local path reaches the realm"
    );
    let version_id = head
        .resolved_version_id
        .or(head.version_id)
        .ok_or("the head names its version")?;
    let origin = local_origin(&device.context, version_id)
        .await?
        .ok_or("the device keeps the origin of what it published")?;
    assert_eq!(origin.folder_id, folder.folder_id);
    assert_eq!(origin.relative, "note.txt");
    assert!(origin.modified_at_ms.is_some());
    Ok(())
}

#[tokio::test]
async fn creates_remote_bucket() -> TestResult<()> {
    // A create-enabled bind must create on the realm node before syncing either direction.
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("note.txt"), b"from the device")?;

    let folder = bind_folder(
        &manual_context(&device.context),
        BindFolderInput {
            folder_id: Ulid::generate(),
            root: root.path().to_string_lossy().to_string(),
            group_id,
            remote: RemoteBinding {
                node_id: server.node_id(),
                bucket: REMOTE_BUCKET.to_string(),
                prefix: String::new(),
            },
            create_bucket: true,
            mode: FolderMode::TwoWay,
            propagate_deletes: true,
            realm_id: realm.realm_id,
            user_id: realm.user_id,
        },
    )
    .await?;
    let bucket = drive(
        GetBucketOperation::new(REMOTE_BUCKET.to_string()),
        &server.context,
    )
    .await?;
    assert_eq!(bucket.group_id, group_id);

    let plan = reconcile_folder(&device.context, &folder).await?;
    assert_eq!(plan.uploads, 1);
    await_uploads(&realm).await?;
    assert_eq!(
        read_object(&realm, &server.context, group_id, "note.txt").await?,
        b"from the device"
    );

    put_object(
        &realm,
        &server.context,
        group_id,
        "shared.txt",
        b"from the realm",
    )
    .await?;
    reconcile_folder(&device.context, &folder).await?;
    assert_eq!(
        std::fs::read(root.path().join("shared.txt"))?,
        b"from the realm"
    );
    Ok(())
}

/// The bind answers before the directory is walked; the pass it wakes observes the folder and
/// publishes it in the background.
#[tokio::test]
async fn binds_before_scanning() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("note.txt"), b"from the device")?;

    let folder = bind_folder(&device.context, create_input(&realm, root.path(), group_id)).await?;
    assert_eq!(
        folder.observed_files, 0,
        "the bind must not walk the folder"
    );

    wait_until(
        "the background pass publishes the file",
        server.node_id(),
        || async {
            read_object(&realm, &server.context, group_id, "note.txt")
                .await
                .is_ok_and(|bytes| bytes == b"from the device")
        },
    )
    .await?;
    assert_eq!(
        read_bound(&device.context, folder.folder_id)
            .await?
            .observed_files,
        1
    );
    Ok(())
}

/// A root that is not a usable directory is refused before the realm node creates the bucket.
#[tokio::test]
async fn refuses_missing_root() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    let root = tempfile::tempdir()?;

    let error = bind_folder(
        &device.context,
        create_input(&realm, &root.path().join("missing"), group_id),
    )
    .await
    .expect_err("a missing root is refused");
    assert!(matches!(
        error,
        FolderError::Offer(OfferedDirectoryError::Source(_))
    ));
    assert!(
        matches!(
            drive(
                GetBucketOperation::new(REMOTE_BUCKET.to_string()),
                &server.context
            )
            .await,
            Err(GetBucketError::NotFound)
        ),
        "a refused bind must not leave the bucket behind"
    );
    Ok(())
}

/// A bucket the owner asks for lands on the realm node without a folder; a name the node
/// already holds is refused whichever group holds it, and this computer holds no realm bucket.
#[tokio::test]
async fn creates_realm_bucket() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let other_group = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    let input = |group_id, node_id| RealmBucketInput {
        bucket: REMOTE_BUCKET.to_string(),
        group_id,
        node_id,
        realm_id: realm.realm_id,
        user_id: realm.user_id,
    };

    create_realm_bucket(&device.context, input(group_id, server.node_id())).await?;
    let bucket = drive(
        GetBucketOperation::new(REMOTE_BUCKET.to_string()),
        &server.context,
    )
    .await?;
    assert_eq!(bucket.group_id, group_id);

    for group in [group_id, other_group] {
        assert_eq!(
            create_realm_bucket(&device.context, input(group, server.node_id())).await,
            Err(FolderError::BucketExists {
                node: server.node_id(),
                bucket: REMOTE_BUCKET.to_string(),
            })
        );
    }
    assert_eq!(
        create_realm_bucket(&device.context, input(group_id, device.node_id())).await,
        Err(FolderError::NotRealmNode(device.node_id()))
    );

    // A folder bind still adopts the bucket of its own group.
    let root = tempfile::tempdir()?;
    let folder = bind_folder(
        &manual_context(&device.context),
        create_input(&realm, root.path(), group_id),
    )
    .await?;
    assert_eq!(folder.state, FolderState::Active);

    // The node decides existence in its create transaction: one of two racing creates wins.
    let fresh = RealmBucketInput {
        bucket: "fresh-data".to_string(),
        ..input(group_id, server.node_id())
    };
    let (left, right) = tokio::join!(
        create_realm_bucket(&device.context, fresh.clone()),
        create_realm_bucket(&device.context, fresh)
    );
    let mut outcomes = [left, right];
    outcomes.sort_by_key(Result::is_err);
    assert_eq!(outcomes[0], Ok(()));
    assert_eq!(
        outcomes[1],
        Err(FolderError::BucketExists {
            node: server.node_id(),
            bucket: "fresh-data".to_string(),
        })
    );
    Ok(())
}

/// A pass that listed a folder before its unbind writes nothing for it afterwards: the offered
/// bucket and the sync rows do not come back.
#[tokio::test]
async fn skips_unbound_folder() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    create_bucket(&server.context, group_id, realm.user_id).await?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("note.txt"), b"from the device")?;
    let listed = bind(&realm, root.path(), group_id).await?;

    unbind_folder(&manual_context(&device.context), listed.folder_id).await?;
    reconcile_folders(&manual_context(&device.context)).await;
    let plan = reconcile_folder(&device.context, &listed).await?;
    assert_eq!(plan.uploads, 0);
    let offers = list_offers(&device.context).await?;
    assert!(
        offers
            .iter()
            .all(|offer| offer.bucket != listed.local_bucket)
    );
    let (entries, _) = list_entries(&device.context, listed.folder_id, None, None).await?;
    assert!(entries.is_empty());
    Ok(())
}

/// An unbind cut short after it marked the folder, by a cancelled call or a restart, is finished
/// by the folder pass instead of staying `deleting`.
#[tokio::test]
async fn finishes_cancelled_unbind() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    create_bucket(&server.context, group_id, realm.user_id).await?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("note.txt"), b"from the device")?;
    let folder = bind(&realm, root.path(), group_id).await?;
    reconcile_folder(&device.context, &folder).await?;
    assert!(
        list_offers(&device.context)
            .await?
            .iter()
            .any(|offer| offer.bucket == folder.local_bucket)
    );

    set_folder_state(&device.context, folder.folder_id, FolderState::Deleting).await?;
    reconcile_folders(&manual_context(&device.context)).await;
    assert_eq!(
        read_bound(&device.context, folder.folder_id).await,
        Err(FolderError::NotFound)
    );
    let offers = list_offers(&device.context).await?;
    assert!(
        offers
            .iter()
            .all(|offer| offer.bucket != folder.local_bucket)
    );
    let (entries, _) = list_entries(&device.context, folder.folder_id, None, None).await?;
    assert!(entries.is_empty());
    assert!(
        matches!(
            drive(
                GetBucketOperation::new(folder.local_bucket.clone()),
                &device.context
            )
            .await,
            Err(GetBucketError::NotFound)
        ),
        "the unbind must not leave the folder's local bucket behind"
    );
    Ok(())
}

/// A bind that got no answer stays recorded as `binding`: it reserves the root against a retry,
/// and the folder pass, which also runs after a restart, creates the bucket and activates it.
#[tokio::test]
async fn recovers_interrupted_bind() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    let root = tempfile::tempdir()?;
    let unanswered = std::sync::Arc::new(DriverContext {
        metadata_handle: None,
        task_handle: None,
        ..(*device.context).clone()
    });

    let input = create_input(&realm, root.path(), group_id);
    let folder_id = input.folder_id;
    assert_eq!(
        bind_folder(&unanswered, input).await,
        Err(FolderError::Unavailable)
    );
    // The device's own timer may already have finished it; either way the bind is recorded.
    let recorded = read_bound(&device.context, folder_id).await?.state;
    assert!(
        matches!(
            recorded,
            FolderState::Binding {
                create_bucket: true
            } | FolderState::Active
        ),
        "{recorded:?}"
    );
    assert_eq!(
        bind_folder(
            &manual_context(&device.context),
            create_input(&realm, root.path(), group_id)
        )
        .await,
        Err(FolderError::RootOverlaps(
            root.path().to_string_lossy().to_string()
        ))
    );

    reconcile_folders(&manual_context(&device.context)).await;
    assert_eq!(
        read_bound(&device.context, folder_id).await?.state,
        FolderState::Active
    );
    let bucket = drive(
        GetBucketOperation::new(REMOTE_BUCKET.to_string()),
        &server.context,
    )
    .await?;
    assert_eq!(bucket.group_id, group_id);
    Ok(())
}

#[tokio::test]
async fn refuses_foreign_bucket() -> TestResult<()> {
    // A create-enabled bind must not adopt a same-named bucket from another group.
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let foreign_group = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    create_bucket(&server.context, foreign_group, realm.user_id).await?;
    let root = tempfile::tempdir()?;

    let error = bind_folder(
        &device.context,
        BindFolderInput {
            folder_id: Ulid::generate(),
            root: root.path().to_string_lossy().to_string(),
            group_id,
            remote: RemoteBinding {
                node_id: server.node_id(),
                bucket: REMOTE_BUCKET.to_string(),
                prefix: String::new(),
            },
            create_bucket: true,
            mode: FolderMode::TwoWay,
            propagate_deletes: true,
            realm_id: realm.realm_id,
            user_id: realm.user_id,
        },
    )
    .await
    .expect_err("a bucket from another group must be refused");
    assert_eq!(
        error,
        FolderError::RemoteBucketConflict {
            bucket: REMOTE_BUCKET.to_string(),
            reason: format!("bucket \"{REMOTE_BUCKET}\" belongs to another group"),
        }
    );
    assert!(list_folders(&device.context).await?.is_empty());
    Ok(())
}

/// A file both sides changed keeps the local bytes, gains a conflicted copy and
/// publishes a new realm version; replacing it is an explicit, audited step
/// that applies exactly once.
#[tokio::test]
async fn resolves_conflicts() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    create_bucket(&server.context, group_id, realm.user_id).await?;

    let root = tempfile::tempdir()?;
    let file = root.path().join("paper.txt");
    std::fs::write(&file, b"first")?;
    let folder = bind(&realm, root.path(), group_id).await?;
    reconcile_folder(&device.context, &folder).await?;
    await_uploads(&realm).await?;

    // Both sides move before the next pass.
    std::fs::write(&file, b"the owner's own edit")?;
    put_object(
        &realm,
        &server.context,
        group_id,
        "paper.txt",
        b"the realm's edit",
    )
    .await?;
    reconcile_folder(&device.context, &folder).await?;

    assert_eq!(std::fs::read(&file)?, b"the owner's own edit");
    let base = entry_of(&device.context, folder.folder_id, "paper.txt").await?;
    let EntryState::Conflict {
        conflicted_copy, ..
    } = base.entry.clone()
    else {
        return Err(format!("both sides changed: {:?}", base.entry).into());
    };
    assert_eq!(
        std::fs::read(root.path().join(&conflicted_copy))?,
        b"the realm's edit"
    );
    await_uploads(&realm).await?;
    assert_eq!(
        read_object(&realm, &server.context, group_id, "paper.txt").await?,
        b"the owner's own edit",
        "a local edit always becomes the next realm version"
    );

    let local = base.local.clone().ok_or("the entry names its local side")?;
    let expected = ExpectedEntry {
        fingerprint: local.fingerprint.clone().ok_or("a local fingerprint")?,
        blake3: local.blake3.ok_or("a local hash")?,
        remote_version: None,
    };
    let action = ApplyActionInput {
        folder_id: folder.folder_id,
        kind: ActionKind::Replace,
        scope: ActionScope::Entry {
            relative: "paper.txt".to_string(),
        },
        expected: Some(expected),
        actor: realm.user_id,
    };
    let applied = apply_action(&device.context, action.clone()).await?;
    assert_eq!(applied.outcome, ActionOutcome::Applied);
    assert_eq!(std::fs::read(&file)?, b"the realm's edit");

    // The same request twice must not replace the new bytes again.
    let replayed = apply_action(&device.context, action).await?;
    assert_eq!(replayed.outcome, ActionOutcome::Stale);
    assert_eq!(std::fs::read(&file)?, b"the realm's edit");
    Ok(())
}

#[tokio::test]
async fn refuses_invalid_remotes() -> TestResult<()> {
    // Binding validates both realm membership and remote bucket existence.
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    let root = tempfile::tempdir()?;
    let input = |node_id| BindFolderInput {
        folder_id: Ulid::generate(),
        root: root.path().to_string_lossy().to_string(),
        group_id,
        remote: RemoteBinding {
            node_id,
            bucket: REMOTE_BUCKET.to_string(),
            prefix: String::new(),
        },
        create_bucket: false,
        mode: FolderMode::TwoWay,
        propagate_deletes: true,
        realm_id: realm.realm_id,
        user_id: realm.user_id,
    };

    let missing = bind_folder(&device.context, input(server.node_id()))
        .await
        .expect_err("the missing remote bucket is refused");
    assert_eq!(
        missing,
        FolderError::RemoteBucketMissing {
            node: server.node_id(),
            bucket: REMOTE_BUCKET.to_string(),
        }
    );
    assert_eq!(
        missing.to_string(),
        format!(
            "the bucket \"{REMOTE_BUCKET}\" does not exist on node {}",
            server.node_id()
        )
    );

    assert_eq!(
        bind_folder(&device.context, input(device.node_id()))
            .await
            .expect_err("a user node is not a realm server"),
        FolderError::NotRealmNode(device.node_id())
    );
    assert!(list_folders(&device.context).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn recovers_missing_bucket() -> TestResult<()> {
    // A missing bucket records the pass, then a recreated bucket revives its upload.
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    create_bucket(&server.context, group_id, realm.user_id).await?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("note.txt"), b"from the device")?;
    let folder = bind(&realm, root.path(), group_id).await?;

    drive(
        DeleteBucketOperation::new(REMOTE_BUCKET.to_string()),
        &server.context,
    )
    .await?;
    let error = reconcile_folder(&device.context, &folder)
        .await
        .expect_err("the missing bucket is reported");
    assert_eq!(error, ReconcileFolderError::Refused(SyncRefusal::NotFound));
    let failed = read_bound(&device.context, folder.folder_id).await?;
    assert_eq!(
        failed.last_error.as_deref(),
        Some(error.describe(&folder.remote).as_str())
    );
    assert!(
        failed
            .last_error
            .as_deref()
            .is_some_and(|m| m.contains(REMOTE_BUCKET))
    );
    assert!(failed.last_error_ms.is_some());
    assert_eq!(failed.observed_files, 1);
    assert_eq!(failed.last_reconcile_ms, folder.last_reconcile_ms);

    let sweep = offer_directory(
        &device.context,
        OfferDirectoryInput {
            bucket: folder.local_bucket.clone(),
            root: folder.root.clone(),
            group_id,
            realm_id: realm.realm_id,
            node_id: device.node_id(),
            user_id: realm.user_id,
        },
    )
    .await?;
    let local = sweep.observed.first().ok_or("the local file is observed")?;
    let upload = SyncUpload {
        folder_id: folder.folder_id,
        relative: local.relative.clone(),
        deleted: false,
        fingerprint: local.fingerprint.clone(),
        blake3: None,
        size: local.size,
        local_version: Some(local.version_id),
        queued_at_ms: 1,
        state: UploadState::Failed {
            reason: "NotFound".to_string(),
            retryable: false,
        },
    };
    let (key_space, key, value) = upload_entry(&upload)?;
    let stored = device
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        stored,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    create_bucket(&server.context, group_id, realm.user_id).await?;
    let plan = reconcile_folder(&device.context, &failed).await?;
    assert_eq!(plan.uploads, 1);
    let recovered = read_bound(&device.context, folder.folder_id).await?;
    assert_eq!(recovered.last_error, None);
    assert_eq!(recovered.last_error_ms, None);
    assert_eq!(recovered.observed_files, 1);
    await_uploads(&realm).await?;
    assert_eq!(
        read_object(&realm, &server.context, group_id, "note.txt").await?,
        b"from the device"
    );
    Ok(())
}
