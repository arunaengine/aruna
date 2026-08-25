// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
//! Two-node proof of the synced-folder contract: a device and the realm node it
//! binds to. Every assertion here is about local data winning locally.

mod topology;

use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    ActionKind, ActionOutcome, ActionScope, BucketInfo, EntryState, FolderMode, RemoteBinding,
    RoutingSnapshot, SyncBase,
};
use aruna_core::types::GroupId;
use aruna_operations::device::sync::actions::{ApplyActionInput, ExpectedEntry, apply_action};
use aruna_operations::device::sync::folders::{
    BindFolderInput, bind_folder, list_entries, list_transfers,
};
use aruna_operations::device::sync::outbox::drain_sync_outbox;
use aruna_operations::device::sync::reconcile_folder;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::create_bucket::CreateBucketOperation;
use aruna_operations::s3::delete_object::{DeleteObjectInput, DeleteObjectOperation};
use aruna_operations::s3::get_object::{GetObjectInput, GetObjectOperation};
use aruna_operations::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use futures_util::StreamExt;
use topology::{TestResult, Topology, wait_for_convergence};
use ulid::Ulid;

const MANAGEMENT_NODES: usize = 2;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 1;
const REMOTE_BUCKET: &str = "lab-data";

fn body(bytes: &'static [u8]) -> BackendStream<Result<bytes::Bytes, StreamError>> {
    BackendStream::new(tokio_util::io::ReaderStream::new(bytes))
}

/// Waits until the realm has pulled every queued upload.
///
/// The drain is a timer task the reconciliation arms, so it runs concurrently
/// with an explicit pass and may hold the row this one wanted. Publishing is
/// therefore complete when the outbox is empty, never after one call.
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
            },
        ),
        context,
    )
    .await?
    .ok_or("bucket creation did not finish")??;
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
    .await?
    .ok_or("the put did not finish")??;
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
            node_id,
        }),
        context,
    )
    .await?
    .ok_or("the get did not finish")??;
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
    .await?
    .ok_or("the delete did not finish")??;
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

/// A folder bound to the realm node's bucket, with its first sweep taken.
async fn bind(
    realm: &Topology,
    root: &std::path::Path,
    group_id: GroupId,
) -> TestResult<aruna_core::structs::SyncedFolder> {
    let device = realm.user_node();
    let server = realm.node(0);
    let folder = bind_folder(
        &device.context,
        BindFolderInput {
            folder_id: Ulid::generate(),
            root: root.to_string_lossy().to_string(),
            group_id,
            remote: RemoteBinding {
                node_id: server.node_id(),
                bucket: REMOTE_BUCKET.to_string(),
                prefix: String::new(),
            },
            mode: FolderMode::TwoWay,
            propagate_deletes: true,
            realm_id: realm.realm_id,
            node_id: device.node_id(),
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

    let plan = reconcile_folder(&device.context, &folder)
        .await
        .ok_or("the first pass must reconcile")?;
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
    reconcile_folder(&device.context, &folder)
        .await
        .ok_or("the second pass must reconcile")?;
    assert_eq!(
        std::fs::read(root.path().join("shared.txt"))?,
        b"from the realm"
    );

    delete_object(&realm, &server.context, group_id, "note.txt").await?;
    reconcile_folder(&device.context, &folder)
        .await
        .ok_or("the third pass must reconcile")?;
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
    reconcile_folder(&device.context, &folder)
        .await
        .ok_or("the first pass must reconcile")?;
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
    reconcile_folder(&device.context, &folder)
        .await
        .ok_or("the conflict pass must reconcile")?;

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
