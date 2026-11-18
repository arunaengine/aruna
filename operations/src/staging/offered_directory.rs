//! Offers a local directory as a read-only bucket of reference versions on this device.
//! Writes are refused; the files change only on the owner's own filesystem.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::create::CreateBucketError;
use crate::s3::usage::UsageUpdateError;
use crate::staging::bucket_guard::BucketGuardError;
use crate::staging::list_offers::ListOffersError;
use aruna_core::UserId;
use aruna_core::calls::IoError;
use aruna_core::errors::{ConversionError, StagingSourceError, StorageError};
use aruna_core::id::NodeId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

pub(crate) use super::offer_run::{CheckRootOperation, check_root};
pub use super::offer_run::{
    OfferDirectoryOperation, WithdrawOfferOperation, offer_directory, withdraw_offer,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfferDirectoryInput {
    pub bucket: String,
    /// Root as the owner gives it. It is stored device-locally and resolved on
    /// every access; nothing derived from it is ever published.
    pub root: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub user_id: UserId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfferDirectoryResult {
    pub bucket: String,
    pub files: usize,
    /// Observations tombstoned because their file is gone from the root.
    pub removed: usize,
    /// Every file the walk saw, with the version that now names it. A synced
    /// folder reconciles against exactly these observations.
    pub observed: Vec<ObservedFile>,
}

/// One file as the sweep observed it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedFile {
    pub relative: String,
    pub fingerprint: String,
    pub size: u64,
    pub modified_at_ms: Option<u64>,
    pub version_id: Ulid,
    /// The stat the listing read, for the decisions that need more than the
    /// fingerprint it was folded into.
    pub stat: Option<aruna_core::structs::execution::offered_directory::FileStat>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WithdrawOfferInput {
    pub bucket: String,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub user_id: UserId,
}

/// The parts of an offer every observation write needs. The root is absent on
/// purpose: an observation is bound to the registration, never to a path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ObservationScope {
    pub(super) bucket: String,
    pub(super) group_id: GroupId,
    pub(super) realm_id: RealmId,
    pub(super) node_id: NodeId,
    pub(super) user_id: UserId,
}

impl OfferDirectoryInput {
    pub(super) fn scope(&self) -> ObservationScope {
        ObservationScope {
            bucket: self.bucket.clone(),
            group_id: self.group_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
            user_id: self.user_id,
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum OfferedDirectoryError {
    #[error("bucket `{0}` already exists and is not an offered directory")]
    BucketTaken(String),
    #[error("bucket `{0}` is an offered directory and is read-only")]
    ReadOnly(String),
    #[error("bucket `{0}` is not an offered directory")]
    NotOffered(String),
    #[error("the offered directory holds more than {0} files")]
    TooManyFiles(usize),
    #[error("this node cannot read local directories")]
    HandleMissing,
    #[error(transparent)]
    Source(#[from] StagingSourceError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Bucket(#[from] CreateBucketError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
}

impl From<IoError> for OfferedDirectoryError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

impl From<ListOffersError> for OfferedDirectoryError {
    fn from(error: ListOffersError) -> Self {
        match error {
            ListOffersError::Storage(error) => OfferedDirectoryError::Storage(error),
            ListOffersError::Conversion(error) => OfferedDirectoryError::Conversion(error),
        }
    }
}

impl From<BucketGuardError> for OfferedDirectoryError {
    fn from(error: BucketGuardError) -> Self {
        match error {
            BucketGuardError::ReadOnly(bucket) => OfferedDirectoryError::ReadOnly(bucket),
            BucketGuardError::Storage(error) => OfferedDirectoryError::Storage(error),
            BucketGuardError::Conversion(error) => OfferedDirectoryError::Conversion(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::object::get::{GetObjectInput, GetObjectOperation};
    use crate::staging::bucket_guard::BucketGuardOperation;
    use crate::staging::list_offers::ListOffersOperation;
    use crate::tests::staging::setup_driver_context;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, USAGE_STATS_KEYSPACE};
    use aruna_core::structs::storage::blob::{
        BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
    };
    use aruna_core::structs::storage::usage::{UsageCounters, usage_group_key};
    use futures_util::StreamExt;

    fn input(bucket: &str, root: &str) -> OfferDirectoryInput {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        OfferDirectoryInput {
            bucket: bucket.to_string(),
            root: root.to_string(),
            group_id: Ulid::from_bytes([2u8; 16]),
            realm_id,
            node_id: NodeId::from_bytes(&[3u8; 32]).expect("node id must parse"),
            user_id: UserId::new(Ulid::from_bytes([4u8; 16]), realm_id),
        }
    }

    // The offered file must be readable as an object without any byte of it
    // ever being copied into the node's own blob store.
    #[tokio::test]
    async fn serves_offered_file() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        std::fs::write(root.path().join("note.txt"), b"offered").expect("file must be written");
        let offer = input("offered", root.path().to_str().expect("utf-8 root"));

        let result = drive(offer_directory(offer.clone()), context)
            .await
            .expect("offer must succeed");
        assert_eq!(result.files, 1);

        let read = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "offered".to_string(),
                key: "note.txt".to_string(),
                version_id: None,
                range: None,
                group_id: offer.group_id,
                user_identity: offer.user_id,
                realm_id: offer.user_id.realm_id,
                node_id: offer.node_id,
            }),
            context,
        )
        .await
        .expect("get must run");

        let mut body = Vec::new();
        let mut blob = read.blob.0;
        while let Some(chunk) = blob.next().await {
            body.extend_from_slice(&chunk.expect("chunk must read"));
        }
        assert_eq!(body, b"offered");
    }

    #[tokio::test]
    async fn refuses_writes() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        let offer = input("locked", root.path().to_str().expect("utf-8 root"));
        drive(offer_directory(offer), context)
            .await
            .expect("offer must succeed");

        assert_eq!(
            drive(BucketGuardOperation::new("locked"), context)
                .await
                .map_err(OfferedDirectoryError::from),
            Err(OfferedDirectoryError::ReadOnly("locked".to_string()))
        );
        assert_eq!(
            drive(BucketGuardOperation::new("other"), context)
                .await
                .map_err(OfferedDirectoryError::from),
            Ok(())
        );
    }

    // A bucket that is not this offer's must keep its ordinary write semantics.
    #[tokio::test]
    async fn refuses_taken_bucket() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        let offer = input("taken", root.path().to_str().expect("utf-8 root"));
        crate::tests::staging::create_test_bucket(context, offer.group_id, offer.user_id, "taken")
            .await;

        assert_eq!(
            drive(offer_directory(offer), context).await,
            Err(OfferedDirectoryError::BucketTaken("taken".to_string()))
        );
    }

    // Re-offering an unchanged directory keeps the identities realm nodes may
    // already reference; a rewritten file mints a successor.
    #[tokio::test]
    async fn reoffer_tracks_changes() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        let file = root.path().join("data.bin");
        std::fs::write(&file, b"one").expect("file must be written");
        let offer = input("refresh", root.path().to_str().expect("utf-8 root"));

        drive(offer_directory(offer.clone()), context)
            .await
            .expect("first offer must succeed");
        let first = current_version(context, "refresh", "data.bin").await;
        drive(offer_directory(offer.clone()), context)
            .await
            .expect("second offer must succeed");
        assert_eq!(current_version(context, "refresh", "data.bin").await, first);

        std::fs::write(&file, b"one-changed").expect("file must be rewritten");
        drive(offer_directory(offer), context)
            .await
            .expect("third offer must succeed");
        assert_ne!(current_version(context, "refresh", "data.bin").await, first);
    }

    // The group is charged for what the directory currently offers, so a
    // rewritten file replaces its predecessor's bytes instead of adding to them.
    #[tokio::test]
    async fn charges_current_inventory() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        let file = root.path().join("data.bin");
        std::fs::write(&file, b"one").expect("file must be written");
        let offer = input("usage", root.path().to_str().expect("utf-8 root"));

        drive(offer_directory(offer.clone()), context)
            .await
            .expect("first offer must succeed");
        let usage = group_usage(context, offer.group_id).await;
        assert_eq!((usage.objects, usage.referenced_bytes), (1, 3));

        std::fs::write(&file, b"one-changed").expect("file must be rewritten");
        drive(offer_directory(offer.clone()), context)
            .await
            .expect("second offer must succeed");
        let usage = group_usage(context, offer.group_id).await;
        assert_eq!((usage.objects, usage.referenced_bytes), (1, 11));

        crate::driver::drive(
            crate::node::usage_rebuild::RebuildStatsOperation::new(),
            context,
        )
        .await
        .expect("recount must succeed");
        assert_eq!(group_usage(context, offer.group_id).await, usage);
    }

    // A file the owner removed stops being offered: the object becomes a delete
    // marker and its bytes are released.
    #[tokio::test]
    async fn tombstones_vanished_files() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        std::fs::write(root.path().join("keep.txt"), b"keep").expect("file must be written");
        std::fs::write(root.path().join("gone.txt"), b"gone").expect("file must be written");
        let offer = input("sweep", root.path().to_str().expect("utf-8 root"));

        let first = drive(offer_directory(offer.clone()), context)
            .await
            .expect("first offer must succeed");
        assert_eq!((first.files, first.removed), (2, 0));

        std::fs::remove_file(root.path().join("gone.txt")).expect("file must be removed");
        let second = drive(offer_directory(offer.clone()), context)
            .await
            .expect("second offer must succeed");
        assert_eq!((second.files, second.removed), (1, 1));

        let usage = group_usage(context, offer.group_id).await;
        assert_eq!((usage.objects, usage.referenced_bytes), (1, 4));

        crate::driver::drive(
            crate::node::usage_rebuild::RebuildStatsOperation::new(),
            context,
        )
        .await
        .expect("recount must succeed");
        assert_eq!(group_usage(context, offer.group_id).await, usage);
        assert!(
            head_version(context, "sweep", "gone.txt")
                .await
                .is_deleted()
        );
    }

    // Withdrawing an offer stops it resolving and empties the bucket, so what
    // the device still lists is only what it still offers.
    #[tokio::test]
    async fn withdraws_offer() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        std::fs::write(root.path().join("note.txt"), b"note").expect("file must be written");
        let offer = input("withdrawn", root.path().to_str().expect("utf-8 root"));
        drive(offer_directory(offer.clone()), context)
            .await
            .expect("offer must succeed");
        assert_eq!(
            drive(ListOffersOperation::new(), context)
                .await
                .expect("list must run")
                .len(),
            1
        );

        let removed = drive(
            withdraw_offer(WithdrawOfferInput {
                bucket: offer.bucket.clone(),
                realm_id: offer.realm_id,
                node_id: offer.node_id,
                user_id: offer.user_id,
            }),
            context,
        )
        .await
        .expect("withdraw must succeed");

        assert_eq!(removed, 1);
        assert!(
            drive(ListOffersOperation::new(), context)
                .await
                .expect("list must run")
                .is_empty()
        );
        assert!(
            head_version(context, "withdrawn", "note.txt")
                .await
                .is_deleted()
        );
        assert_eq!(
            drive(BucketGuardOperation::new("withdrawn"), context)
                .await
                .map_err(OfferedDirectoryError::from),
            Ok(())
        );
        let usage = group_usage(context, offer.group_id).await;
        assert_eq!((usage.objects, usage.referenced_bytes), (0, 0));
    }

    #[tokio::test]
    async fn refuses_unknown_offer() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        assert_eq!(
            drive(
                withdraw_offer(WithdrawOfferInput {
                    bucket: "absent".to_string(),
                    realm_id: RealmId::from_bytes([7u8; 32]),
                    node_id: NodeId::from_bytes(&[3u8; 32]).expect("node id must parse"),
                    user_id: UserId::new(
                        Ulid::from_bytes([4u8; 16]),
                        RealmId::from_bytes([7u8; 32])
                    ),
                }),
                &fixture.driver_context
            )
            .await,
            Err(OfferedDirectoryError::NotOffered("absent".to_string()))
        );
        assert!(
            drive(ListOffersOperation::new(), context)
                .await
                .expect("list must run")
                .is_empty()
        );
    }

    async fn read(
        context: &DriverContext,
        key_space: &str,
        key: &[u8],
    ) -> Option<byteview::ByteView> {
        let read = StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        };
        match context.storage_handle.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("the row must read: {}", other.label()),
        }
    }

    async fn group_usage(context: &DriverContext, group_id: GroupId) -> UsageCounters {
        read(context, USAGE_STATS_KEYSPACE, &usage_group_key(group_id))
            .await
            .map(|value| UsageCounters::from_bytes(value.as_ref()).expect("counters must decode"))
            .unwrap_or_default()
    }

    async fn pointer(context: &DriverContext, bucket: &str, key: &str) -> CurrentVersionPointer {
        let head = BlobHeadKey::new(bucket, key).to_bytes().expect("head key");
        let row = read(context, BLOB_HEAD_KEYSPACE, &head).await;
        CurrentVersionPointer::from_bytes(&row.expect("pointer must exist")).expect("pointer")
    }

    async fn head_version(context: &DriverContext, bucket: &str, key: &str) -> BlobVersion {
        let version_id = pointer(context, bucket, key).await.version_id;
        let row_key = VersionKey::new(bucket, key, version_id)
            .to_bytes()
            .expect("version key");
        let row = read(context, BLOB_VERSIONS_KEYSPACE, &row_key).await;
        BlobVersion::from_bytes(&row.expect("version must exist")).expect("version")
    }

    async fn current_version(context: &DriverContext, bucket: &str, key: &str) -> Ulid {
        pointer(context, bucket, key).await.version_id
    }
}
