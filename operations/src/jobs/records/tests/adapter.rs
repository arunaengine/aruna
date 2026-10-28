//! Tests that record transport separates the peer from the publisher and refuses bad offers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, JobRecordFrame, LaunchFrame, PageLimit, StorageEffect};
use aruna_core::events::{JobRecordRejection, LaunchDecline};
use aruna_core::handle::Handle;
use aruna_core::structs::execution::job::JobFamilyRecord;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_storage::{FjallStorage, StorageHandle};
use tempfile::TempDir;

use crate::driver::DriverContext;
use crate::jobs::records::transport::{serve_job_record, serve_launch_offer};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::tests::records::{Family, REALM, secret, user};

async fn fixture() -> (TempDir, Arc<DriverContext>, NetHandle, Family) {
    let dir = tempfile::tempdir().expect("temp dir");
    let storage: StorageHandle =
        FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("loopback"),
            realm_id: REALM,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .expect("net handle starts");
    let family = Family::with_local([1u8; 32], Some(net.node_id()));
    seed_config(&storage, &family.config, net.node_id()).await;
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    (dir, context, net, family)
}

async fn seed_config(
    storage: &StorageHandle,
    config: &RealmConfigDocument,
    node_id: aruna_core::NodeId,
) {
    let target = DocumentTarget::RealmConfig {
        realm_id: RealmId(REALM.0),
    };
    let actor = Actor {
        node_id,
        user_id: user(),
        realm_id: REALM,
    };
    storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: config.to_bytes(&actor).expect("config encodes").into(),
            txn_id: None,
        }))
        .await;
}

#[tokio::test]
async fn separates_peer_authority() {
    // An authenticated relay is not an author, and an authorized author does
    // not make an unknown relay a peer: both checks must hold on their own.
    let (_dir, context, net, family) = fixture().await;
    let spec = family.spec();
    let record = JobRecordFrame::new(family.sign(
        &family.holder,
        JobFamilyRecord::Spec(Box::new(spec.clone())),
    ))
    .expect("bounded record");

    let outsider = serve_job_record(
        &context,
        secret(9).public(),
        MetadataTransportMessage::ForwardJobRecord {
            placement: family.placement,
            record: Box::new(record.clone()),
        },
    )
    .await;
    assert_eq!(
        outsider,
        MetadataTransportMessage::ForwardedJobRecord {
            result: Err(JobRecordRejection::Unauthorized),
        }
    );

    let forged = family.spec_for(family.job_id, secret(9).public());
    let forged =
        JobRecordFrame::new(family.sign(&secret(9), JobFamilyRecord::Spec(Box::new(forged))))
            .expect("bounded record");
    let relayed = serve_job_record(
        &context,
        crate::tests::records::node(2),
        MetadataTransportMessage::ForwardJobRecord {
            placement: family.placement,
            record: Box::new(forged),
        },
    )
    .await;
    assert_eq!(
        relayed,
        MetadataTransportMessage::ForwardedJobRecord {
            result: Err(JobRecordRejection::Unauthorized),
        }
    );

    let accepted = serve_job_record(
        &context,
        crate::tests::records::node(2),
        MetadataTransportMessage::ForwardJobRecord {
            placement: family.placement,
            record: Box::new(record),
        },
    )
    .await;
    assert_eq!(
        accepted,
        MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) }
    );
    net.shutdown().await;
}

#[tokio::test]
async fn holds_bound_pages() {
    // A holder acknowledges a spec that binds input pages only once it keeps those pages, so
    // the publisher retries until every holder can serve them after the admitting node is gone.
    use aruna_core::structs::execution::job::{
        CapturedInput, InputContent, InputMode, InputSelection, InputSource, PageEntries, paginate,
    };
    let (_dir, context, net, family) = fixture().await;
    let version_id = ulid::Ulid::from_parts(1, 1);
    let pair = (
        InputSelection {
            source: InputSource::S3 {
                bucket: "bakta".to_string(),
                key: "db/bakta.db".to_string(),
                version_id: Some(version_id.to_string()),
            },
            source_node_id: None,
            dest_key: "db/bakta.db".to_string(),
            mode: InputMode::Mount,
            container_path: Some("/db/bakta.db".to_string()),
            name: None,
            description: None,
        },
        CapturedInput {
            destination_key: "db/bakta.db".to_string(),
            source_node_id: family.holder.public(),
            version_id,
            content: InputContent::Blake3([1; 32]),
            bytes: 1,
            policies: Vec::new(),
            source_group_id: ulid::Ulid::from_parts(2, 2),
            public: false,
            link: None,
            source_pin: None,
            database: None,
            releases: Vec::new(),
            releases_complete: false,
            grant: None,
        },
    );
    let (manifest, pages) = paginate(&[pair], PageEntries::Spec).expect("pages");
    let mut spec = family.spec();
    spec.input_pages = Some(manifest);
    let record = JobFamilyRecord::Spec(Box::new(spec.clone()));
    let record = JobRecordFrame::new(family.sign(&family.holder, record)).expect("bounded");
    let publish = || MetadataTransportMessage::ForwardJobRecord {
        placement: family.placement,
        record: Box::new(record.clone()),
    };
    let missing = serve_job_record(&context, crate::tests::records::node(2), publish()).await;
    assert_eq!(missing, MetadataTransportMessage::ForwardedWriteUnavailable);
    crate::jobs::records::pages::store_pages(&context, &spec.submission_id, &pages)
        .await
        .expect("pages stored");
    let kept = serve_job_record(&context, crate::tests::records::node(2), publish()).await;
    assert_eq!(
        kept,
        MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) }
    );
    net.shutdown().await;
}

#[tokio::test]
async fn refuses_unknown_offer() {
    // An offer from outside the realm is declined before admission, and one naming
    // another target is not this node's launch to accept.
    let (_dir, context, net, family) = fixture().await;
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let offer =
        LaunchFrame::new(family.sign(&family.holder, JobFamilyRecord::Launch(Box::new(launch))))
            .expect("bounded launch");

    let declined = serve_launch_offer(&context, secret(9).public(), offer.clone()).await;
    assert_eq!(
        declined,
        MetadataTransportMessage::ForwardedLaunchOffer {
            result: Err(LaunchDecline::Unauthorized),
        }
    );
    assert_eq!(
        serve_launch_offer(&context, crate::tests::records::node(2), offer).await,
        MetadataTransportMessage::ForwardedLaunchOffer {
            result: Err(LaunchDecline::Unauthorized),
        }
    );
    net.shutdown().await;
}

#[tokio::test]
async fn refuses_page_mismatch() {
    // A peer cannot use a valid local placement to read another submission.
    let (_dir, context, net, family) = fixture().await;
    let placement = aruna_core::structs::placement::record::PlacementRef {
        strategy_id: family.placement.strategy_id,
        shard: family.placement.shard + 1,
    };
    let response = serve_job_record(
        &context,
        crate::tests::records::node(2),
        MetadataTransportMessage::ForwardRecordPage {
            placement,
            submission_id: family.submission_id,
            request_digest: None,
            cursor: None,
            limit: PageLimit::new(1),
        },
    )
    .await;
    assert_eq!(
        response,
        MetadataTransportMessage::ForwardedRecordPage {
            result: Err(JobRecordRejection::Invalid),
        }
    );
    net.shutdown().await;
}
