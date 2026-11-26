//! Tests that record transport separates the peer from the publisher and refuses bad offers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, JobRecordFrame, LaunchFrame, PageLimit, StorageEffect};
use aruna_core::events::{JobRecordRejection, LaunchDecline};
use aruna_core::handle::Handle;
use aruna_core::structs::execution::job::family::JobFamilyRecord;
use aruna_core::structs::execution::job::pages::{InputPage, PageManifest};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_p2p::NetHandle;
use aruna_p2p::config::{DiscoveryMethod, NetConfig, RelayMethod};
use aruna_storage::storage::handle::StorageHandle;
use aruna_storage::storage::memory::MemoryStorage;
use tempfile::TempDir;

use crate::tests::records::{Family, REALM, secret, serve_job_record, serve_launch_offer, user};
use aruna_core::metadata::protocol::MetadataTransportMessage;
use aruna_runner::driver::DriverContext;

pub(super) async fn fixture() -> (TempDir, Arc<DriverContext>, NetHandle, Family) {
    let dir = tempfile::tempdir().expect("temp dir");
    let storage: StorageHandle = MemoryStorage::open();
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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
        vfs_handle: None,
    });
    (dir, context, net, family)
}

async fn seed_config(
    storage: &StorageHandle,
    config: &RealmConfigDocument,
    node_id: aruna_core::id::NodeId,
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

/// The fixture spec created now: a holder refuses a spec older than the horizon.
fn fresh_spec(family: &Family) -> aruna_core::structs::execution::job::family::LogicalJobSpec {
    let mut spec = family.spec();
    spec.created_at_ms = aruna_core::time::unix_timestamp_millis();
    spec.store_digest().expect("spec digest stored")
}

#[tokio::test]
async fn separates_peer_authority() {
    // An authenticated relay is not an author, and an authorized author does
    // not make an unknown relay a peer: both checks must hold on their own.
    let (_dir, context, net, family) = fixture().await;
    let spec = fresh_spec(&family);
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
            attached: Vec::new(),
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
            attached: Vec::new(),
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
            attached: Vec::new(),
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

// A receipt offered with its launch is refused when the launch's scheduler had no open
// membership interval at the launch's clock, although the receipt binds that launch.
#[tokio::test]
async fn refuses_absent_scheduler() {
    let (_dir, context, net, family) = fixture().await;
    let spec = fresh_spec(&family);
    let frame = |key: &iroh::SecretKey, record| {
        JobRecordFrame::new(family.sign(key, record)).expect("bounded record")
    };
    let offer = |record, attached| MetadataTransportMessage::ForwardJobRecord {
        placement: family.placement,
        record: Box::new(record),
        attached,
    };
    let held = frame(
        &family.holder,
        JobFamilyRecord::Spec(Box::new(spec.clone())),
    );
    let peer = crate::tests::records::node(2);
    let stored = serve_job_record(&context, peer, offer(held, Vec::new())).await;
    let accepted = MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) };
    assert_eq!(stored, accepted);
    let budget = family.budget(&spec, secret(9).public());
    let launch = family.launch(&spec, secret(9).public(), 0);
    let receipt = family.receipt(&launch, 1);
    let attached = vec![
        frame(&secret(9), JobFamilyRecord::Launch(Box::new(launch))),
        frame(&secret(9), JobFamilyRecord::Budget(budget)),
    ];
    let receipt = frame(&family.target, JobFamilyRecord::Receipt(Box::new(receipt)));
    let refused = serve_job_record(&context, peer, offer(receipt, attached)).await;
    let unauthorized = MetadataTransportMessage::ForwardedJobRecord {
        result: Err(JobRecordRejection::Unauthorized),
    };
    assert_eq!(refused, unauthorized);
    net.shutdown().await;
}

/// One captured input of a spec as its input pages.
fn spec_pages(family: &Family) -> (PageManifest, Vec<InputPage>) {
    use aruna_core::structs::execution::job::input::{
        CapturedInput, InputContent, InputMode, InputSelection, InputSource,
    };
    use aruna_core::structs::execution::job::pages::{PageEntries, paginate};
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
            served_by: None,
        },
    );
    paginate(&[pair], PageEntries::Spec).expect("pages")
}

// A claim offered with the paged spec it binds is acknowledged only once this holder keeps the
// spec's pages, also when the offer is retried after both were admitted, with or without the
// spec attached.
#[tokio::test]
async fn holds_attached_pages() {
    let (_dir, context, net, family) = fixture().await;
    let (manifest, pages) = spec_pages(&family);
    let mut spec = fresh_spec(&family);
    spec.input_pages = Some(manifest);
    let spec = spec.store_digest().expect("spec digest stored");
    let claim = JobFamilyRecord::Claim(family.claim(&spec));
    let claim = JobRecordFrame::new(family.sign(&family.holder, claim)).expect("bounded");
    let submission = spec.submission_id;
    let spec = JobFamilyRecord::Spec(Box::new(spec));
    let spec = JobRecordFrame::new(family.sign(&family.holder, spec)).expect("bounded");
    let offer = |attached| MetadataTransportMessage::ForwardJobRecord {
        placement: family.placement,
        record: Box::new(claim.clone()),
        attached,
    };
    let peer = crate::tests::records::node(2);

    let missing = serve_job_record(&context, peer, offer(vec![spec.clone()])).await;
    assert_eq!(missing, MetadataTransportMessage::ForwardedWriteUnavailable);
    let retried = serve_job_record(&context, peer, offer(vec![spec.clone()])).await;
    assert_eq!(retried, MetadataTransportMessage::ForwardedWriteUnavailable);
    // The sender attaches the spec only after a spec-needed answer, so a plain retry follows.
    let plain = serve_job_record(&context, peer, offer(Vec::new())).await;
    assert_eq!(plain, MetadataTransportMessage::ForwardedWriteUnavailable);
    let writes = crate::jobs::records::pages::page_rows(&submission, &pages).expect("page rows");
    let store = aruna_core::effects::StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    context.storage_handle.send_storage_effect(store).await;
    let kept = serve_job_record(&context, peer, offer(Vec::new())).await;
    let accepted = MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) };
    assert_eq!(kept, accepted);
    net.shutdown().await;
}

// A receipt retried alone is acknowledged only once this holder keeps the input pages of the
// stored launch it binds, which an earlier offer left without them.
#[tokio::test]
async fn holds_launch_pages() {
    let (_dir, context, net, family) = fixture().await;
    let (manifest, pages) = spec_pages(&family);
    let spec = fresh_spec(&family);
    let mut launch = family.launch(&spec, family.holder.public(), 0);
    launch.input_pages = Some(manifest);
    let receipt = family.receipt(&launch, 1);
    let frame = |key: &iroh::SecretKey, record| {
        JobRecordFrame::new(family.sign(key, record)).expect("bounded record")
    };
    let budget = family.budget(&spec, family.holder.public());
    let submission = spec.submission_id;
    let offers = [
        frame(&family.holder, JobFamilyRecord::Spec(Box::new(spec))),
        frame(&family.holder, JobFamilyRecord::Budget(budget)),
    ];
    let offer = |record| MetadataTransportMessage::ForwardJobRecord {
        placement: family.placement,
        record: Box::new(record),
        attached: Vec::new(),
    };
    let peer = crate::tests::records::node(2);
    let accepted = MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) };
    for record in offers {
        assert_eq!(
            serve_job_record(&context, peer, offer(record)).await,
            accepted
        );
    }
    let launch = frame(&family.holder, JobFamilyRecord::Launch(Box::new(launch)));
    let missing = serve_job_record(&context, peer, offer(launch)).await;
    assert_eq!(missing, MetadataTransportMessage::ForwardedWriteUnavailable);
    let receipt = frame(&family.target, JobFamilyRecord::Receipt(Box::new(receipt)));

    let waiting = serve_job_record(&context, peer, offer(receipt.clone())).await;
    assert_eq!(waiting, MetadataTransportMessage::ForwardedWriteUnavailable);
    let writes = crate::jobs::records::pages::page_rows(&submission, &pages).expect("page rows");
    let store = aruna_core::effects::StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    context.storage_handle.send_storage_effect(store).await;
    assert_eq!(
        serve_job_record(&context, peer, offer(receipt)).await,
        accepted
    );
    net.shutdown().await;
}

#[tokio::test]
async fn holds_bound_pages() {
    // A holder acknowledges a spec that binds input pages only once it keeps those pages, so
    // the publisher retries until every holder can serve them after the admitting node is gone.
    let (_dir, context, net, family) = fixture().await;
    let (manifest, pages) = spec_pages(&family);
    let mut spec = fresh_spec(&family);
    spec.input_pages = Some(manifest);
    let spec = spec.store_digest().expect("spec digest stored");
    let record = JobFamilyRecord::Spec(Box::new(spec.clone()));
    let record = JobRecordFrame::new(family.sign(&family.holder, record)).expect("bounded");
    let publish = || MetadataTransportMessage::ForwardJobRecord {
        placement: family.placement,
        attached: Vec::new(),
        record: Box::new(record.clone()),
    };
    let missing = serve_job_record(&context, crate::tests::records::node(2), publish()).await;
    assert_eq!(missing, MetadataTransportMessage::ForwardedWriteUnavailable);
    let writes =
        crate::jobs::records::pages::page_rows(&spec.submission_id, &pages).expect("page rows");
    let store = aruna_core::effects::StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    context.storage_handle.send_storage_effect(store).await;
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
