//! The transport peer and the record publisher are separate authorities.

use std::sync::Arc;

use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, JobRecordFrame, LaunchFrame, StorageEffect};
use aruna_core::events::{JobRecordRejection, LaunchDecline};
use aruna_core::handle::Handle;
use aruna_core::structs::{Actor, JobFamilyRecord, RealmConfigDocument, RealmId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_storage::{FjallStorage, StorageHandle};
use tempfile::TempDir;

use super::fixture::{Family, REALM, secret, user};
use crate::driver::DriverContext;
use crate::jobs::records::transport::{serve_job_record, serve_launch_offer};
use crate::metadata::protocol::MetadataTransportMessage;

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
    let target = DocumentSyncTarget::RealmConfig {
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
        super::fixture::node(2),
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
        super::fixture::node(2),
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
async fn refuses_unknown_offer() {
    // A launch offer from a node outside the realm is declined before any
    // admission work, and an offer naming another target is not this node's
    // launch to accept.
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
        serve_launch_offer(&context, super::fixture::node(2), offer).await,
        MetadataTransportMessage::ForwardedLaunchOffer {
            result: Err(LaunchDecline::Unauthorized),
        }
    );
    net.shutdown().await;
}
