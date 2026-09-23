//! Checks link push planning, state transitions and token binding without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::credential_encryption::CredentialEncryptionKey;
use crate::invenio::InvenioCredential;
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use crate::structured_id::{BucketId, PlacementHandle};

fn job(nonce: u64) -> JobId {
    JobId::from_parts(
        1_700_000_000_000,
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
        nonce,
    )
    .unwrap()
}

fn link() -> InvenioLink {
    let realm = RealmId::from_bytes([3; 32]);
    InvenioLink {
        link_id: Ulid::from_bytes([1; 16]),
        document_id: Ulid::from_bytes([2; 16]),
        group_id: Ulid::from_bytes([4; 16]),
        connector_id: Ulid::from_bytes([5; 16]),
        endpoint: "https://zenodo.org/api/".into(),
        owner_node: iroh::SecretKey::from_bytes(&[7; 32]).public(),
        owner_node_url: "https://node.example/api/v1".into(),
        created_by: UserId::local(Ulid::from_bytes([6; 16]), realm),
        status: LinkStatus::Enabled,
        auto_publish: false,
        public_files: false,
        metadata_json: "{}".into(),
        remote: LinkRemote::default(),
        last_push: None,
        active_job: None,
        sequence: 0,
        limits: RoCrateLimits::default(),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
    }
}

fn record(id: &str, published: bool, doi: Option<&str>) -> InvenioRecord {
    InvenioRecord {
        id: id.into(),
        url: format!("https://zenodo.org/api/records/{id}"),
        published,
        parent_id: "parent-1".into(),
        revision_id: 3,
        doi: doi.map(str::to_string),
        html_url: None,
    }
}

fn pushed(id: &str, published: bool, doi: Option<&str>) -> PushOutcome {
    PushOutcome::Pushed {
        record: record(id, published, doi),
        event_id: Ulid::from_bytes([9; 16]),
        dataset_digest: Some([8; 32]),
    }
}

#[test]
fn plans_lineage_pushes() {
    let mut link = link();
    let first = link.destination(false);
    assert_eq!((first.draft_id, first.new_version), (None, None));
    assert_eq!(first.link.unwrap().parent_id, None);

    link.begin(job(1), SystemTime::now()).unwrap();
    assert!(link.finish(job(1), &pushed("draft-1", false, None), SystemTime::now()));
    let update = link.destination(false);
    assert_eq!(update.draft_id.as_deref(), Some("draft-1"));
    assert_eq!(update.new_version, None);

    link.begin(job(2), SystemTime::now()).unwrap();
    link.finish(
        job(2),
        &pushed("draft-1", true, Some("10.1/x")),
        SystemTime::now(),
    );
    assert_eq!(link.remote.record_id.as_deref(), Some("draft-1"));
    assert!(link.remote.published && link.remote.draft_id.is_none());
    let next = link.destination(false);
    assert_eq!(next.draft_id, None);
    assert_eq!(next.new_version.as_deref(), Some("draft-1"));
    assert_eq!(next.link.unwrap().published_id.as_deref(), Some("draft-1"));

    link.begin(job(3), SystemTime::now()).unwrap();
    link.finish(job(3), &pushed("draft-2", false, None), SystemTime::now());
    assert_eq!(link.remote.doi.as_deref(), Some("10.1/x"));
    assert_eq!(link.remote.parent_id.as_deref(), Some("parent-1"));
    assert!(!link.remote.published);
    link.auto_publish = true;
    assert!(link.destination(false).publish);
}

#[test]
fn runs_one_push() {
    let mut link = link();
    link.begin(job(1), SystemTime::now()).unwrap();
    assert_eq!(link.sequence, 1);
    assert_eq!(link.begin(job(1), SystemTime::now()), Ok(()));
    assert_eq!(link.sequence, 1);
    assert_eq!(link.begin(job(2), SystemTime::now()), Err(LinkBusy));
    assert!(!link.finish(job(2), &pushed("x", false, None), SystemTime::now()));
    assert_eq!(link.active_job, Some(job(1)));
    assert!(link.finish(job(1), &PushOutcome::Cancelled, SystemTime::now()));
    assert_eq!((link.active_job, link.last_push.is_none()), (None, true));
    assert!(!link.finish(job(1), &PushOutcome::Cancelled, SystemTime::now()));
}

#[test]
fn records_push_failures() {
    let mut link = link();
    link.begin(job(1), SystemTime::now()).unwrap();
    link.finish(
        job(1),
        &PushOutcome::Failed(LinkFailure::RemoteChanged),
        SystemTime::now(),
    );
    assert_eq!(
        link.status,
        LinkStatus::Failed {
            reason: LinkFailure::RemoteChanged
        }
    );
    assert!(!link.rotate(SystemTime::now()));
    link.begin(job(2), SystemTime::now()).unwrap();
    assert_eq!(link.status, LinkStatus::Enabled);
    link.patch(
        &LinkPatch {
            paused: Some(true),
            ..LinkPatch::default()
        },
        SystemTime::now(),
    );
    link.finish(
        job(2),
        &PushOutcome::Failed(LinkFailure::TokenRejected),
        SystemTime::now(),
    );
    assert_eq!(link.status, LinkStatus::Paused);
    link.status = LinkStatus::Failed {
        reason: LinkFailure::TokenRejected,
    };
    assert!(link.rotate(SystemTime::now()));
    assert_eq!(link.status, LinkStatus::Enabled);
    assert_eq!(
        LinkFailure::SourceUnavailable.reason(),
        "source_unavailable"
    );
    assert_eq!(LinkFailure::Other("gone".into()).reason(), "gone");
}

#[test]
fn patch_resumes_link() {
    let mut link = link();
    let pause = LinkPatch {
        paused: Some(true),
        auto_publish: Some(true),
        metadata_json: Some(r#"{"title":"T"}"#.into()),
        ..LinkPatch::default()
    };
    assert!(!link.patch(&pause, SystemTime::now()));
    assert_eq!(link.status, LinkStatus::Paused);
    assert!(link.auto_publish && link.metadata_json.contains("title"));
    let resume = LinkPatch {
        paused: Some(false),
        ..LinkPatch::default()
    };
    assert!(link.patch(&resume, SystemTime::now()));
    assert!(!link.patch(&resume, SystemTime::now()));
    link.status = LinkStatus::Failed {
        reason: LinkFailure::SourceUnavailable,
    };
    assert!(link.patch(&resume, SystemTime::now()));
}

#[test]
fn detects_unpushed_changes() {
    let mut link = link();
    let event = Ulid::from_bytes([9; 16]);
    assert!(link.changed(event, None));
    link.begin(job(1), SystemTime::now()).unwrap();
    link.finish(job(1), &pushed("d", false, None), SystemTime::now());
    assert!(!link.changed(event, Some([1; 32])));
    let later = Ulid::from_bytes([10; 16]);
    assert!(!link.changed(later, Some([8; 32])));
    assert!(link.changed(later, Some([1; 32])));
    assert!(link.changed(later, None));
    let first = link.push_key(later, false);
    link.begin(job(2), SystemTime::now()).unwrap();
    assert_ne!(first, link.push_key(later, false));
    assert_ne!(link.push_key(later, true), link.push_key(later, false));
}

#[test]
fn binds_link_token() {
    let key = CredentialEncryptionKey::derive(&[1; 32]);
    let link = link();
    let (user, group, connector) = (link.created_by, link.group_id, link.connector_id);
    let seal = |link_id| {
        InvenioCredential::seal_link(
            &key,
            user,
            group,
            connector,
            link_id,
            link.endpoint.clone(),
            "link-token",
        )
        .unwrap()
    };
    let sealed = seal(Some(link.link_id));
    let open = |credential: &InvenioCredential| {
        credential.open(&key, user, group, connector, &link.endpoint)
    };
    assert_eq!(open(&sealed).unwrap(), "link-token");
    let mut moved = sealed.clone();
    moved.link_id = Some(Ulid::from_bytes([2; 16]));
    assert!(open(&moved).is_err());
    moved.link_id = None;
    assert!(open(&moved).is_err());
    let mut unbound = seal(None);
    assert_eq!(open(&unbound).unwrap(), "link-token");
    unbound.link_id = Some(link.link_id);
    assert!(open(&unbound).is_err());
    assert!(!format!("{sealed:?}").contains("link-token"));
    assert!(!format!("{link:?}").contains("link-token"));
}
