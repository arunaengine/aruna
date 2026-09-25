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
        generation: 0,
        warning: None,
        direction: LinkDirection::Push,
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
        concept_doi: None,
        in_review: false,
        warning: None,
    }
}

fn pushed(id: &str, published: bool, doi: Option<&str>) -> PushOutcome {
    PushOutcome::Pushed {
        record: Box::new(record(id, published, doi)),
        event_id: Ulid::from_bytes([9; 16]),
        dataset_digest: Some([8; 32]),
        files: vec!["data.txt".into()],
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
    link.finish(
        job(3),
        &pushed("draft-2", false, Some("10.1/y")),
        SystemTime::now(),
    );
    assert_eq!(link.remote.doi.as_deref(), Some("10.1/y"));
    assert!(link.remote.doi_reserved && !link.remote.published);
    assert_eq!(link.remote.parent_id.as_deref(), Some("parent-1"));
    assert_eq!(link.remote.files, ["data.txt"]);
    let target = link.destination(false).link.unwrap();
    assert_eq!((target.revision_id, target.files.len()), (Some(3), 1));
    link.auto_publish = true;
    assert!(!link.destination(false).publish, "auto_publish waits");
}

#[test]
fn stores_draft_early() {
    let mut link = link();
    let draft = record("draft-1", false, Some("10.1/r"));
    assert!(!link.draft(job(1), &draft, SystemTime::now()));
    link.begin(job(1), SystemTime::now()).unwrap();
    assert!(link.draft(job(1), &draft, SystemTime::now()));
    assert!(!link.draft(job(1), &record("x", true, None), SystemTime::now()));
    link.finish(
        job(1),
        &PushOutcome::Failed(LinkFailure::Other("upload failed".into())),
        SystemTime::now(),
    );
    assert_eq!(link.remote.draft_id.as_deref(), Some("draft-1"));
    assert_eq!(link.remote.revision_id, Some(3));
    assert!(link.remote.doi_reserved);
    assert_eq!(link.destination(false).draft_id.as_deref(), Some("draft-1"));
}

#[test]
fn review_blocks_publishing() {
    let mut link = link();
    link.auto_publish = true;
    link.begin(job(1), SystemTime::now()).unwrap();
    let mut submitted = record("draft-1", false, Some("10.1/r"));
    submitted.in_review = true;
    let outcome = PushOutcome::Pushed {
        record: Box::new(submitted),
        event_id: Ulid::from_bytes([9; 16]),
        dataset_digest: None,
        files: Vec::new(),
    };
    link.finish(job(1), &outcome, SystemTime::UNIX_EPOCH);
    assert_eq!(link.remote.review, LinkReview::Pending);
    assert_eq!(link.publish_due_ms(), None);
    let state = RemoteState {
        draft: None,
        latest: Some(record("draft-1", true, Some("10.1/r"))),
        review: LinkReview::Accepted,
        files: Vec::new(),
    };
    link.accept(&state, SystemTime::now());
    assert_eq!(link.remote.review, LinkReview::Accepted);
    assert!(link.remote.published && !link.remote.doi_reserved);
    assert_eq!(link.remote.record_id.as_deref(), Some("draft-1"));
}

#[test]
fn auto_publish_waits() {
    let mut link = link();
    link.auto_publish = true;
    assert_eq!(link.publish_due_ms(), None);
    link.begin(job(1), SystemTime::now()).unwrap();
    link.finish(job(1), &pushed("d", false, None), SystemTime::UNIX_EPOCH);
    assert_eq!(link.publish_due_ms(), Some(AUTO_PUBLISH_QUIET_MS));
    link.status = LinkStatus::Paused;
    assert_eq!(link.publish_due_ms(), None);
}

#[test]
fn accepts_remote_base() {
    let mut link = link();
    link.status = LinkStatus::Failed {
        reason: LinkFailure::RemoteChanged,
    };
    link.warning = Some("checksum".into());
    let mut edited = record("draft-2", false, None);
    edited.revision_id = 9;
    let state = RemoteState {
        draft: Some(edited),
        latest: Some(record("v1", true, Some("10.1/v1"))),
        review: LinkReview::None,
        files: vec!["remote.txt".into()],
    };
    link.accept(&state, SystemTime::now());
    assert_eq!(link.status, LinkStatus::Enabled);
    assert_eq!(link.warning, None);
    assert_eq!(link.remote.record_id.as_deref(), Some("v1"));
    assert_eq!(link.remote.draft_id.as_deref(), Some("draft-2"));
    assert_eq!(link.remote.revision_id, Some(9));
    assert_eq!(link.remote.files, ["remote.txt"]);
}

#[test]
fn debounces_queued_changes() {
    let document = Ulid::from_bytes([2; 16]);
    let first = LinkQueueEntry::debounce(document, None, 1_000);
    assert_eq!(first.due_at_ms, 1_000 + LINK_DEBOUNCE_MS);
    let next = LinkQueueEntry::debounce(document, Some(&first), 5_000);
    assert_eq!(
        (next.due_at_ms, next.first_at_ms),
        (5_000 + LINK_DEBOUNCE_MS, 1_000)
    );
    let late = LinkQueueEntry::debounce(document, Some(&next), 1_000 + LINK_DEBOUNCE_CAP_MS);
    assert_eq!(late.due_at_ms, 1_000 + LINK_DEBOUNCE_CAP_MS);
    // An old auto_publish wait is no change, so a new change still gets its quiet time.
    let publish = LinkQueueEntry {
        document_id: document,
        due_at_ms: 1_000 + AUTO_PUBLISH_QUIET_MS,
        first_at_ms: 1_000,
    };
    let change = LinkQueueEntry::debounce(document, Some(&publish), 600_000);
    assert_eq!(
        (change.due_at_ms, change.first_at_ms),
        (600_000 + LINK_DEBOUNCE_MS, 600_000)
    );
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

#[test]
fn orders_sync_changes() {
    let mut link = link();
    link.stamp(5_000);
    let first = link.sync_change(PlacementRef::NIL);
    // A clock that went back still moves the generation forward.
    link.stamp(10);
    let second = link.sync_change(PlacementRef::NIL);
    assert_eq!(second.current.generation, 5_001);
    assert!(second.current > first.current);
    assert_eq!(first, {
        let mut copy = link.clone();
        copy.generation = 5_000;
        copy.sync_change(PlacementRef::NIL)
    });
    let delete = link.delete_change(PlacementRef::NIL);
    assert_eq!(delete.kind, DocumentChangeKind::Delete);
    assert!(delete.current > second.current);
    assert_eq!(delete.current.actor, link.owner_node);
}

fn pulling(auto_update: bool) -> InvenioLink {
    let mut link = link();
    link.direction = LinkDirection::Pull(Box::new(LinkPull {
        auto_update,
        options: InvenioOptions::default(),
        target: ImportRoCrateTarget {
            bucket: "research".into(),
            prefix: "zenodo".into(),
        },
        latest_remote_id: None,
        latest_revision: None,
        last_checked_at: None,
        next_check_ms: 0,
        failures: 0,
        revision: None,
        local_changed: false,
    }));
    link.hold(
        &record("v1", true, Some("10.5281/zenodo.1")),
        Ulid::from_bytes([20; 16]),
        SystemTime::UNIX_EPOCH,
    );
    link
}

fn found(latest_id: &str, revision: u64, local: u8) -> PullCheck {
    PullCheck::Found {
        latest_id: latest_id.into(),
        revision,
        local: Some(Ulid::from_bytes([local; 16])),
    }
}

#[test]
fn pull_checks_versions() {
    let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000);
    let mut link = pulling(true);
    assert!(!link.checked(&found("v1", 3, 20), now));
    assert_eq!(link.pull_reason(), None);
    let pull = link.pull().unwrap();
    assert_eq!(pull.next_check_ms, 1_000_000 + PULL_CHECK_MS);
    assert_eq!(pull.last_checked_at, Some(now));
    // A repository edit of the held version counts as an update.
    assert!(link.checked(&found("v1", 4, 20), now));
    assert!(link.checked(&found("v2", 1, 20), now));
    assert_eq!(link.pull_reason(), Some("update_available"));
    // A local edit holds the update back until the user pulls.
    assert!(!link.checked(&found("v2", 1, 21), now));
    assert_eq!(link.pull_reason(), Some("local_changed"));
    let mut manual = pulling(false);
    assert!(!manual.checked(&found("v2", 1, 20), now));
    assert_eq!(manual.pull_reason(), Some("update_available"));
    manual.status = LinkStatus::Paused;
    assert_eq!(manual.pull_reason(), None);
}

#[test]
fn pull_checks_back_off() {
    let mut link = pulling(true);
    let now = SystemTime::UNIX_EPOCH;
    let waits = (0..12)
        .map(|_| {
            link.checked(&PullCheck::Unavailable, now);
            link.pull().unwrap().next_check_ms
        })
        .collect::<Vec<_>>();
    assert_eq!(
        waits[..3],
        [PULL_RETRY_MS, 2 * PULL_RETRY_MS, 4 * PULL_RETRY_MS]
    );
    assert_eq!(waits[11], PULL_CHECK_MS);
    link.checked(&found("v1", 3, 20), now);
    assert_eq!(link.pull().unwrap().failures, 0);
}

#[test]
fn pull_records_version() {
    let now = SystemTime::now();
    let mut link = pulling(false);
    link.checked(&found("v2", 5, 21), now);
    let v2 = InvenioRecord {
        revision_id: 5,
        ..record("v2", true, Some("10.5281/zenodo.2"))
    };
    let revision = Ulid::from_bytes([22; 16]);
    assert!(!link.pulled(job(1), &v2, revision, now), "only its own job");
    link.begin(job(1), now).unwrap();
    assert!(link.pulled(job(1), &v2, revision, now));
    assert_eq!(link.active_job, None);
    assert_eq!(link.remote.record_id.as_deref(), Some("v2"));
    assert_eq!(link.remote.doi.as_deref(), Some("10.5281/zenodo.2"));
    assert_eq!(link.pull().unwrap().revision, Some(revision));
    assert!(!link.update_available() && !link.pull().unwrap().local_changed);
    let patch = LinkPatch {
        auto_update: Some(true),
        ..LinkPatch::default()
    };
    link.patch(&patch, now);
    assert!(link.pull().unwrap().auto_update);
}

#[test]
fn matches_same_lineage() {
    let pull = pulling(false);
    let mut push = link();
    assert!(!push.same_lineage(&pull));
    push.remote.parent_id = Some("parent-1".into());
    push.endpoint = "https://zenodo.org/api".into();
    assert!(push.same_lineage(&pull));
    push.endpoint = "https://sandbox.zenodo.org/api/".into();
    assert!(!push.same_lineage(&pull));
}
