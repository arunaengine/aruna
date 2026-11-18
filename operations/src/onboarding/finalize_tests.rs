//! Tests node join finalization as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{called, child, node, runs_child};
use aruna_core::onboarding::OnboardingPurpose;
use aruna_core::structs::storage::node_info::KIND_LABEL_KEY;

fn signing_key() -> SigningKey {
    SigningKey::from_bytes(&[3; 32])
}

fn realm() -> RealmId {
    RealmId::from_bytes(signing_key().verifying_key().to_bytes())
}

fn input() -> BootstrapFinalizeInput {
    BootstrapFinalizeInput {
        enrollment_id: Ulid::from_bytes([1; 16]),
        secret_hash: "secret-hash".to_string(),
        node_id: node(2),
        realm_id: realm(),
        local_node_id: node(1),
        realm_signing_key: signing_key(),
        now: 10,
        node_location: Some("eu".to_string()),
        node_weight: Some(100),
        node_labels: Default::default(),
    }
}

fn config() -> RealmConfigDocument {
    RealmConfigDocument::new(realm(), Vec::new(), 2)
}

fn secret(mode: OnboardingMode) -> OnboardingSecretRecord {
    OnboardingSecretRecord {
        enrollment_id: Ulid::from_bytes([1; 16]),
        secret_hash: "secret-hash".to_string(),
        mode,
        purpose: OnboardingPurpose::NodeEnrollment,
        expires_at: 100,
        claimed_node_id: None,
    }
}

fn ticket(documents: Vec<DocumentTarget>) -> OnboardingTicket {
    OnboardingTicket::issue(&signing_key(), &realm(), node(2), 100, documents).unwrap()
}

fn mutated() -> IoResult {
    child(Ok::<_, MutatePlacementError>(config()))
}

fn listed() -> IoResult {
    child(Ok::<_, EnsureConfigError>(config()))
}

fn conflict() -> IoResult {
    child(Err::<RealmConfigDocument, _>(
        EnsureConfigError::StorageError(StorageError::TransactionConflict),
    ))
}

fn topic_answer(result: Result<(), TopicPeerError>) -> IoResult {
    IoResult::Event(Box::new(Event::Net(NetEvent::TopicPeers(result))))
}

fn consumed(consumed_now: bool) -> IoResult {
    child(Ok::<_, ConsumeSecretError>(ConsumeSecretOutput {
        record: secret(OnboardingMode::Server),
        consumed_now,
    }))
}

/// Runs up to the listing of the joiner in the realm config.
fn reserved(mode: OnboardingMode) -> FinalizeBootstrapOperation {
    let mut operation = FinalizeBootstrapOperation::new(input());
    assert!(runs_child(operation.first()));
    let record = child(Ok::<_, ReserveSecretError>(secret(mode)));
    assert!(runs_child(operation.next(record)));
    assert_eq!(operation.stage, FinalizeStage::Activate);
    assert!(runs_child(operation.next(mutated())));
    assert_eq!(operation.stage, FinalizeStage::Node { attempts: 1 });
    operation
}

/// Runs up to the sync ticket.
fn before_ticket(mode: OnboardingMode) -> FinalizeBootstrapOperation {
    let mut operation = reserved(mode);
    assert!(runs_child(operation.next(listed())));
    assert!(runs_child(
        operation.next(child(Ok::<_, InterestError>(true)))
    ));
    assert!(runs_child(operation.next(mutated())));
    let started = child(Ok::<_, MutatePlacementError>(Vec::<Ulid>::new()));
    assert!(runs_child(operation.next(started)));
    let reconciled = PlacementReconcileOutcome::default();
    assert!(runs_child(
        operation.next(child(Ok::<_, IoError>(reconciled)))
    ));
    assert_eq!(operation.stage, FinalizeStage::Issue);
    operation
}

fn issued() -> IoResult {
    child(Ok::<_, IssueSyncError>(ticket(Vec::new())))
}

fn sync_topics() -> OnboardingSyncTopics {
    OnboardingSyncTopics {
        shared: vec![irokle::TopicId::from_bytes([1; 32])],
        shard: vec![irokle::TopicId::from_bytes([2; 32])],
    }
}

/// Runs up to the answer to the request that creates the shared topics.
fn ensuring_topics() -> FinalizeBootstrapOperation {
    let mut operation = before_ticket(OnboardingMode::Server);
    assert!(runs_child(operation.next(issued())));
    let topics = child(Ok::<_, BootstrapFinalizeError>(sync_topics()));
    let IoCall::Effect(effect) = called(operation.next(topics)) else {
        panic!("expected the topic effect");
    };
    assert_eq!(
        *effect,
        Effect::Net(NetEffect::EnsureTopics {
            topics: sync_topics().shared,
            peers: vec![node(2)],
        })
    );
    operation
}

/// Runs up to the consumption of the secret.
fn consuming() -> FinalizeBootstrapOperation {
    let mut operation = ensuring_topics();
    let IoCall::Effect(effect) = called(operation.next(topic_answer(Ok(())))) else {
        panic!("expected the peer effect");
    };
    let mut topics = sync_topics().shared;
    topics.extend(sync_topics().shard);
    assert_eq!(
        *effect,
        Effect::Net(NetEffect::AllowTopicPeers {
            topics,
            peers: vec![node(2)],
        })
    );
    assert!(runs_child(operation.next(topic_answer(Ok(())))));
    assert_eq!(operation.stage, FinalizeStage::Consume);
    operation
}

fn finished() -> BootstrapFinalizeOutput {
    BootstrapFinalizeOutput {
        mode: OnboardingMode::Server,
        onboarding_sync_ticket: ticket(Vec::new()).encode().unwrap(),
    }
}

#[test]
fn reserved_label_refused() {
    let mut request = input();
    request
        .node_labels
        .insert(KIND_LABEL_KEY.to_string(), "Server".to_string());
    assert_eq!(
        FinalizeBootstrapOperation::new(request).first(),
        Err(BootstrapFinalizeError::ReservedNodeLabel(
            KIND_LABEL_KEY.to_string()
        ))
    );
}

#[test]
fn long_location_refused() {
    let mut request = input();
    request.node_location = Some("x".repeat(65));
    assert_eq!(
        FinalizeBootstrapOperation::new(request).first(),
        Err(BootstrapFinalizeError::LongNodeLocation)
    );
}

#[test]
fn claimed_secret_stops() {
    let mut operation = FinalizeBootstrapOperation::new(input());
    operation.first().unwrap();
    let claimed = child(Err::<OnboardingSecretRecord, _>(
        ReserveSecretError::AlreadyClaimed,
    ));
    assert_eq!(
        operation.next(claimed),
        Err(BootstrapFinalizeError::Reserve(
            ReserveSecretError::AlreadyClaimed
        ))
    );
}

#[test]
fn listing_conflicts_retry() {
    let mut operation = reserved(OnboardingMode::Server);
    for _ in 1..NODE_UPDATE_RETRIES {
        assert!(runs_child(operation.next(conflict())));
    }
    assert_eq!(
        operation.next(conflict()),
        Err(BootstrapFinalizeError::EnsureRealmConfig(
            EnsureConfigError::StorageError(StorageError::TransactionConflict)
        ))
    );
}

#[test]
fn listing_error_stops() {
    let failed = child(Err::<RealmConfigDocument, _>(
        EnsureConfigError::ConfigMissing,
    ));
    assert_eq!(
        reserved(OnboardingMode::Server).next(failed),
        Err(BootstrapFinalizeError::EnsureRealmConfig(
            EnsureConfigError::ConfigMissing
        ))
    );
}

#[test]
fn watch_failure_continues() {
    let mut operation = reserved(OnboardingMode::Server);
    operation.next(listed()).unwrap();
    let failed = child(Err::<bool, _>(InterestError::Encode("bad".to_string())));
    assert!(runs_child(operation.next(failed)));
    assert_eq!(operation.stage, FinalizeStage::Joiner);
}

#[test]
fn device_skips_topics() {
    let owner = UserId::nil(realm());
    let mut operation = before_ticket(OnboardingMode::User { owner });
    assert!(runs_child(operation.next(issued())));
    assert_eq!(operation.stage, FinalizeStage::Consume);
}

#[test]
fn server_derives_topics() {
    let mut operation = before_ticket(OnboardingMode::Server);
    assert!(runs_child(operation.next(issued())));
    assert_eq!(operation.stage, FinalizeStage::Topics);
}

#[test]
fn missing_network_reported() {
    assert_eq!(
        ensuring_topics().next(topic_answer(Err(TopicPeerError::NoNetwork))),
        Err(BootstrapFinalizeError::NetHandleUnavailable)
    );
}

#[test]
fn refused_admission_reported() {
    let failed = TopicPeerError::Failed("full".to_string());
    assert_eq!(
        ensuring_topics().next(topic_answer(Err(failed))),
        Err(BootstrapFinalizeError::PeerAdmission("full".to_string()))
    );
}

#[test]
fn repeat_join_silent() {
    assert_eq!(
        consuming().next(consumed(false)),
        Ok(Next::Done(finished()))
    );
}

#[test]
fn first_join_notifies() {
    let mut operation = consuming();
    assert!(runs_child(operation.next(consumed(true))));
    assert_eq!(operation.stage, FinalizeStage::Notify);
    let sent = child(Ok::<(), IoError>(()));
    assert_eq!(operation.next(sent), Ok(Next::Done(finished())));
}

#[test]
fn failed_notice_ignored() {
    let mut operation = consuming();
    operation.next(consumed(true)).unwrap();
    let failed = child(Err::<(), _>(IoError::Conflict));
    assert_eq!(operation.next(failed), Ok(Next::Done(finished())));
}

fn noticing() -> OnboardedNoticeOperation {
    let mut operation = OnboardedNoticeOperation::new(realm(), node(2));
    operation.stamp(5);
    assert!(runs_child(operation.first()));
    operation
}

fn authorization(answer: Option<RealmAuthorizationDocument>) -> IoResult {
    child(Ok::<_, ReadAuthorizationError>(answer))
}

#[test]
fn no_authorization_skips() {
    assert_eq!(noticing().next(authorization(None)), Ok(Next::Done(())));
}

#[test]
fn adminless_realm_skips() {
    let document = RealmAuthorizationDocument::default_realm_doc(realm());
    assert_eq!(
        noticing().next(authorization(Some(document))),
        Ok(Next::Done(()))
    );
}

#[test]
fn admins_are_notified() {
    let mut document = RealmAuthorizationDocument::default_realm_doc(realm());
    let admin = UserId::new(Ulid::from_bytes([4; 16]), realm());
    for role in document.roles.values_mut() {
        if role.name == "realm_admin" {
            role.assigned_users.insert(admin);
        }
    }
    let mut operation = noticing();
    assert!(runs_child(operation.next(authorization(Some(document)))));
    let sent = child(Ok::<(), EmitNotificationsError>(()));
    assert_eq!(operation.next(sent), Ok(Next::Done(())));
}

fn deriving(documents: Vec<DocumentTarget>) -> SyncTopicsOperation {
    let mut operation = SyncTopicsOperation::new(realm(), node(2), &ticket(documents));
    assert!(runs_child(operation.first()));
    operation
}

#[test]
fn shared_documents_listed() {
    let target = DocumentTarget::NodeInfo {
        realm_id: realm(),
        node_id: node(1),
    };
    let expected = target.sync_topic_id(realm(), &PlacementRef::NIL);
    let answer = child(Ok::<_, GetConfigError>(config()));
    assert_eq!(
        deriving(vec![target]).next(answer),
        Ok(Next::Done(OnboardingSyncTopics {
            shared: vec![expected],
            shard: Vec::new(),
        }))
    );
}

#[test]
fn unplaced_user_refused() {
    let user_id = UserId::local(Ulid::from_bytes([4; 16]), realm());
    let answer = child(Ok::<_, GetConfigError>(config()));
    assert_eq!(
        deriving(vec![DocumentTarget::User { user_id }]).next(answer),
        Err(BootstrapFinalizeError::UserPlacementUnavailable(user_id))
    );
}

#[test]
fn missing_config_reported() {
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert_eq!(
        deriving(Vec::new()).next(missing),
        Err(BootstrapFinalizeError::GetRealmConfig(
            GetConfigError::DocumentNotFound
        ))
    );
}
