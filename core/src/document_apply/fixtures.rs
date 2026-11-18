//! Builds the events and child answers the document apply tests feed their operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use ulid::Ulid;

use super::ApplyError;
use super::validate::{ConfigSnapshot, ValidateAdminPlan};
use crate::UserId;
use crate::admin_documents::{
    AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use crate::calls::{ChildOutcome, IoResult};
use crate::config_authority::AdminEventValidation;
use crate::document::{BatchEvent, DocumentEvent, DocumentTarget};
use crate::structs::identity::auth::Actor;
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::record::PlacementRef;

pub(super) fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

pub(super) fn node() -> crate::NodeId {
    iroh::SecretKey::from_bytes(&[2; 32]).public()
}

pub(super) fn actor() -> irokle::ActorId {
    irokle::ActorId::from_bytes([3; 32])
}

pub(super) fn signature() -> iroh::Signature {
    iroh::Signature::from_bytes(&[0; 64])
}

pub(super) fn config_target() -> DocumentTarget {
    DocumentTarget::RealmConfig { realm_id: realm() }
}

pub(super) fn config_topic() -> irokle::TopicId {
    config_target().sync_topic_id(realm(), &PlacementRef::NIL)
}

fn admin_event(
    seq: u64,
    target: AdminDocumentTarget,
    op: AdminDocumentOperation,
) -> AdminDocumentEvent {
    AdminDocumentEvent {
        event_id: Ulid::from_parts(seq, 1),
        target,
        origin_node_id: node(),
        origin_seq: seq,
        observed: AdminDocumentClock::default(),
        actor: Actor {
            node_id: node(),
            user_id: UserId::nil(realm()),
            realm_id: realm(),
        },
        op,
    }
}

/// A realm config event that may join a buffered run.
pub(super) fn run_event(seq: u64) -> AdminDocumentEvent {
    let op = AdminDocumentOperation::ConfigTransitionAborted {
        transition_id: Ulid::from_parts(seq, 2),
    };
    let target = AdminDocumentTarget::RealmConfig { realm_id: realm() };
    admin_event(seq, target, op)
}

/// A realm config event that applies on its own.
pub(super) fn single_event(seq: u64) -> AdminDocumentEvent {
    let op = AdminDocumentOperation::ConfigDescriptionSet {
        description: format!("description {seq}"),
    };
    let target = AdminDocumentTarget::RealmConfig { realm_id: realm() };
    admin_event(seq, target, op)
}

/// A distribution snapshot of the set `set`.
pub(super) fn snapshot_event(seq: u64, set: u64) -> AdminDocumentEvent {
    let target = AdminDocumentTarget::Distribution {
        realm_id: realm(),
        set_id: Ulid::from_parts(set, 3),
    };
    let op = AdminDocumentOperation::ConfigDescriptionSet {
        description: format!("snapshot {seq}"),
    };
    admin_event(seq, target, op)
}

/// The admin event as it travels on the realm config topic.
pub(super) fn wrapped(event: AdminDocumentEvent) -> DocumentEvent {
    DocumentEvent::AdminOperation {
        target: config_target(),
        event: Box::new(event),
        placement: PlacementRef::NIL,
        origin_signature: signature(),
    }
}

pub(super) fn batch_event(event: DocumentEvent) -> BatchEvent {
    BatchEvent {
        event,
        actor: actor(),
        actor_seq: 1,
        publisher_allowed: true,
    }
}

/// The check of `event` on the realm config topic, without a cached snapshot.
pub(super) fn check_plan(event: AdminDocumentEvent) -> ValidateAdminPlan {
    ValidateAdminPlan {
        topic_id: config_topic(),
        actor_id: actor(),
        target: config_target(),
        event,
        realm_id: realm(),
        placement: PlacementRef::NIL,
        signature: signature(),
        cache: None,
    }
}

/// A child that finished with `value`.
pub(super) fn child<T: Any + Send + Debug + PartialEq>(value: T) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<T, ApplyError>(value)))
}

/// A child that failed with `error`, where `T` is what it would have returned.
pub(super) fn failed<T: Any + Send + Debug + PartialEq>(error: ApplyError) -> IoResult {
    IoResult::Child(ChildOutcome::new(Err::<T, ApplyError>(error)))
}

/// The answer of a [`ValidateAdminPlan`].
pub(super) fn checked(validation: AdminEventValidation) -> IoResult {
    child((validation, None::<Arc<ConfigSnapshot>>))
}
