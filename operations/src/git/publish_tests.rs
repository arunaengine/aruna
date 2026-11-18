//! Tests publishing Git records call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::realm::RealmConfigDocument;

use super::super::testing::{auth, child, dataset, node, realm};
use super::*;

fn config() -> RealmConfigDocument {
    RealmConfigDocument::new(realm(), Vec::new(), 3)
}

fn unlock() -> GitChange {
    GitChange::Unlock {
        id: Ulid::from_bytes([3; 16]),
    }
}

fn publishing(node_id: Option<NodeId>) -> PublishRecordOperation {
    let extras = (None, Vec::new());
    PublishRecordOperation::new(node_id, &dataset(), auth().user_id, unlock(), extras)
}

// A node without a network identity cannot say that it holds the document.
#[test]
fn holders_need_node() {
    let mut holders = HoldersOperation::new(None, &dataset());
    assert_eq!(holders.first(), Err(GitError::Unavailable));
}

// A node outside the document's holders is told so.
#[test]
fn outsider_not_holder() {
    let mut holders = HoldersOperation::new(Some(node(9)), &dataset());
    holders.first().unwrap();
    let answer = holders.next(child(Ok::<_, GetConfigError>(config())));
    assert_eq!(answer, Err(GitError::NotHolder));
}

// An unreadable realm config leaves the store unavailable.
#[test]
fn missing_config_unavailable() {
    let mut publish = publishing(Some(node(1)));
    publish.first().unwrap();
    let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
    assert_eq!(publish.next(child(missing)), Err(GitError::Unavailable));
}

// The time on a record is the runner's stamp, and a full history refuses a new record.
#[test]
fn full_history_refuses() {
    let mut publish = publishing(Some(node(1)));
    publish.stamp(42);
    assert_eq!(publish.now_ms, 42);
    publish.step = PublishStep::Scan;
    publish.config = Some(config());
    let record = |seed: u128| GitRecord {
        event_id: Ulid::from(seed),
        realm_id: realm(),
        group_id: dataset().group_id,
        document_id: dataset().document_id,
        placement: dataset().placement,
        user_id: auth().user_id,
        node_id: node(1),
        occurred_at_ms: 1,
        change: unlock(),
    };
    let full: Vec<_> = (0..MAX_RECORDS as u128).map(record).collect();
    let answer = publish.next(child(Ok::<_, GitError>(full)));
    assert_eq!(answer, Err(GitError::Full));
}
