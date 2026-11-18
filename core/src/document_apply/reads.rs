//! Reads the realm and group documents that replicated events are checked against.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::str::FromStr;

use super::{Step, invalid};
use crate::NodeId;
use crate::admin_documents::AdminDocumentTarget;
use crate::deferred_topics::DocumentSyncDependency;
use crate::document::DocumentTarget;
use crate::keyspaces::DOCUMENT_STATE_KEYSPACE;
use crate::reducer::{AdminDocumentState, decode_reducer_state};
use crate::row_plan::PlanRows;
use crate::storage_entries::reducer_state_key;
use crate::structs::identity::group::GroupAuthorizationDocument;
use crate::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument, RealmId};
use crate::sync_checks::published_by;
use crate::types::{GroupId, Value};

fn target_row(rows: &mut PlanRows, target: &DocumentTarget) -> Step<Option<Value>> {
    Ok(rows.read(target.storage_keyspace(), target.storage_key())?)
}

pub fn realm_config(rows: &mut PlanRows, realm_id: RealmId) -> Step<Option<RealmConfigDocument>> {
    let target = DocumentTarget::RealmConfig { realm_id };
    let value = target_row(rows, &target)?;
    Ok(value
        .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
        .transpose()
        .map_err(invalid)?)
}

pub fn realm_authorization(
    rows: &mut PlanRows,
    realm_id: RealmId,
) -> Step<Option<RealmAuthorizationDocument>> {
    let target = DocumentTarget::RealmAuthorization { realm_id };
    let value = target_row(rows, &target)?;
    Ok(value
        .map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes))
        .transpose()
        .map_err(invalid)?)
}

pub fn group_authorization(
    rows: &mut PlanRows,
    group_id: GroupId,
) -> Step<Option<GroupAuthorizationDocument>> {
    let target = DocumentTarget::GroupAuthorization { group_id };
    let value = target_row(rows, &target)?;
    Ok(value
        .map(|bytes| GroupAuthorizationDocument::from_bytes(&bytes))
        .transpose()
        .map_err(invalid)?)
}

pub fn reducer_state(
    rows: &mut PlanRows,
    target: &AdminDocumentTarget,
) -> Step<Option<AdminDocumentState>> {
    let value = rows.read(DOCUMENT_STATE_KEYSPACE, reducer_state_key(target))?;
    Ok(value
        .map(|bytes| decode_reducer_state(&bytes))
        .transpose()
        .map_err(invalid)?)
}

/// Whether the transport publisher of a relayed event is a realm node allowed to relay. User
/// nodes are never relays, so they never appear here.
pub fn relay_allowed(
    rows: &mut PlanRows,
    topic_id: irokle::TopicId,
    publisher: irokle::ActorId,
    realm_id: RealmId,
) -> Step<bool> {
    let Some(config) = realm_config(rows, realm_id)? else {
        return Ok(false);
    };
    Ok(config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .any(|node_id| published_by(topic_id, publisher, &node_id)))
}

/// Whether the state a deferred topic waits for has replicated.
pub fn dependency_available(rows: &mut PlanRows, dependency: DocumentSyncDependency) -> Step<bool> {
    Ok(match dependency {
        DocumentSyncDependency::RealmConfig(realm_id) => realm_config(rows, realm_id)?.is_some(),
        DocumentSyncDependency::RealmAuthorization(realm_id) => {
            realm_authorization(rows, realm_id)?.is_some()
        }
        DocumentSyncDependency::PlacementStrategy {
            realm_id,
            strategy_id,
        } => realm_config(rows, realm_id)?.is_some_and(|config| {
            config.realm_id == realm_id && config.strategy(&strategy_id).is_some()
        }),
        DocumentSyncDependency::GroupAuthorization(group_id) => {
            group_authorization(rows, group_id)?.is_some()
        }
    })
}
