//! Stores replicated repository links: a newer revision of the same owner wins, and a tombstone
//! keeps every later replay out.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::registry::{derive_placement, revision_entries};
use super::{ApplyError, PlacementOutcome, Step, invalid};
use crate::document::{DocumentChange, DocumentChangeKind, DocumentTarget};
use crate::keyspaces::SYNC_REVISION_KEYSPACE;
use crate::row_plan::{PlanRows, Planned, RowPlan};
use crate::storage_entries::sync_revision_key;
use crate::structs::identity::realm::RealmId;
use crate::types::Value;

/// Folds one link change, its sidecar and manifest entry into one transaction.
#[derive(Debug, PartialEq)]
pub struct LinkPlan {
    pub realm_id: RealmId,
    pub target: DocumentTarget,
    pub bytes: Option<Vec<u8>>,
    pub change: DocumentChange,
}

impl RowPlan for LinkPlan {
    type Output = PlacementOutcome<bool>;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<PlacementOutcome<bool>>> {
        let DocumentTarget::RepositoryLink { document_id, .. } = &self.target else {
            return Ok(Planned::output(PlacementOutcome::Rejected));
        };
        let change = &self.change;
        match derive_placement(rows, self.realm_id, None, *document_id, change.placement)? {
            PlacementOutcome::Accepted(_) => {}
            PlacementOutcome::Deferred(dependency) => {
                return Ok(Planned::output(PlacementOutcome::Deferred(dependency)));
            }
            PlacementOutcome::Rejected => return Ok(Planned::output(PlacementOutcome::Rejected)),
        }
        let local = rows
            .read(SYNC_REVISION_KEYSPACE, sync_revision_key(&self.target))?
            .map(|value| postcard::from_bytes::<DocumentChange>(&value))
            .transpose()
            .map_err(invalid)?;
        let newer = match local {
            None => true,
            Some(local) if local.kind == DocumentChangeKind::Delete => false,
            Some(local) => {
                change.current > local.current && change.current.actor == local.current.actor
            }
        };
        if !newer {
            return Ok(Planned::output(PlacementOutcome::Accepted(false)));
        }
        let row = (
            self.target.storage_keyspace().to_string(),
            self.target.storage_key(),
        );
        let mut writes = revision_entries(&self.target, change)?;
        let deletes = match &self.bytes {
            Some(bytes) => {
                writes.push((row.0, row.1, Value::from(bytes.clone())));
                Vec::new()
            }
            None => vec![row],
        };
        Ok(Planned::changes(
            PlacementOutcome::Accepted(true),
            deletes,
            writes,
        ))
    }
}
