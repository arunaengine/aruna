//! Admits a coordinator's publication into a member bucket of a distributed set: against the
//! record this node holds before the negotiation, and again inside the apply transaction, so a
//! record installed in between conflicts the apply or refuses it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
use aruna_core::structs::storage::distribution::DistributionView;
use aruna_core::types::Effects;
use smallvec::smallvec;

use super::{IncomingVersionError, IncomingVersionOperation, IncomingVersionState};

impl IncomingVersionOperation {
    /// Reads this node's record of the destination's set; `recheck` reads it in the apply
    /// transaction.
    pub(super) fn read_publication_record(&mut self, recheck: bool) -> Effects {
        let Some(binding) = self.destination_binding.as_ref() else {
            return match recheck {
                true => self.fail(IncomingVersionError::ForeignPublication),
                false => self.reject_negotiation(IncomingVersionError::ForeignPublication),
            };
        };
        let key = binding.set_id.to_bytes().to_vec().into();
        let txn_id = match recheck {
            true => self.txn_id,
            false => None,
        };
        self.state = match recheck {
            true => IncomingVersionState::RecheckPublication,
            false => IncomingVersionState::ReadPublicationRecord,
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: DISTRIBUTION_KEYSPACE.to_string(),
            key,
            txn_id,
        })]
    }

    /// Whether the record admits this publication from the authenticated peer.
    fn admission(&self, event: Event) -> Result<(), IncomingVersionError> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return Err(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let (Some(binding), Some(publication)) = (
            self.destination_binding.as_ref(),
            self.manifest.distribution.as_ref(),
        ) else {
            return Err(IncomingVersionError::ForeignPublication);
        };
        let view = value
            .map(|value| DistributionView::from_bytes(value.as_ref()))
            .transpose()?
            .ok_or(IncomingVersionError::RecordMissing)?;
        let key = &self.manifest.key;
        view.snapshot()
            .admit(binding, self.publisher_node_id, key, publication)?;
        Ok(())
    }

    pub(super) fn accept_publication_record(&mut self, event: Event) -> Effects {
        match self.admission(event) {
            Ok(()) => self.read_existing_version(),
            Err(error) => self.reject_negotiation(error),
        }
    }

    pub(super) fn accept_publication_recheck(&mut self, event: Event) -> Effects {
        match self.admission(event) {
            Ok(()) => self.verify_replaced(),
            Err(error) => self.fail(error),
        }
    }
}
