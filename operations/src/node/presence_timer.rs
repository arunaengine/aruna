//! Runs the realm presence timer: announces this node in the realm's DHT and plans the refresh
//! before the announcement expires. A user device never announces, so its timer stops.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use tracing::error;

use crate::triggers::TimerPlan;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_ops_identity::realm::announce_presence::{
    AnnouncePresenceConfig, AnnouncePresenceError, AnnouncePresenceOperation,
    PRESENCE_REFRESH_AFTER,
};

/// One run of the presence timer. A failed announce is refreshed on the same beat as a good one.
#[derive(Clone, Debug, PartialEq)]
pub struct PresenceTimerOperation {
    realm_id: RealmId,
    node_id: NodeId,
    announcing: bool,
}

impl PresenceTimerOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId) -> Self {
        Self {
            realm_id,
            node_id,
            announcing: false,
        }
    }

    /// A User-kind device is DHT read-only and never announces; an unreadable config is
    /// bootstrap, which does.
    fn is_device(&self, config: &RealmConfigDocument) -> bool {
        (config.node_kind(&self.node_id)).is_some_and(|kind| !kind.is_sync_eligible())
    }
}

impl Operation for PresenceTimerOperation {
    type Output = TimerPlan;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<TimerPlan>, IoError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TimerPlan>, IoError> {
        if self.announcing {
            if let Err(error) = result.child::<(), AnnouncePresenceError>()? {
                error!(?error, "Failed to process realm presence timer event");
            }
            return Ok(Next::Done(TimerPlan::from(Some(PRESENCE_REFRESH_AFTER))));
        }
        let config = result.child::<RealmConfigDocument, GetConfigError>()?;
        if config.is_ok_and(|config| self.is_device(&config)) {
            return Ok(Next::Done(TimerPlan::default()));
        }
        self.announcing = true;
        // The plan arms the refresh, so the announce leaves its own timer alone.
        let announce = AnnouncePresenceOperation::new(AnnouncePresenceConfig {
            realm_id: self.realm_id,
            node_id: self.node_id,
            schedule_refresh: false,
        });
        Ok(Next::Call(IoCall::child(announce)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmNodeKind;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([2; 32])
    }

    fn configured(kind: RealmNodeKind) -> IoResult {
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
        config.ensure_node(node(), kind);
        IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(config)))
    }

    // A device stops its presence timer; a server announces and refreshes on the beat, also
    // after a failed announce, as does a node whose config is not readable yet.
    #[test]
    fn presence_refresh_planned() {
        let owner = aruna_core::user::id::UserId::nil(realm());
        let mut device = PresenceTimerOperation::new(realm(), node());
        device.first().unwrap();
        let stopped = device.next(configured(RealmNodeKind::User { owner }));
        assert_eq!(stopped, Ok(Next::Done(TimerPlan::default())));

        let refresh = Ok(Next::Done(TimerPlan::from(Some(PRESENCE_REFRESH_AFTER))));
        let mut server = PresenceTimerOperation::new(realm(), node());
        server.first().unwrap();
        let announce = server.next(configured(RealmNodeKind::Server));
        assert!(matches!(announce, Ok(Next::Call(IoCall::Child(_)))));
        let failed = AnnouncePresenceError::ScheduleFailed("down".to_string());
        let failed = IoResult::Child(ChildOutcome::new(Err::<(), _>(failed)));
        assert_eq!(server.next(failed), refresh);

        let mut joining = PresenceTimerOperation::new(realm(), node());
        joining.first().unwrap();
        let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
        joining
            .next(IoResult::Child(ChildOutcome::new(missing)))
            .unwrap();
        let announced = IoResult::Child(ChildOutcome::new(Ok::<(), AnnouncePresenceError>(())));
        assert_eq!(joining.next(announced), refresh);
    }
}
