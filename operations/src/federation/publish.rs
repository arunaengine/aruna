//! Registers the realm with its registry or withdraws it. Only the management node with the
//! lowest node id reports, and withdrawals stop after a bounded number of attempts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::str::FromStr;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::Effect;
use aruna_core::errors::ConversionError;
use aruna_core::events::Event;
use aruna_core::federation::{PUBLISH_SOON, RealmDescriptor, RegistrationMode, Signed};
use aruna_core::keyspaces::FEDERATION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{done, encoded_row, read_row, values};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::Key;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

/// How often the reporting node renews its registration.
pub(crate) const PUBLISH_INTERVAL: Duration = Duration::from_secs(6 * 3600);
/// Withdrawals sent while registration is disabled and the registry URL is kept.
const MAX_WITHDRAWALS: u8 = 3;
const STATE_KEY: &[u8] = b"publication";

/// Local withdrawal count of one disable cycle, named by the disabled descriptor's issue time
/// and the registry URL, so each cycle gets its own budget.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PublicationState {
    pub cycle: Option<(u64, Url)>,
    pub withdrawals: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Publication {
    Nothing,
    Register {
        registry_url: Url,
        descriptor: Signed<RealmDescriptor>,
        nodes_configured: Option<u64>,
    },
    Withdraw {
        registry_url: Url,
        /// Issue time of the disabled settings' descriptor.
        issued_at: u64,
    },
}

/// Whether `node_id` is the management node with the lowest id.
pub fn is_reporting(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    config
        .nodes
        .iter()
        .filter(|node| node.kind == RealmNodeKind::Management)
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .min_by_key(|node| *node.as_bytes())
        == Some(node_id)
}

/// The publication this node owes and the state to keep afterwards.
fn decide(
    config: &RealmConfigDocument,
    node_id: NodeId,
    state: PublicationState,
) -> (Publication, PublicationState) {
    let Some(settings) = config.federation.as_ref() else {
        return (Publication::Nothing, state);
    };
    let Some(registry_url) = settings.registry_url.clone() else {
        return (Publication::Nothing, state);
    };
    if !is_reporting(config, node_id) {
        return (Publication::Nothing, state);
    }
    let issued_at = settings.descriptor.payload.issued_at;
    let cycle = Some((issued_at, registry_url.clone()));
    let used = if state.cycle == cycle {
        state.withdrawals
    } else {
        0
    };
    match settings.registration {
        RegistrationMode::Enabled => (
            Publication::Register {
                registry_url,
                descriptor: settings.descriptor.clone(),
                nodes_configured: u64::try_from(config.nodes.len()).ok(),
            },
            PublicationState::default(),
        ),
        RegistrationMode::Disabled if used < MAX_WITHDRAWALS => (
            Publication::Withdraw {
                registry_url,
                issued_at,
            },
            PublicationState {
                cycle,
                withdrawals: used + 1,
            },
        ),
        RegistrationMode::Disabled => (Publication::Nothing, state),
    }
}

/// Whether a captured publication still matches the current settings: this node still
/// reports, and the mode, registry URL and descriptor issue time are unchanged.
pub(crate) fn still_due(
    config: &RealmConfigDocument,
    node_id: NodeId,
    publication: &Publication,
) -> bool {
    let Some(settings) = config.federation.as_ref() else {
        return false;
    };
    let (registry_url, issued_at, mode) = match publication {
        Publication::Nothing => return false,
        Publication::Register {
            registry_url,
            descriptor,
            ..
        } => (
            registry_url,
            descriptor.payload.issued_at,
            RegistrationMode::Enabled,
        ),
        Publication::Withdraw {
            registry_url,
            issued_at,
        } => (registry_url, *issued_at, RegistrationMode::Disabled),
    };
    is_reporting(config, node_id)
        && settings.registration == mode
        && settings.registry_url.as_ref() == Some(registry_url)
        && settings.descriptor.payload.issued_at == issued_at
}

#[derive(Debug, Error, PartialEq)]
pub enum PublishError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("the realm config is missing")]
    ConfigMissing,
    #[error("the publication timer was not armed: {0}")]
    Timer(String),
}

/// The realm config row of `realm_id`.
fn config_read(realm_id: RealmId) -> (String, Key) {
    let config = DocumentTarget::RealmConfig { realm_id };
    (config.storage_keyspace().to_string(), config.storage_key())
}

/// Decides the publication and records the attempt before any request is sent, so a crash
/// cannot repeat withdrawals beyond the bound.
#[derive(Debug, PartialEq)]
pub(crate) struct PublishOperation {
    realm_id: RealmId,
    node_id: NodeId,
    /// The decided publication while its state write runs.
    decided: Option<Publication>,
}

impl PublishOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId) -> Self {
        Self {
            realm_id,
            node_id,
            decided: None,
        }
    }

    fn on_read(&mut self, config: Option<&[u8]>, stored: Option<&[u8]>) -> PublishTurn {
        let Some(config) = config else {
            return Ok(Next::Done(Publication::Nothing));
        };
        let config = RealmConfigDocument::from_bytes(config)?;
        let previous: PublicationState = stored
            .map(postcard::from_bytes)
            .transpose()
            .map_err(ConversionError::from)?
            .unwrap_or_default();
        let (publication, next) = decide(&config, self.node_id, previous.clone());
        if next == previous {
            return Ok(Next::Done(publication));
        }
        self.decided = Some(publication);
        let row = encoded_row(FEDERATION_KEYSPACE, STATE_KEY, &next)?;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

type PublishTurn = Result<Next<Publication>, PublishError>;

impl Operation for PublishOperation {
    type Output = Publication;
    type Error = PublishError;

    fn first(&mut self) -> PublishTurn {
        let state = (FEDERATION_KEYSPACE.to_string(), STATE_KEY.into());
        Ok(Next::Call(IoCall::Read(vec![
            config_read(self.realm_id),
            state,
        ])))
    }

    fn next(&mut self, result: IoResult) -> PublishTurn {
        match self.decided.take() {
            None => {
                let [config, stored] = values::<2>(result)?;
                self.on_read(config.as_deref(), stored.as_deref())
            }
            Some(publication) => {
                done(result)?;
                Ok(Next::Done(publication))
            }
        }
    }
}

/// Whether this node reports and the settings name a registry.
fn publishes_soon(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    let registry = config.federation.as_ref();
    registry.is_some_and(|settings| settings.registry_url.is_some())
        && is_reporting(config, node_id)
}

/// Moves the registry publication up on the reporting node after replicated realm settings
/// materialize here, also when another management node served the change. True when armed.
#[derive(Debug, PartialEq)]
pub(crate) struct PublishSoonOperation {
    realm_id: RealmId,
    node_id: NodeId,
    arming: bool,
}

impl PublishSoonOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId) -> Self {
        Self {
            realm_id,
            node_id,
            arming: false,
        }
    }

    fn arm(&mut self, result: IoResult) -> Result<Next<bool>, PublishError> {
        let [bytes] = values::<1>(result)?;
        let config = RealmConfigDocument::from_bytes(&bytes.ok_or(PublishError::ConfigMissing)?)?;
        if !publishes_soon(&config, self.node_id) {
            return Ok(Next::Done(false));
        }
        self.arming = true;
        let timer = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::PublishRegistration,
            after: PUBLISH_SOON,
        });
        Ok(Next::Call(IoCall::Effect(Box::new(timer))))
    }
}

impl Operation for PublishSoonOperation {
    type Output = bool;
    type Error = PublishError;

    fn first(&mut self) -> Result<Next<bool>, PublishError> {
        let (space, key) = config_read(self.realm_id);
        Ok(Next::Call(read_row(&space, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, PublishError> {
        if !self.arming {
            return self.arm(result);
        }
        let IoResult::Event(event) = result else {
            let got = result.label();
            return Err(IoError::Unexpected {
                expected: "a task timer result",
                got,
            }
            .into());
        };
        match *event {
            Event::Task(TaskEvent::TimerScheduled { .. }) => Ok(Next::Done(true)),
            Event::Task(TaskEvent::Error { message, .. }) => Err(PublishError::Timer(message)),
            event => Err(IoError::Unexpected {
                expected: "a task timer result",
                got: event.label(),
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::tasks::task_persistence::read_timer;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::federation::{AcceptedRealms, FederationSettings};
    use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
    use aruna_core::time::unix_timestamp_secs;
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn lower(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
        if a.as_bytes() < b.as_bytes() {
            (a, b)
        } else {
            (b, a)
        }
    }

    fn config(registration: RegistrationMode, registry: bool) -> RealmConfigDocument {
        let key = SigningKey::from_bytes(&[4; 32]);
        let realm_id = RealmId::from_bytes(key.verifying_key().to_bytes());
        let capabilities = NodeCapabilities::management_node(key).unwrap();
        let url = |value: &str| Url::parse(value).unwrap();
        let descriptor = RealmDescriptor {
            realm_id,
            name: "Realm".to_string(),
            description: String::new(),
            api_url: url("https://api.example.org"),
            portal_url: url("https://portal.example.org"),
            contact: None,
            login_providers: Vec::new(),
            issued_at: 1,
        };
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let (first, second) = lower(node(1), node(2));
        config.ensure_node(first, RealmNodeKind::Management);
        config.ensure_node(second, RealmNodeKind::Management);
        config.ensure_node(node(3), RealmNodeKind::Server);
        config.federation = Some(FederationSettings {
            name: descriptor.name.clone(),
            description: String::new(),
            contact: None,
            login_providers: Vec::new(),
            api_url: descriptor.api_url.clone(),
            portal_url: descriptor.portal_url.clone(),
            registry_url: registry.then(|| url("https://registry.example.org")),
            registration,
            accepted_realms: AcceptedRealms::None,
            descriptor: Signed::sign(descriptor, &capabilities).unwrap(),
        });
        config
    }

    fn reporter() -> NodeId {
        lower(node(1), node(2)).0
    }

    #[test]
    fn registers_when_enabled() {
        let state = PublicationState {
            cycle: None,
            withdrawals: 2,
        };
        let (publication, next) =
            decide(&config(RegistrationMode::Enabled, true), reporter(), state);
        assert!(matches!(
            publication,
            Publication::Register {
                nodes_configured: Some(3),
                ..
            }
        ));
        assert_eq!(next, PublicationState::default());
    }

    #[test]
    fn lowest_management_reports() {
        // The higher management node and a server node stay silent.
        let config = config(RegistrationMode::Enabled, true);
        let higher = lower(node(1), node(2)).1;
        for node_id in [higher, node(3)] {
            let (publication, _) = decide(&config, node_id, PublicationState::default());
            assert_eq!(publication, Publication::Nothing);
        }
    }

    #[test]
    fn withdrawals_bounded() {
        let config = config(RegistrationMode::Disabled, true);
        let mut state = PublicationState::default();
        for attempt in 1..=MAX_WITHDRAWALS {
            let (publication, next) = decide(&config, reporter(), state.clone());
            assert!(matches!(publication, Publication::Withdraw { .. }));
            assert_eq!(next.withdrawals, attempt);
            state = next;
        }
        let (publication, next) = decide(&config, reporter(), state.clone());
        assert_eq!(publication, Publication::Nothing);
        assert_eq!(next, state);
    }

    #[test]
    fn new_cycle_budget() {
        // A later disable cycle or another registry starts a fresh count.
        let mut config = config(RegistrationMode::Disabled, true);
        let used = PublicationState {
            cycle: Some((1, Url::parse("https://registry.example.org").unwrap())),
            withdrawals: MAX_WITHDRAWALS,
        };
        let (publication, _) = decide(&config, reporter(), used.clone());
        assert_eq!(publication, Publication::Nothing);
        let settings = config.federation.as_mut().unwrap();
        settings.descriptor.payload.issued_at = 2;
        let (publication, next) = decide(&config, reporter(), used.clone());
        assert!(matches!(
            publication,
            Publication::Withdraw { issued_at: 2, .. }
        ));
        assert_eq!(next.withdrawals, 1);
        let settings = config.federation.as_mut().unwrap();
        settings.descriptor.payload.issued_at = 1;
        settings.registry_url = Some(Url::parse("https://other.example.org").unwrap());
        let (publication, _) = decide(&config, reporter(), used);
        assert!(matches!(publication, Publication::Withdraw { .. }));
    }

    #[test]
    fn stale_capture_discarded() {
        let enabled = config(RegistrationMode::Enabled, true);
        let (captured, _) = decide(&enabled, reporter(), PublicationState::default());
        assert!(still_due(&enabled, reporter(), &captured));
        assert!(!still_due(&enabled, lower(node(1), node(2)).1, &captured));
        let disabled = config(RegistrationMode::Disabled, true);
        assert!(!still_due(&disabled, reporter(), &captured));
        assert!(!still_due(
            &config(RegistrationMode::Enabled, false),
            reporter(),
            &captured
        ));
        let mut moved = enabled.clone();
        let settings = moved.federation.as_mut().unwrap();
        settings.registry_url = Some(Url::parse("https://other.example.org").unwrap());
        assert!(!still_due(&moved, reporter(), &captured));
        let mut resigned = enabled;
        let settings = resigned.federation.as_mut().unwrap();
        settings.descriptor.payload.issued_at = 2;
        assert!(!still_due(&resigned, reporter(), &captured));
    }

    #[test]
    fn cleared_url_silent() {
        // Without a registry URL nothing is sent, not even a withdrawal.
        let config = config(RegistrationMode::Disabled, false);
        let (publication, _) = decide(&config, reporter(), PublicationState::default());
        assert_eq!(publication, Publication::Nothing);
    }

    #[test]
    fn reporter_publishes_soon() {
        // The management node that served the change may not report; the reporter acts.
        let unset = config(RegistrationMode::Enabled, false);
        let config = config(RegistrationMode::Enabled, true);
        assert!(publishes_soon(&config, reporter()));
        assert!(!publishes_soon(&config, lower(node(1), node(2)).1));
        assert!(!publishes_soon(&unset, reporter()));
    }

    #[tokio::test]
    async fn materialized_change_shortens() {
        // Replicated settings arriving at the reporter persist a due time a few seconds away.
        let dir = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let config = config(RegistrationMode::Enabled, true);
        let target = DocumentTarget::RealmConfig {
            realm_id: config.realm_id,
        };
        let actor = Actor {
            node_id: reporter(),
            user_id: UserId::local(Ulid::from_bytes([1; 16]), config.realm_id),
            realm_id: config.realm_id,
        };
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        let key = TaskKey::PublishRegistration;
        let storage = &context.storage_handle;
        let other = lower(node(1), node(2)).1;
        let skipped = drive(PublishSoonOperation::new(config.realm_id, other), &context).await;
        assert_eq!(skipped, Ok(false));
        assert_eq!(read_timer(storage, &key).await.unwrap(), None);
        // Without a task handle the runner still persists the due time before it refuses.
        let armed = drive(
            PublishSoonOperation::new(config.realm_id, reporter()),
            &context,
        )
        .await;
        assert!(matches!(armed, Err(PublishError::Timer(_))));
        let due = read_timer(storage, &key).await.unwrap().unwrap();
        let limit = (unix_timestamp_secs() + PUBLISH_SOON.as_secs() + 1) * 1000;
        assert!(due.due_unix_millis <= limit);
    }

    #[tokio::test]
    async fn disabled_survives_restart() {
        // The attempt count is stored, so a restarted node does not withdraw again.
        let dir = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let config = config(RegistrationMode::Disabled, true);
        let target = DocumentTarget::RealmConfig {
            realm_id: config.realm_id,
        };
        let actor = Actor {
            node_id: reporter(),
            user_id: UserId::local(Ulid::from_bytes([1; 16]), config.realm_id),
            realm_id: config.realm_id,
        };
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        let mut sent = 0;
        for _ in 0..MAX_WITHDRAWALS + 2 {
            let publication = drive(PublishOperation::new(config.realm_id, reporter()), &context)
                .await
                .unwrap();
            if matches!(publication, Publication::Withdraw { .. }) {
                sent += 1;
            }
        }
        assert_eq!(sent, MAX_WITHDRAWALS);
    }

    fn soon_read(realm_id: RealmId) -> Result<Next<bool>, PublishError> {
        let target = DocumentTarget::RealmConfig { realm_id };
        let read = read_row(target.storage_keyspace(), target.storage_key());
        Ok(Next::Call(read))
    }

    fn soon_config(config: &RealmConfigDocument) -> IoResult {
        IoResult::Values(vec![Some(postcard::to_allocvec(config).unwrap().into())])
    }

    fn soon_timer() -> Result<Next<bool>, PublishError> {
        let timer = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::PublishRegistration,
            after: PUBLISH_SOON,
        });
        Ok(Next::Call(IoCall::Effect(Box::new(timer))))
    }

    fn task(event: TaskEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Task(event)))
    }

    #[test]
    fn soon_arms_reporter() {
        let config = config(RegistrationMode::Enabled, true);
        let mut operation = PublishSoonOperation::new(config.realm_id, reporter());
        assert_eq!(operation.first(), soon_read(config.realm_id));
        assert_eq!(operation.next(soon_config(&config)), soon_timer());
        let scheduled = task(TaskEvent::TimerScheduled {
            key: TaskKey::PublishRegistration,
            after: PUBLISH_SOON,
        });
        assert_eq!(operation.next(scheduled), Ok(Next::Done(true)));

        // Another node, or settings without a registry, arm nothing.
        let other = lower(node(1), node(2)).1;
        for (config, node_id) in [
            (config.clone(), other),
            (self::config(RegistrationMode::Enabled, false), reporter()),
        ] {
            let mut operation = PublishSoonOperation::new(config.realm_id, node_id);
            operation.first().unwrap();
            assert_eq!(operation.next(soon_config(&config)), Ok(Next::Done(false)));
        }
    }

    #[test]
    fn soon_failures_end() {
        let config = config(RegistrationMode::Enabled, true);
        let started = || {
            let mut operation = PublishSoonOperation::new(config.realm_id, reporter());
            operation.first().unwrap();
            operation
        };
        let missing = started().next(IoResult::Values(vec![None]));
        assert_eq!(missing, Err(PublishError::ConfigMissing));
        let mut refused = started();
        refused.next(soon_config(&config)).unwrap();
        let error = task(TaskEvent::Error {
            key: Some(TaskKey::PublishRegistration),
            message: "full".to_string(),
        });
        assert_eq!(
            refused.next(error),
            Err(PublishError::Timer("full".to_string()))
        );
    }
}
