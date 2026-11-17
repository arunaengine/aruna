//! Registers the realm with its registry or withdraws it. Only the management node with the
//! lowest node id reports, and withdrawals stop after a bounded number of attempts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::str::FromStr;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::federation::{RealmDescriptor, RegistrationMode, Signed};
use aruna_core::keyspaces::FEDERATION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::Effects;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use url::Url;

/// How often the reporting node renews its registration.
pub const PUBLISH_INTERVAL: Duration = Duration::from_secs(6 * 3600);
/// Delay of the first publication after a settings change, so renames reach the registry at once.
pub const PUBLISH_SOON: Duration = Duration::from_secs(5);
/// Withdrawals sent while registration is disabled and the registry URL is kept.
pub const MAX_WITHDRAWALS: u8 = 3;
const STATE_KEY: &[u8] = b"publication";

/// Local withdrawal count of one disable cycle, named by the disabled descriptor's issue time
/// and the registry URL, so each cycle gets its own budget.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationState {
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
pub(crate) fn still_due(config: &RealmConfigDocument, node_id: NodeId, publication: &Publication) -> bool {
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
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("operation did not finish")]
    NotFinished,
    #[error("the realm config is missing")]
    ConfigMissing,
    #[error("the publication timer was not armed: {0}")]
    Timer(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum PublishState {
    Init,
    Read,
    Write,
    Finish,
    Error,
}

/// Decides the publication and records the attempt before any request is sent, so a crash
/// cannot repeat withdrawals beyond the bound.
#[derive(Debug, PartialEq)]
pub struct PublishOperation {
    realm_id: RealmId,
    node_id: NodeId,
    state: PublishState,
    output: Option<Result<Publication, PublishError>>,
}

impl PublishOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId) -> Self {
        Self {
            realm_id,
            node_id,
            state: PublishState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: PublishError) -> Effects {
        self.state = PublishState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn finish(&mut self, publication: Publication) -> Effects {
        self.state = PublishState::Finish;
        self.output = Some(Ok(publication));
        smallvec![]
    }

    fn on_read(
        &mut self,
        config: Option<&[u8]>,
        stored: Option<&[u8]>,
    ) -> Result<Effects, PublishError> {
        let Some(config) = config else {
            return Ok(self.finish(Publication::Nothing));
        };
        let config = RealmConfigDocument::from_bytes(config)?;
        let previous: PublicationState = stored
            .map(postcard::from_bytes)
            .transpose()
            .map_err(ConversionError::from)?
            .unwrap_or_default();
        let (publication, next) = decide(&config, self.node_id, previous.clone());
        if next == previous {
            return Ok(self.finish(publication));
        }
        self.state = PublishState::Write;
        self.output = Some(Ok(publication));
        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: FEDERATION_KEYSPACE.to_string(),
            key: STATE_KEY.into(),
            value: postcard::to_allocvec(&next)
                .map_err(ConversionError::from)?
                .into(),
            txn_id: None,
        })])
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(PublishError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for PublishOperation {
    type Output = Publication;
    type Error = PublishError;

    fn start(&mut self) -> Effects {
        self.state = PublishState::Read;
        let config = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (config.storage_keyspace().to_string(), config.storage_key()),
                (FEDERATION_KEYSPACE.to_string(), STATE_KEY.into()),
            ],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PublishState::Read => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, config), (_, stored)] = values.as_slice() else {
                        return self.unexpected_event(
                            "realm config and publication state",
                            format!("{values:?}"),
                        );
                    };
                    match self.on_read(config.as_deref(), stored.as_deref()) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            PublishState::Write => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    self.state = PublishState::Finish;
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage write result", format!("{other:?}")),
            },
            PublishState::Init | PublishState::Finish | PublishState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PublishState::Finish | PublishState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PublishError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Whether this node reports and the settings name a registry.
fn publishes_soon(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    let registry = config.federation.as_ref();
    registry.is_some_and(|settings| settings.registry_url.is_some())
        && is_reporting(config, node_id)
}

#[derive(Debug, PartialEq)]
enum SoonState {
    Idle,
    ReadConfig,
    ArmTimer,
    Done(Result<bool, PublishError>),
}

/// Moves the registry publication up on the reporting node after replicated realm settings
/// materialize here, also when another management node served the change. True when armed.
#[derive(Debug, PartialEq)]
pub struct PublishSoonOperation {
    realm_id: RealmId,
    node_id: NodeId,
    state: SoonState,
}

impl PublishSoonOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId) -> Self {
        Self {
            realm_id,
            node_id,
            state: SoonState::Idle,
        }
    }

    fn label(&self) -> &'static str {
        match self.state {
            SoonState::Idle => "idle",
            SoonState::ReadConfig => "reading the realm config",
            SoonState::ArmTimer => "arming the publication timer",
            SoonState::Done(_) => "done",
        }
    }

    fn decide(&mut self, event: Event) -> Result<Effects, PublishError> {
        let (state, got) = (self.label(), event_kind(&event));
        let expected = match self.state {
            SoonState::ReadConfig => "storage read result",
            _ => "task timer result",
        };
        let unexpected = move || PublishError::UnexpectedEvent {
            state: state.to_string(),
            expected,
            got: got.to_string(),
        };
        match self.state {
            SoonState::ReadConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let bytes = value.ok_or(PublishError::ConfigMissing)?;
                    let config = RealmConfigDocument::from_bytes(&bytes)?;
                    if !publishes_soon(&config, self.node_id) {
                        self.state = SoonState::Done(Ok(false));
                        return Ok(smallvec![]);
                    }
                    self.state = SoonState::ArmTimer;
                    Ok(smallvec![Effect::Task(TaskEffect::ShortenTimer {
                        key: TaskKey::PublishRegistration,
                        after: PUBLISH_SOON,
                    })])
                }
                Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
                _ => Err(unexpected()),
            },
            SoonState::ArmTimer => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SoonState::Done(Ok(true));
                    Ok(smallvec![])
                }
                Event::Task(TaskEvent::Error { message, .. }) => Err(PublishError::Timer(message)),
                _ => Err(unexpected()),
            },
            SoonState::Idle | SoonState::Done(_) => Err(unexpected()),
        }
    }
}

/// The kind of an event, for an error message without formatting its payload.
fn event_kind(event: &Event) -> &'static str {
    match event {
        Event::Storage(_) => "storage event",
        Event::Task(_) => "task event",
        _ => "other event",
    }
}

impl Operation for PublishSoonOperation {
    type Output = bool;
    type Error = PublishError;

    fn start(&mut self) -> Effects {
        self.state = SoonState::ReadConfig;
        let config = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: config.storage_keyspace().to_string(),
            key: config.storage_key(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.decide(event) {
            Ok(effects) => effects,
            Err(error) => {
                self.state = SoonState::Done(Err(error));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, SoonState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            SoonState::Done(result) => result,
            _ => Err(PublishError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::handle::Handle;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::time::unix_timestamp_secs;
    use crate::tasks::task_persistence::read_timer;
    use aruna_core::UserId;
    use aruna_core::federation::{AcceptedRealms, FederationSettings};
    use aruna_core::structs::identity::auth::Actor;
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
    fn realm_url_joins() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let url = realm_url(
            &Url::parse("https://r.example.org/base").unwrap(),
            &realm_id,
        );
        let url = url.unwrap();
        assert_eq!(
            url.as_str(),
            format!("https://r.example.org/base/v1/realms/{realm_id}")
        );
        assert_eq!(
            totals_url(url).unwrap().as_str(),
            format!("https://r.example.org/base/v1/realms/{realm_id}/totals")
        );
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
            task_handle: None,
            compute_handle: None,
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
        let armed = drive(PublishSoonOperation::new(config.realm_id, reporter()), &context).await;
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
            task_handle: None,
            compute_handle: None,
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

    fn soon_read(realm_id: RealmId) -> Effect {
        let target = DocumentTarget::RealmConfig { realm_id };
        Effect::Storage(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })
    }

    fn soon_config(config: &RealmConfigDocument) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(postcard::to_allocvec(config).unwrap().into()),
        })
    }

    fn soon_timer() -> Effect {
        Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::PublishRegistration,
            after: PUBLISH_SOON,
        })
    }

    #[test]
    fn soon_arms_reporter() {
        let config = config(RegistrationMode::Enabled, true);
        let mut operation = PublishSoonOperation::new(config.realm_id, reporter());
        let scheduled = Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::PublishRegistration,
            after: PUBLISH_SOON,
        });
        check_trace(
            &mut operation,
            &[soon_read(config.realm_id)],
            vec![(soon_config(&config), vec![soon_timer()]), (scheduled, vec![])],
        );
        assert_eq!(operation.finalize(), Ok(true));

        // Another node, or settings without a registry, arm nothing.
        let other = lower(node(1), node(2)).1;
        for (config, node_id) in [
            (config.clone(), other),
            (self::config(RegistrationMode::Enabled, false), reporter()),
        ] {
            let mut operation = PublishSoonOperation::new(config.realm_id, node_id);
            check_trace(
                &mut operation,
                &[soon_read(config.realm_id)],
                vec![(soon_config(&config), vec![])],
            );
            assert_eq!(operation.finalize(), Ok(false));
        }
    }

    #[test]
    fn soon_failures_end() {
        let config = config(RegistrationMode::Enabled, true);
        let started = || {
            let mut operation = PublishSoonOperation::new(config.realm_id, reporter());
            operation.start();
            operation
        };
        let mut missing = started();
        missing.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        assert_eq!(missing.finalize(), Err(PublishError::ConfigMissing));
        let mut failed = started();
        failed.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(failed.finalize(), Err(StorageError::Timeout.into()));
        let mut refused = started();
        refused.step(soon_config(&config));
        refused.step(Event::Task(TaskEvent::Error {
            key: Some(TaskKey::PublishRegistration),
            message: "full".to_string(),
        }));
        assert_eq!(refused.finalize(), Err(PublishError::Timer("full".to_string())));
    }

    #[test]
    fn soon_rejects_stray() {
        let config = config(RegistrationMode::Enabled, true);
        let started = || {
            let mut operation = PublishSoonOperation::new(config.realm_id, reporter());
            operation.start();
            operation
        };
        let expected = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (label, effects, error) in rejects_events(started, sample_events, expected) {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert!(matches!(error, PublishError::UnexpectedEvent { .. }), "{label}");
        }
        let arming = || {
            let mut operation = started();
            operation.step(soon_config(&config));
            operation
        };
        let expected = |event: &Event| matches!(event, Event::Task(TaskEvent::Error { .. }));
        for (label, effects, error) in rejects_events(arming, sample_events, expected) {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert!(matches!(error, PublishError::UnexpectedEvent { .. }), "{label}");
        }
        let fresh = || PublishSoonOperation::new(config.realm_id, reporter());
        assert_eq!(rejects_events(fresh, sample_events, |_| false).len(), sample_events().len());
    }

    #[test]
    fn soon_abort_finalize() {
        let config = config(RegistrationMode::Enabled, true);
        let mut operation = PublishSoonOperation::new(config.realm_id, reporter());
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
        operation.step(soon_config(&config));
        check_abort(&mut operation, &[]);
        rejects_finalize(operation);
    }
}
