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
use aruna_core::federation::{
    FederationError, RealmDescriptor, RealmKpis, Registration, RegistrationMode, Signed, Withdrawal,
};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::FEDERATION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::time::unix_timestamp_secs;
use aruna_core::types::Effects;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use url::Url;

use crate::driver::{DriverContext, drive};
use crate::metadata::stats::{count_realm_documents, count_realm_groups};
use crate::realm::get_config::GetConfigOperation;
use crate::tasks::task_persistence::persist_task_effect;

/// How often the reporting node renews its registration.
pub const PUBLISH_INTERVAL: Duration = Duration::from_secs(6 * 3600);
/// Delay of the first publication after a settings change on this node.
pub const PUBLISH_SOON: Duration = Duration::from_secs(60);
/// Withdrawals sent while registration is disabled and the registry URL is kept.
pub const MAX_WITHDRAWALS: u8 = 3;
const STATE_KEY: &[u8] = b"publication";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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
fn is_reporting(config: &RealmConfigDocument, node_id: NodeId) -> bool {
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
fn still_due(config: &RealmConfigDocument, node_id: NodeId, publication: &Publication) -> bool {
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

/// Runs the decision and sends the signed request through the screened egress client.
pub async fn publish_registration(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    capabilities: &NodeCapabilities,
) -> Result<(), String> {
    let publication = drive(PublishOperation::new(realm_id, node_id), context)
        .await
        .map_err(|error| error.to_string())?;
    let now = unix_timestamp_secs();
    let sign_error = |error: FederationError| error.to_string();
    let captured = publication.clone();
    let (method, registry_url, body) = match publication {
        Publication::Nothing => return Ok(()),
        Publication::Register {
            registry_url,
            descriptor,
            nodes_configured,
        } => {
            let kpis = realm_kpis(context, realm_id, nodes_configured).await;
            let registration = Registration {
                descriptor,
                kpis,
                observed_at: now,
                issued_at: now,
            };
            let signed = Signed::sign(registration, capabilities).map_err(sign_error)?;
            let body = serde_json::to_vec(&signed).map_err(|error| error.to_string())?;
            (reqwest::Method::PUT, registry_url, body)
        }
        Publication::Withdraw { registry_url, .. } => {
            let withdrawal = Withdrawal {
                realm_id,
                issued_at: now,
            };
            let signed = Signed::sign(withdrawal, capabilities).map_err(sign_error)?;
            let body = serde_json::to_vec(&signed).map_err(|error| error.to_string())?;
            (reqwest::Method::DELETE, registry_url, body)
        }
    };
    let blob = context
        .blob_handle
        .as_ref()
        .ok_or("no blob handle for registry egress")?;
    let url = realm_url(&registry_url, &realm_id)?;
    // The KPI counts take time; settings may have changed since the decision.
    let current = drive(GetConfigOperation::new(realm_id), context)
        .await
        .map_err(|error| error.to_string())?;
    if !still_due(&current, node_id, &captured) {
        return Ok(());
    }
    let response = blob
        .repository_request(method, url)
        .map_err(|error| error.to_string())?
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!("registry answered {}", response.status()));
    }
    Ok(())
}

/// The realm's totals; a count that fails is unknown (`None`), never zero.
async fn realm_kpis(
    context: &DriverContext,
    realm_id: RealmId,
    nodes_configured: Option<u64>,
) -> RealmKpis {
    RealmKpis {
        live_datasets: count_realm_documents(context, realm_id)
            .await
            .inspect_err(|error| warn!(error = %error, "Dataset count unavailable"))
            .ok()
            .flatten(),
        groups: count_realm_groups(context, realm_id)
            .await
            .inspect_err(|error| warn!(error = %error, "Group count unavailable"))
            .ok(),
        nodes_configured,
    }
}

/// The registry route of one realm below the configured registry base URL.
fn realm_url(registry_url: &Url, realm_id: &RealmId) -> Result<Url, String> {
    let mut base = registry_url.clone();
    if !base.path().ends_with('/') {
        base.set_path(&format!("{}/", base.path()));
    }
    base.join(&format!("v1/realms/{realm_id}"))
        .map_err(|error| error.to_string())
}

/// Arms the registration timer at startup without postponing a restored due time. The due
/// time is persisted, so repeated restarts do not keep moving the first publication.
pub async fn restore_publish_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    shorten_timer(storage, Some(task_handle), PUBLISH_INTERVAL).await;
}

/// Whether this node reports and the settings name a registry.
fn publishes_soon(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    let registry = config.federation.as_ref();
    registry.is_some_and(|settings| settings.registry_url.is_some())
        && is_reporting(config, node_id)
}

/// Publishes soon on the reporting node after replicated realm settings materialize here,
/// also when another management node served the change.
pub async fn publish_soon(context: &DriverContext, realm_id: RealmId, node_id: NodeId) {
    match drive(GetConfigOperation::new(realm_id), context).await {
        Ok(config) if publishes_soon(&config, node_id) => {}
        Ok(_) => return,
        Err(error) => {
            warn!(error = %error, "Realm config unavailable for registry publication");
            return;
        }
    }
    let task_handle = context.task_handle.as_ref();
    shorten_timer(&context.storage_handle, task_handle, PUBLISH_SOON).await;
}

async fn shorten_timer(storage: &StorageHandle, task_handle: Option<&TaskHandle>, after: Duration) {
    let effect = TaskEffect::ShortenTimer {
        key: TaskKey::PublishRegistration,
        after,
    };
    if let Err(message) = persist_task_effect(storage, &effect).await {
        warn!(message = %message, "Failed to persist registry publication timer");
    }
    let Some(task_handle) = task_handle else {
        return;
    };
    if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) =
        task_handle.send_effect(Effect::Task(effect)).await
    {
        warn!(message = %message, "Failed to arm registry publication timer");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            issued_at: 1,
        };
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let (first, second) = lower(node(1), node(2));
        config.ensure_node(first, RealmNodeKind::Management);
        config.ensure_node(second, RealmNodeKind::Management);
        config.ensure_node(node(3), RealmNodeKind::Server);
        config.federation = Some(FederationSettings {
            name: descriptor.name.clone(),
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
        assert_eq!(
            url.unwrap().as_str(),
            format!("https://r.example.org/base/v1/realms/{realm_id}")
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
        // Replicated settings arriving at the reporter persist a due time one minute away.
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
        publish_soon(&context, config.realm_id, lower(node(1), node(2)).1).await;
        assert_eq!(read_timer(storage, &key).await.unwrap(), None);
        publish_soon(&context, config.realm_id, reporter()).await;
        let due = read_timer(storage, &key).await.unwrap().unwrap();
        let limit = (unix_timestamp_secs() + 61) * 1000;
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

    #[tokio::test]
    async fn failed_count_unknown() {
        // A count that fails is reported as `null`; a count that works reports zero as zero.
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
        let realm_id = RealmId::from_bytes([7; 32]);
        let kpis = realm_kpis(&context, realm_id, Some(2)).await;
        assert_eq!(kpis.groups, Some(0));
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::GROUP_KEYSPACE.to_string(),
                key: b"broken".to_vec().into(),
                value: b"not a group".to_vec().into(),
                txn_id: None,
            })
            .await;
        let kpis = realm_kpis(&context, realm_id, Some(2)).await;
        let expected = serde_json::json!({
            "live_datasets": null,
            "groups": null,
            "nodes_configured": 2
        });
        assert_eq!(serde_json::to_value(kpis).unwrap(), expected);
    }
}
