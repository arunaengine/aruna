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
use aruna_core::task::TaskKey;
use aruna_core::time::unix_timestamp_secs;
use aruna_core::types::Effects;
use aruna_tasks::TaskHandle;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use url::Url;

use crate::driver::{DriverContext, drive};
use crate::metadata::stats::{count_realm_documents, count_realm_groups};

/// How often the reporting node renews its registration.
pub const PUBLISH_INTERVAL: Duration = Duration::from_secs(6 * 3600);
/// Withdrawals sent while registration is disabled and the registry URL is kept.
pub const MAX_WITHDRAWALS: u8 = 3;
const STATE_KEY: &[u8] = b"publication";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationState {
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
    match settings.registration {
        RegistrationMode::Enabled => (
            Publication::Register {
                registry_url,
                descriptor: settings.descriptor.clone(),
                nodes_configured: u64::try_from(config.nodes.len()).ok(),
            },
            PublicationState::default(),
        ),
        RegistrationMode::Disabled if state.withdrawals < MAX_WITHDRAWALS => (
            Publication::Withdraw { registry_url },
            PublicationState {
                withdrawals: state.withdrawals + 1,
            },
        ),
        RegistrationMode::Disabled => (Publication::Nothing, state),
    }
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
        let previous = stored
            .map(postcard::from_bytes)
            .transpose()
            .map_err(ConversionError::from)?
            .unwrap_or_default();
        let (publication, next) = decide(&config, self.node_id, previous);
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
    let (method, registry_url, body) = match publication {
        Publication::Nothing => return Ok(()),
        Publication::Register {
            registry_url,
            descriptor,
            nodes_configured,
        } => {
            let kpis = RealmKpis {
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
            };
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
        Publication::Withdraw { registry_url } => {
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

/// The registry route of one realm below the configured registry base URL.
fn realm_url(registry_url: &Url, realm_id: &RealmId) -> Result<Url, String> {
    let mut base = registry_url.clone();
    if !base.path().ends_with('/') {
        base.set_path(&format!("{}/", base.path()));
    }
    base.join(&format!("v1/realms/{realm_id}"))
        .map_err(|error| error.to_string())
}

/// Arms the registration timer at startup without postponing a restored due time.
pub async fn restore_publish_timer(task_handle: &TaskHandle) {
    if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) = task_handle
        .send_effect(Effect::Task(aruna_core::task::TaskEffect::ShortenTimer {
            key: TaskKey::PublishRegistration,
            after: PUBLISH_INTERVAL,
        }))
        .await
    {
        warn!(message = %message, "Failed to arm registry publication timer");
    }
}

