//! Signs the decided registration or withdrawal and sends it to the realm's registry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::federation::{RealmKpis, RealmTotals, Registration, Signed, Withdrawal};
use aruna_core::operation::Operation;
use aruna_core::outbound::{HttpMethod, HttpRequest, OutboundEffect, OutboundEvent};
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use thiserror::Error;
use tracing::warn;
use url::Url;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::federation::publish::{Publication, PublishError, PublishOperation, still_due};
use crate::federation::totals::{DataTotalsError, DataTotalsOperation};
use crate::metadata::realm_counts::{
    RealmCountError, RealmDocumentsOperation, RealmGroupsOperation,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Error, PartialEq)]
pub enum RegistrationError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("{0}")]
    Failed(String),
}

fn failed(error: impl std::fmt::Display) -> RegistrationError {
    RegistrationError::Failed(error.to_string())
}

/// The counts a registration reports, read before it is signed.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct RegistrationCounts {
    pub kpis: RealmKpis,
    /// `None` when a total is unknown.
    pub totals: Option<RealmTotals>,
}

/// The signed bodies of one publication and where the first one goes.
#[derive(Debug, PartialEq)]
struct SignedRequest {
    method: HttpMethod,
    url: Url,
    body: Vec<u8>,
    totals: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
enum SendStep {
    Config,
    Publication,
    Totals,
}

type SendTurn = Result<Next<()>, RegistrationError>;

/// Sends one publication unless the settings changed since it was decided; counting takes time.
/// A refused totals report only warns: a registry from before that route keeps the registration.
#[derive(Debug, PartialEq)]
pub(crate) struct SendRegistrationOperation {
    realm_id: RealmId,
    node_id: NodeId,
    capabilities: NodeCapabilities,
    publication: Publication,
    counts: RegistrationCounts,
    now: u64,
    request: Option<SignedRequest>,
    step: SendStep,
}

impl SendRegistrationOperation {
    pub(crate) fn new(
        (realm_id, node_id): (RealmId, NodeId),
        capabilities: NodeCapabilities,
        publication: Publication,
        counts: RegistrationCounts,
        now: u64,
    ) -> Self {
        Self {
            realm_id,
            node_id,
            capabilities,
            publication,
            counts,
            now,
            request: None,
            step: SendStep::Config,
        }
    }

    fn sign(&self) -> Result<Option<SignedRequest>, RegistrationError> {
        let (method, registry_url, body, totals) = match &self.publication {
            Publication::Nothing => return Ok(None),
            Publication::Register {
                registry_url,
                descriptor,
                nodes_configured,
            } => {
                let registration = Registration {
                    descriptor: descriptor.clone(),
                    kpis: RealmKpis {
                        nodes_configured: *nodes_configured,
                        ..self.counts.kpis
                    },
                    observed_at: self.now,
                    issued_at: self.now,
                };
                let signed = Signed::sign(registration, &self.capabilities).map_err(failed)?;
                let body = serde_json::to_vec(&signed).map_err(failed)?;
                (HttpMethod::Put, registry_url, body, self.signed_totals())
            }
            Publication::Withdraw { registry_url, .. } => {
                let withdrawal = Withdrawal {
                    realm_id: self.realm_id,
                    issued_at: self.now,
                };
                let signed = Signed::sign(withdrawal, &self.capabilities).map_err(failed)?;
                let body = serde_json::to_vec(&signed).map_err(failed)?;
                (HttpMethod::Delete, registry_url, body, None)
            }
        };
        let url = realm_url(registry_url, &self.realm_id).map_err(RegistrationError::Failed)?;
        Ok(Some(SignedRequest {
            method,
            url,
            body,
            totals,
        }))
    }

    fn signed_totals(&self) -> Option<Vec<u8>> {
        let signed = Signed::sign(self.counts.totals?, &self.capabilities)
            .inspect_err(|error| warn!(error = %error, "Signing realm totals failed"))
            .ok()?;
        serde_json::to_vec(&signed).ok()
    }

    fn send_totals(&mut self) -> SendTurn {
        let Some(SignedRequest {
            url,
            totals: Some(body),
            ..
        }) = self.request.take()
        else {
            return Ok(Next::Done(()));
        };
        match totals_url(url) {
            Ok(url) => {
                self.step = SendStep::Totals;
                Ok(Next::Call(json_request(HttpMethod::Put, &url, body)))
            }
            Err(error) => {
                warn!(error = %error, "Registry refused the realm totals");
                Ok(Next::Done(()))
            }
        }
    }
}

/// One signed JSON body as a call through the screened egress client.
fn json_request(method: HttpMethod, url: &Url, body: Vec<u8>) -> IoCall {
    let request = HttpRequest {
        method,
        url: url.to_string(),
        headers: vec![("content-type", "application/json".to_string())],
        body: body.into(),
        deadline: REQUEST_TIMEOUT,
    };
    IoCall::from(Effect::Outbound(OutboundEffect::Request(Box::new(request))))
}

/// Whether the registry accepted a request, or why not.
fn accepted(result: IoResult) -> Result<Result<(), String>, IoError> {
    let Event::Outbound(OutboundEvent::Answered { result }) = result.event()? else {
        let expected = "OutboundEvent::Answered";
        let got = "another outbound event";
        return Err(IoError::Unexpected { expected, got });
    };
    Ok(match result {
        Ok(status) => match reqwest::StatusCode::from_u16(status) {
            Ok(status) if status.is_success() => Ok(()),
            Ok(status) => Err(format!("registry answered {status}")),
            Err(_) => Err(format!("registry answered {status}")),
        },
        Err(error) => Err(error.to_string()),
    })
}

impl Operation for SendRegistrationOperation {
    type Output = ();
    type Error = RegistrationError;

    fn first(&mut self) -> SendTurn {
        self.request = self.sign()?;
        if self.request.is_none() {
            return Ok(Next::Done(()));
        }
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> SendTurn {
        match self.step {
            SendStep::Config => {
                let current = result.child::<RealmConfigDocument, GetConfigError>()?;
                let current = current.map_err(failed)?;
                if !still_due(&current, self.node_id, &self.publication) {
                    return Ok(Next::Done(()));
                }
                let request = self.request.as_mut().ok_or(IoError::Unfinished)?;
                let body = std::mem::take(&mut request.body);
                self.step = SendStep::Publication;
                Ok(Next::Call(json_request(request.method, &request.url, body)))
            }
            SendStep::Publication => {
                accepted(result)?.map_err(RegistrationError::Failed)?;
                self.send_totals()
            }
            SendStep::Totals => {
                if let Err(error) = accepted(result)? {
                    warn!(error = %error, "Registry refused the realm totals");
                }
                Ok(Next::Done(()))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum PublishStep {
    Decide,
    Documents,
    Groups,
    Totals,
    Send,
}

/// A count's value; a count that fails only warns and stays unknown, never zero.
fn known<T, E: std::fmt::Display>(count: Result<T, E>, name: &'static str) -> Option<T> {
    count
        .inspect_err(|error| warn!(error = %error, count = name, "Realm count unavailable"))
        .ok()
}

/// Decides what this node publishes, reads the counts a registration reports and sends it.
/// Totals need the dataset count and the usage sum, so either one unknown omits them.
#[derive(Debug, PartialEq)]
pub(crate) struct PublishRegistrationOperation {
    realm_id: RealmId,
    node_id: NodeId,
    capabilities: NodeCapabilities,
    publication: Publication,
    counts: RegistrationCounts,
    now: u64,
    step: PublishStep,
}

impl PublishRegistrationOperation {
    pub(crate) fn new(realm_id: RealmId, node_id: NodeId, capabilities: NodeCapabilities) -> Self {
        Self {
            realm_id,
            node_id,
            capabilities,
            publication: Publication::Nothing,
            counts: RegistrationCounts::default(),
            now: 0,
            step: PublishStep::Decide,
        }
    }

    fn send(&mut self) -> SendTurn {
        self.step = PublishStep::Send;
        let publication = std::mem::replace(&mut self.publication, Publication::Nothing);
        let send = SendRegistrationOperation::new(
            (self.realm_id, self.node_id),
            self.capabilities.clone(),
            publication,
            self.counts,
            self.now,
        );
        Ok(Next::Call(IoCall::child(send)))
    }
}

impl Operation for PublishRegistrationOperation {
    type Output = ();
    type Error = RegistrationError;

    fn first(&mut self) -> SendTurn {
        let decide = PublishOperation::new(self.realm_id, self.node_id);
        Ok(Next::Call(IoCall::child(decide)))
    }

    fn next(&mut self, result: IoResult) -> SendTurn {
        match self.step {
            PublishStep::Decide => {
                let publication = result.child::<Publication, PublishError>()?;
                self.publication = publication.map_err(failed)?;
                if !matches!(self.publication, Publication::Register { .. }) {
                    return self.send();
                }
                self.step = PublishStep::Documents;
                let documents = RealmDocumentsOperation::new(self.realm_id);
                Ok(Next::Call(IoCall::child(documents)))
            }
            PublishStep::Documents => {
                let documents = result.child::<Option<u64>, RealmCountError>()?;
                self.counts.kpis.live_datasets = known(documents, "datasets").flatten();
                self.step = PublishStep::Groups;
                let groups = RealmGroupsOperation::new(self.realm_id);
                Ok(Next::Call(IoCall::child(groups)))
            }
            PublishStep::Groups => {
                let groups = result.child::<u64, RealmCountError>()?;
                self.counts.kpis.groups = known(groups, "groups");
                if self.counts.kpis.live_datasets.is_none() {
                    return self.send();
                }
                self.step = PublishStep::Totals;
                let totals = DataTotalsOperation::new(self.node_id, Some(self.realm_id));
                Ok(Next::Call(IoCall::child(totals)))
            }
            PublishStep::Totals => {
                let totals = result.child::<(u64, u64), DataTotalsError>()?;
                let datasets = self.counts.kpis.live_datasets;
                if let (Some((objects, data_bytes)), Some(datasets)) =
                    (known(totals, "totals"), datasets)
                {
                    self.counts.totals = Some(RealmTotals {
                        realm_id: self.realm_id,
                        data_bytes,
                        objects,
                        datasets,
                        observed_at: self.now,
                    });
                }
                self.send()
            }
            PublishStep::Send => {
                result.child::<(), RegistrationError>()??;
                Ok(Next::Done(()))
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now = now_ms / 1_000;
    }
}

/// The totals route below a realm's registry route.
fn totals_url(mut realm_url: Url) -> Result<Url, String> {
    realm_url
        .path_segments_mut()
        .map_err(|()| "registry URL cannot take a path".to_string())?
        .push("totals");
    Ok(realm_url)
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

#[cfg(test)]
mod tests {
    use aruna_core::calls::ChildOutcome;
    use aruna_core::federation::{
        AcceptedRealms, FederationSettings, RealmDescriptor, RegistrationMode,
    };
    use aruna_core::outbound::OutboundError;
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use ed25519_dalek::SigningKey;

    use super::*;

    const NOW: u64 = 50;

    fn capabilities() -> NodeCapabilities {
        NodeCapabilities::management_node(SigningKey::from_bytes(&[4; 32])).unwrap()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes(SigningKey::from_bytes(&[4; 32]).verifying_key().to_bytes())
    }

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn url(value: &str) -> Url {
        Url::parse(value).unwrap()
    }

    fn descriptor() -> Signed<RealmDescriptor> {
        let descriptor = RealmDescriptor {
            realm_id: realm(),
            name: "Realm".to_string(),
            description: String::new(),
            api_url: url("https://api.example.org"),
            portal_url: url("https://portal.example.org"),
            contact: None,
            login_providers: Vec::new(),
            issued_at: 1,
        };
        Signed::sign(descriptor, &capabilities()).unwrap()
    }

    fn config(registration: RegistrationMode) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
        config.ensure_node(node(), RealmNodeKind::Management);
        config.federation = Some(FederationSettings {
            name: "Realm".to_string(),
            description: String::new(),
            contact: None,
            login_providers: Vec::new(),
            api_url: url("https://api.example.org"),
            portal_url: url("https://portal.example.org"),
            registry_url: Some(url("https://registry.example.org")),
            registration,
            accepted_realms: AcceptedRealms::None,
            descriptor: descriptor(),
        });
        config
    }

    fn register() -> Publication {
        Publication::Register {
            registry_url: url("https://registry.example.org"),
            descriptor: descriptor(),
            nodes_configured: Some(1),
        }
    }

    fn operation(publication: Publication, totals: bool) -> SendRegistrationOperation {
        let counts = RegistrationCounts {
            kpis: RealmKpis::default(),
            totals: totals.then_some(RealmTotals {
                realm_id: realm(),
                data_bytes: 1,
                objects: 1,
                datasets: 1,
                observed_at: NOW,
            }),
        };
        let key = (realm(), node());
        SendRegistrationOperation::new(key, capabilities(), publication, counts, NOW)
    }

    fn current(registration: RegistrationMode) -> IoResult {
        let config: Result<_, GetConfigError> = Ok(config(registration));
        IoResult::Child(ChildOutcome::new(config))
    }

    fn answered(result: Result<u16, OutboundError>) -> IoResult {
        let event = Event::Outbound(OutboundEvent::Answered { result });
        IoResult::Event(Box::new(event))
    }

    fn sent(turn: SendTurn) -> HttpRequest {
        let Ok(Next::Call(IoCall::Effect(effect))) = turn else {
            panic!("expected a request, got {turn:?}");
        };
        let Effect::Outbound(OutboundEffect::Request(request)) = *effect else {
            panic!("expected an outbound request");
        };
        *request
    }

    #[test]
    fn realm_url_joins() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let joined = realm_url(&url("https://r.example.org/base"), &realm_id).unwrap();
        let expected = format!("https://r.example.org/base/v1/realms/{realm_id}");
        assert_eq!(joined.as_str(), expected);
        let totals = totals_url(joined).unwrap();
        assert_eq!(totals.as_str(), format!("{expected}/totals"));
    }

    #[test]
    fn registers_then_totals() {
        let mut send = operation(register(), true);
        let read = IoCall::child(GetConfigOperation::new(realm()));
        assert_eq!(send.first(), Ok(Next::Call(read)));
        let request = sent(send.next(current(RegistrationMode::Enabled)));
        let route = format!("https://registry.example.org/v1/realms/{}", realm());
        assert_eq!(
            (request.method, request.url.as_str()),
            (HttpMethod::Put, route.as_str())
        );
        let totals = sent(send.next(answered(Ok(200))));
        assert_eq!(totals.url, format!("{route}/totals"));
        // A refused totals report keeps the registration.
        assert_eq!(send.next(answered(Ok(404))), Ok(Next::Done(())));
    }

    #[test]
    fn changed_settings_stop() {
        let mut send = operation(register(), false);
        send.first().unwrap();
        let answer = send.next(current(RegistrationMode::Disabled));
        assert_eq!(answer, Ok(Next::Done(())));
    }

    #[test]
    fn refused_publication_fails() {
        let mut send = operation(register(), true);
        send.first().unwrap();
        send.next(current(RegistrationMode::Enabled)).unwrap();
        let answer = send.next(answered(Ok(403)));
        let expected = "registry answered 403 Forbidden".to_string();
        assert_eq!(answer, Err(RegistrationError::Failed(expected)));
        let mut unreachable = operation(register(), false);
        unreachable.first().unwrap();
        unreachable
            .next(current(RegistrationMode::Enabled))
            .unwrap();
        let down = OutboundError::Retryable("down".to_string());
        assert!(unreachable.next(answered(Err(down))).is_err());
    }

    #[test]
    fn withdrawal_sends_delete() {
        let withdraw = Publication::Withdraw {
            registry_url: url("https://registry.example.org"),
            issued_at: 1,
        };
        let mut send = operation(withdraw, true);
        send.first().unwrap();
        let request = sent(send.next(current(RegistrationMode::Disabled)));
        assert_eq!(request.method, HttpMethod::Delete);
        assert_eq!(send.next(answered(Ok(204))), Ok(Next::Done(())));
    }

    #[test]
    fn nothing_sends_nothing() {
        let mut send = operation(Publication::Nothing, false);
        assert_eq!(send.first(), Ok(Next::Done(())));
    }

    fn child<T, E>(outcome: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(outcome))
    }

    fn publisher() -> PublishRegistrationOperation {
        let mut publish = PublishRegistrationOperation::new(realm(), node(), capabilities());
        publish.stamp(NOW * 1_000);
        publish
    }

    /// A publisher that has decided to register and is waiting for the document count.
    fn deciding() -> PublishRegistrationOperation {
        let mut publish = publisher();
        publish.first().unwrap();
        publish
            .next(child::<_, PublishError>(Ok(register())))
            .unwrap();
        publish
    }

    fn sends(publication: Publication, counts: RegistrationCounts) -> SendTurn {
        let key = (realm(), node());
        let send = SendRegistrationOperation::new(key, capabilities(), publication, counts, NOW);
        Ok(Next::Call(IoCall::child(send)))
    }

    fn kpis(live_datasets: Option<u64>, groups: Option<u64>) -> RealmKpis {
        RealmKpis {
            live_datasets,
            groups,
            nodes_configured: None,
        }
    }

    #[test]
    fn registers_with_counts() {
        let mut publish = publisher();
        let decide = IoCall::child(PublishOperation::new(realm(), node()));
        assert_eq!(publish.first(), Ok(Next::Call(decide)));
        let documents = IoCall::child(RealmDocumentsOperation::new(realm()));
        let answer = publish.next(child::<_, PublishError>(Ok(register())));
        assert_eq!(answer, Ok(Next::Call(documents)));
        let groups = IoCall::child(RealmGroupsOperation::new(realm()));
        let answer = publish.next(child::<_, RealmCountError>(Ok(Some(4_u64))));
        assert_eq!(answer, Ok(Next::Call(groups)));
        let totals = IoCall::child(DataTotalsOperation::new(node(), Some(realm())));
        let answer = publish.next(child::<_, RealmCountError>(Ok(2_u64)));
        assert_eq!(answer, Ok(Next::Call(totals)));
        let counts = RegistrationCounts {
            kpis: kpis(Some(4), Some(2)),
            totals: Some(RealmTotals {
                realm_id: realm(),
                data_bytes: 9,
                objects: 5,
                datasets: 4,
                observed_at: NOW,
            }),
        };
        let answer = publish.next(child::<_, DataTotalsError>(Ok((5_u64, 9_u64))));
        assert_eq!(answer, sends(register(), counts));
        let answer = publish.next(child::<(), RegistrationError>(Ok(())));
        assert_eq!(answer, Ok(Next::Done(())));
    }

    #[test]
    fn failed_count_unknown() {
        // A count that fails is reported as unknown and the totals are left out.
        let mut publish = deciding();
        let failed = RealmCountError::Io(IoError::Aborted);
        publish.next(child::<Option<u64>, _>(Err(failed))).unwrap();
        let answer = publish.next(child::<_, RealmCountError>(Ok(0_u64)));
        let counts = RegistrationCounts {
            kpis: kpis(None, Some(0)),
            totals: None,
        };
        assert_eq!(answer, sends(register(), counts));
    }

    #[test]
    fn missing_metadata_unknown() {
        // Without a metadata subsystem the dataset count is unknown, never zero.
        let mut publish = deciding();
        publish
            .next(child::<Option<u64>, RealmCountError>(Ok(None)))
            .unwrap();
        let failed = RealmCountError::Io(IoError::Aborted);
        let answer = publish.next(child::<u64, _>(Err(failed)));
        let counts = RegistrationCounts::default();
        assert_eq!(answer, sends(register(), counts));
    }

    #[test]
    fn failed_totals_send() {
        let mut publish = deciding();
        publish
            .next(child::<_, RealmCountError>(Ok(Some(4_u64))))
            .unwrap();
        publish
            .next(child::<_, RealmCountError>(Ok(2_u64)))
            .unwrap();
        let answer = publish.next(child::<(u64, u64), _>(Err(DataTotalsError::Missing)));
        let counts = RegistrationCounts {
            kpis: kpis(Some(4), Some(2)),
            totals: None,
        };
        assert_eq!(answer, sends(register(), counts));
    }

    #[test]
    fn withdrawal_skips_counts() {
        let withdraw = Publication::Withdraw {
            registry_url: url("https://registry.example.org"),
            issued_at: 1,
        };
        let mut publish = publisher();
        publish.first().unwrap();
        let answer = publish.next(child::<_, PublishError>(Ok(withdraw.clone())));
        assert_eq!(answer, sends(withdraw, RegistrationCounts::default()));
    }

    #[test]
    fn failed_decision_stops() {
        let mut publish = publisher();
        publish.first().unwrap();
        let answer = publish.next(child::<Publication, _>(Err(PublishError::ConfigMissing)));
        let expected = PublishError::ConfigMissing.to_string();
        assert_eq!(answer, Err(RegistrationError::Failed(expected)));
    }

    #[test]
    fn refused_send_fails() {
        let mut publish = publisher();
        publish.first().unwrap();
        publish
            .next(child::<_, PublishError>(Ok(Publication::Nothing)))
            .unwrap();
        let refused = RegistrationError::Failed("registry answered 403".to_string());
        let answer = publish.next(child::<(), _>(Err(refused)));
        let expected = "registry answered 403".to_string();
        assert_eq!(answer, Err(RegistrationError::Failed(expected)));
    }
}
