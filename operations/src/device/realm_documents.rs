//! Keeps a device's copy of the realm-wide documents, fetched by routed reads from peers.
//! Installs never regress, and a marker every peer disagrees with is re-based.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::rows::{outside_answer, outside_call};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::str::FromStr;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::admin_documents::AdminDocumentClock;
use aruna_core::auth::revocation_live;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_DELETE_KEYSPACE, GROUP_KEYSPACE, MANAGEMENT_URL_KEYSPACE,
    NODE_NAME_KEYSPACE, NODE_PLACE_KEYSPACE, REALM_MARKER_KEYSPACE, REALM_REMOVAL_KEYSPACE,
};
use aruna_core::metadata::AuthToken;
use aruna_core::node_state::{InstallInfoOperation, NodeInfoError};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::group_delete::{GroupDeletePhase, GroupDeleteRecord};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::time::unix_timestamp_secs;
use aruna_core::types::{Key, KeySpace, Value};
use aruna_net::streams::{ADMISSION_CLOSE, MEMBERSHIP_CLOSE};
use rand::seq::SliceRandom;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::document_repository::{config_of, realm_config};
use aruna_core::structs::identity::realm::NodeRealm;
use crate::forward::transport::{peer_call_within, timed_answer};
use crate::driver::DriverContext;
use crate::metadata::protocol::{
    DeviceGroupDocuments, MAX_DEVICE_GROUPS, MetadataTransportMessage, RealmDocuments,
};
use crate::metadata::transport_message_kind;
use crate::realm::mutate_placement::node_kind;
use crate::realm::node_name::NodeDisplays;

/// Attempts in which every answering peer served a copy the marker does not
/// cover before the marker is treated as wrong. The realm agreeing against a
/// too-high marker is evidence a peer lied.
const REBASE_ATTEMPTS: u32 = 3;

/// Fetches every realm node refused for realm membership, with no successful fetch between
/// them, before the device reports itself removed.
const REMOVAL_REFUSALS: u32 = 2;

/// How many cached groups one reconciliation looks at. Above
/// [`MAX_DEVICE_GROUPS`] so a group installed outside a fetch is still seen.
const GROUP_SCAN_LIMIT: usize = MAX_DEVICE_GROUPS + 8;

/// What the device accepted last, and how often the realm has disagreed with it
/// since. Device-local: it is the memory that keeps a copy from going backwards.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct RealmMarker {
    clock: AdminDocumentClock,
    behind: u32,
}

/// One fetch attempt: who this device is, whom it may ask, and what it holds.
#[derive(Clone, Debug, PartialEq)]
struct FetchPlan {
    realm_id: RealmId,
    node_id: NodeId,
    owner: UserId,
    auth: AuthContext,
    peers: Vec<NodeId>,
    marker: RealmMarker,
}

impl FetchPlan {
    fn new((node_id, realm_id): (NodeId, RealmId), owner: UserId, peers: Vec<NodeId>) -> Self {
        Self {
            realm_id,
            node_id,
            owner,
            auth: AuthContext {
                user_id: owner,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            peers,
            marker: RealmMarker::default(),
        }
    }
}

/// One answer worth keeping, with the configuration it carries decoded once.
#[derive(Clone, Debug, PartialEq)]
struct Accepted {
    documents: RealmDocuments,
    config: RealmConfigDocument,
    /// The answer's clock, counting only origins its own configuration names.
    clock: AdminDocumentClock,
}

/// The best of what the peers answered: one copy that covers the marker, or a
/// copy two peers agreed on below it.
#[derive(Debug, Default, PartialEq)]
struct Selection {
    current: Option<Accepted>,
    behind: Option<Accepted>,
    candidates: Vec<Accepted>,
    /// Peers that closed the connection because their realm no longer admits this device.
    membership_refusals: usize,
}

impl Selection {
    /// Offers one answer, and says whether the search may stop: it may as soon
    /// as a copy covers the marker.
    fn offer(&mut self, accepted: Accepted, installed: &AdminDocumentClock) -> bool {
        let origins = realm_origins(&accepted.config);
        if covers(&accepted.clock, installed, &origins) {
            self.current = Some(accepted);
            return true;
        }
        let agreed = self.candidates.iter().any(|candidate| {
            candidate.clock == accepted.clock && candidate.documents == accepted.documents
        });
        if agreed {
            let further = self
                .behind
                .as_ref()
                .is_none_or(|best| covers(&accepted.clock, &best.clock, &origins));
            if further {
                self.behind = Some(accepted);
            }
        } else {
            self.candidates.push(accepted);
        }
        false
    }
}

/// Whether a request error is a peer closing the connection because its realm does not admit
/// this device: a new connection is refused at admission, an open one on a membership change.
fn membership_refused(error: &str) -> bool {
    [ADMISSION_CLOSE, MEMBERSHIP_CLOSE]
        .iter()
        .any(|reason| error.contains(&format!("closed by peer: {reason}")))
}

/// Whether an attempt that found only copies below the marker may re-base it,
/// and what the attempt count becomes.
fn rebase_after(behind: u32) -> (bool, u32) {
    let attempts = behind.saturating_add(1);
    match attempts >= REBASE_ATTEMPTS {
        true => (true, 0),
        false => (false, attempts),
    }
}

fn outside_read(key_space: &str, key: Vec<u8>) -> IoCall {
    outside_call(read_row(key_space, key))
}

/// The row an outside read found; an absent, unreadable row is none.
fn outside_row(result: IoResult) -> Option<Value> {
    outside_answer(result)
        .and_then(|read| read.value())
        .ok()
        .flatten()
}

fn realm_key(realm_id: RealmId) -> Vec<u8> {
    realm_id.as_bytes().to_vec()
}

/// The refusal count a stored row holds.
fn refusal_count(row: Option<Value>) -> u32 {
    row.and_then(|bytes| <[u8; 4]>::try_from(bytes.as_ref()).ok())
        .map_or(0, u32::from_be_bytes)
}

/// Whether the realm has removed this device, as repeated membership refusals show.
#[derive(Debug, PartialEq)]
pub struct RealmRemovedOperation {
    realm_id: RealmId,
}

/// The check for a realm removal; see [`RealmRemovedOperation`].
pub fn realm_removed(realm_id: RealmId) -> RealmRemovedOperation {
    RealmRemovedOperation { realm_id }
}

impl Operation for RealmRemovedOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let read = outside_read(REALM_REMOVAL_KEYSPACE, realm_key(self.realm_id));
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        Ok(Next::Done(
            refusal_count(outside_row(result)) >= REMOVAL_REFUSALS,
        ))
    }
}

/// Counts fetches that every realm peer refused for realm membership; a fetch that installed
/// the documents clears the count. A restarting peer refuses once, so one count proves nothing.
#[derive(Debug, PartialEq)]
struct NoteMembershipOperation {
    realm_id: RealmId,
    refused: bool,
    installed: bool,
    writing: bool,
}

impl NoteMembershipOperation {
    fn new(realm_id: RealmId, refused: bool, installed: bool) -> Self {
        Self {
            realm_id,
            refused,
            installed,
            writing: false,
        }
    }
}

impl Operation for NoteMembershipOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let key = realm_key(self.realm_id);
        self.writing = self.installed;
        Ok(match (self.installed, self.refused) {
            (true, _) => {
                let row = (REALM_REMOVAL_KEYSPACE.to_string(), key.into());
                Next::Call(IoCall::Delete(vec![row]))
            }
            (false, true) => Next::Call(outside_read(REALM_REMOVAL_KEYSPACE, key)),
            (false, false) => Next::Done(()),
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.writing {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let refusals = refusal_count(outside_row(result)).saturating_add(1);
        if refusals == REMOVAL_REFUSALS {
            warn!("Every realm node refused this device's realm membership");
        }
        self.writing = true;
        let row = (
            REALM_REMOVAL_KEYSPACE.to_string(),
            Key::from(realm_key(self.realm_id)),
            Value::from(refusals.to_be_bytes().to_vec()),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

/// Where a fetch finds the owner and the peers it asks.
#[derive(Clone, Debug, PartialEq)]
enum FetchSource {
    /// The stored realm configuration.
    Stored,
    /// Supplied by a device that holds no configuration yet.
    Peers { owner: UserId, peers: Vec<NodeId> },
}

/// What the pending call of a document fetch answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FetchStep {
    Config,
    Marker,
    Peer,
    Install,
    MarkBehind,
    Membership,
}

/// Fetches the realm-wide documents and installs them on this device. The budget bounds only
/// the questions: a silent realm costs that much once and the stored copy keeps serving, and
/// half an install would leave documents of two ages.
#[derive(Debug, PartialEq)]
pub struct FetchDocumentsOperation {
    ids: NodeRealm,
    source: FetchSource,
    budget: Duration,
    step: FetchStep,
    plan: Option<FetchPlan>,
    peers: VecDeque<NodeId>,
    asked: Option<NodeId>,
    selection: Selection,
    installed: bool,
}

type Fetching = Result<Next<bool>, IoError>;

/// The fetch of a device that holds its realm configuration.
pub fn fetch_realm_documents(context: &DriverContext, budget: Duration) -> FetchDocumentsOperation {
    FetchDocumentsOperation::new(context.node_and_realm(), FetchSource::Stored, budget)
}

/// The same fetch for a device that holds no realm configuration yet: owner and peers are
/// supplied instead of read from a stored copy. This is the onboarding path, where the realm
/// documents are what the device is missing.
pub fn fetch_from_peers(
    context: &DriverContext,
    owner: UserId,
    peers: Vec<NodeId>,
    budget: Duration,
) -> FetchDocumentsOperation {
    let source = FetchSource::Peers { owner, peers };
    FetchDocumentsOperation::new(context.node_and_realm(), source, budget)
}

impl FetchDocumentsOperation {
    fn new(ids: NodeRealm, source: FetchSource, budget: Duration) -> Self {
        Self {
            ids,
            source,
            budget,
            step: FetchStep::Config,
            plan: None,
            peers: VecDeque::new(),
            asked: None,
            selection: Selection::default(),
            installed: false,
        }
    }

    fn plan(&self) -> Result<&FetchPlan, IoError> {
        self.plan.as_ref().ok_or(IoError::Unfinished)
    }

    fn start(&mut self, owner: UserId, peers: Vec<NodeId>) -> Fetching {
        let ids = self.ids.ok_or(IoError::Unfinished)?;
        let plan = FetchPlan::new(ids, owner, peers);
        let read = outside_read(REALM_MARKER_KEYSPACE, realm_key(plan.realm_id));
        self.plan = Some(plan);
        self.step = FetchStep::Marker;
        Ok(Next::Call(read))
    }

    /// A different node answers first every time, so one lagging peer never owns this device's
    /// view of the realm.
    fn configured(&mut self, config: Option<RealmConfigDocument>) -> Fetching {
        let (Some(config), Some((node_id, _))) = (config, self.ids) else {
            return Ok(Next::Done(false));
        };
        let Some(owner) = node_kind(&config, node_id).and_then(|kind| kind.owner()) else {
            return Ok(Next::Done(false));
        };
        let mut peers = realm_peers(&config, node_id);
        peers.shuffle(&mut rand::rng());
        self.start(owner, peers)
    }

    /// Asks the realm's nodes in turn, stopping at the first copy that covers the marker.
    fn ask_next(&mut self) -> Fetching {
        let auth = self.plan()?.auth.clone();
        while !self.budget.is_zero() {
            let Some(peer) = self.peers.pop_front() else {
                return self.asked_all();
            };
            let message = MetadataTransportMessage::FetchRealmDocuments {
                auth_token: AuthToken::internal(auth.clone()),
            };
            match peer_call_within(peer, &message, self.budget) {
                Ok(call) => {
                    self.asked = Some(peer);
                    self.step = FetchStep::Peer;
                    return Ok(Next::Call(call));
                }
                Err(error) => {
                    debug!(%peer, %error, "Could not reach a realm node for the documents");
                }
            }
        }
        // The budget ran out: this attempt says nothing about the realm.
        self.finish(false, false)
    }

    fn answered(&mut self, result: IoResult) -> Fetching {
        let peer = self.asked.take().ok_or(IoError::Unfinished)?;
        let (answer, elapsed) = timed_answer(result)?;
        self.budget = self.budget.saturating_sub(elapsed);
        let (realm_id, clock) = {
            let plan = self.plan()?;
            (plan.realm_id, plan.marker.clock.clone())
        };
        match answer {
            Ok(MetadataTransportMessage::FetchedRealmDocuments {
                result: Ok(documents),
            }) => {
                let Some(accepted) = accept(documents, realm_id) else {
                    warn!(%peer, "A realm node served a configuration this device refuses");
                    return self.ask_next();
                };
                if self.selection.offer(accepted, &clock) {
                    return self.asked_all();
                }
                debug!(%peer, "A realm node offered an older realm configuration");
            }
            Ok(MetadataTransportMessage::FetchedRealmDocuments {
                result: Err(refusal),
            }) => {
                debug!(%peer, ?refusal, "A realm node refused the document fetch");
                // An authorization verdict is the same at every peer.
                if matches!(refusal, SyncRefusal::Unauthorized | SyncRefusal::Forbidden) {
                    self.selection = Selection::default();
                    return self.asked_all();
                }
            }
            Ok(other) => {
                let message = transport_message_kind(&other);
                debug!(%peer, %message, "Unexpected answer to a document fetch");
            }
            Err(error) => {
                if membership_refused(&error) {
                    self.selection.membership_refusals += 1;
                }
                debug!(%peer, %error, "Could not reach a realm node for the documents");
            }
        }
        self.ask_next()
    }

    /// Installs the best copy found, or counts an attempt that found only older ones.
    fn asked_all(&mut self) -> Fetching {
        let selection = std::mem::take(&mut self.selection);
        let plan = self.plan()?.clone();
        let refused = !plan.peers.is_empty() && selection.membership_refusals == plan.peers.len();
        self.selection.membership_refusals = usize::from(refused);
        let accepted = match (selection.current, selection.behind) {
            (Some(accepted), _) => accepted,
            (None, Some(behind)) => {
                let (rebase, attempts) = rebase_after(plan.marker.behind);
                if !rebase {
                    debug!(
                        attempts,
                        "Every realm node answered below this device's marker"
                    );
                    let marker = RealmMarker {
                        clock: plan.marker.clock.clone(),
                        behind: attempts,
                    };
                    self.step = FetchStep::MarkBehind;
                    return Ok(Next::Call(marker_write(plan.realm_id, &marker)?));
                }
                warn!("Re-basing this device's realm marker on what its realm answers");
                behind
            }
            (None, None) => return self.finish(false, refused),
        };
        self.step = FetchStep::Install;
        let install = InstallDocumentsOperation::new(plan, accepted, 0);
        Ok(Next::Call(IoCall::child(install)))
    }

    /// A fetch from the stored configuration keeps count of the realm's refusals.
    fn finish(&mut self, installed: bool, refused: bool) -> Fetching {
        if self.source != FetchSource::Stored {
            return Ok(Next::Done(installed));
        }
        let note = NoteMembershipOperation::new(self.plan()?.realm_id, refused, installed);
        self.installed = installed;
        self.step = FetchStep::Membership;
        Ok(Next::Call(IoCall::child(note)))
    }

    fn was_refused(&self) -> bool {
        self.selection.membership_refusals > 0
    }
}

fn marker_write(realm_id: RealmId, marker: &RealmMarker) -> Result<IoCall, IoError> {
    let bytes = postcard::to_allocvec(marker).map_err(|error| IoError::Codec(error.to_string()))?;
    Ok(IoCall::Write(vec![marker_row(realm_id, bytes)]))
}

fn marker_row(realm_id: RealmId, bytes: Vec<u8>) -> (KeySpace, Key, Value) {
    (
        REALM_MARKER_KEYSPACE.to_string(),
        Key::from(realm_key(realm_id)),
        Value::from(bytes),
    )
}

/// The marker a stored row holds. An absent one reads as nothing seen, which accepts the first
/// copy and refuses nothing.
fn marker_of(row: Option<Value>) -> RealmMarker {
    row.and_then(|bytes| postcard::from_bytes(&bytes).ok())
        .unwrap_or_default()
}

impl Operation for FetchDocumentsOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Fetching {
        let Some((_, realm_id)) = self.ids else {
            return Ok(Next::Done(false));
        };
        match self.source.clone() {
            FetchSource::Stored => Ok(Next::Call(realm_config(realm_id))),
            FetchSource::Peers { owner, peers } => self.start(owner, peers),
        }
    }

    fn next(&mut self, result: IoResult) -> Fetching {
        match self.step {
            FetchStep::Config => self.configured(config_of(result)?),
            FetchStep::Marker => {
                let marker = marker_of(outside_row(result));
                let plan = self.plan.as_mut().ok_or(IoError::Unfinished)?;
                plan.marker = marker;
                self.peers = plan.peers.clone().into();
                self.ask_next()
            }
            FetchStep::Peer => self.answered(result),
            FetchStep::Install => {
                let installed = result.child::<bool, IoError>()??;
                let refused = self.was_refused();
                self.finish(installed, refused)
            }
            FetchStep::MarkBehind => {
                result.done().ok();
                let refused = self.was_refused();
                self.finish(false, refused)
            }
            FetchStep::Membership => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Failed to record the realm membership state");
                }
                Ok(Next::Done(self.installed))
            }
        }
    }
}

/// Decodes one answer and keeps it only if it is this realm's configuration.
/// The clock it carries is trimmed to the nodes that configuration names, so a
/// peer cannot inflate the marker with origins the realm does not have.
fn accept(mut documents: RealmDocuments, realm_id: RealmId) -> Option<Accepted> {
    let config = RealmConfigDocument::from_bytes(&documents.realm_config).ok()?;
    if config.realm_id != realm_id {
        return None;
    }
    documents.groups.truncate(MAX_DEVICE_GROUPS);
    let clock = trim_clock(&documents.clock, &realm_origins(&config));
    Some(Accepted {
        documents,
        config,
        clock,
    })
}

/// Whether `offered` has seen everything `installed` has, counting the realm's
/// own nodes alone. An origin the realm does not name is one a peer invented,
/// and it never keeps a legitimate copy out.
fn covers(
    offered: &AdminDocumentClock,
    installed: &AdminDocumentClock,
    origins: &BTreeSet<NodeId>,
) -> bool {
    installed
        .origins
        .iter()
        .filter(|(origin, _)| origins.contains(*origin))
        .all(|(origin, seq)| offered.sequence_for(origin) >= *seq)
}

/// The same clock without the origins the realm does not name.
fn trim_clock(clock: &AdminDocumentClock, origins: &BTreeSet<NodeId>) -> AdminDocumentClock {
    AdminDocumentClock {
        origins: clock
            .origins
            .iter()
            .filter(|(origin, _)| origins.contains(*origin))
            .map(|(origin, seq)| (*origin, *seq))
            .collect(),
    }
}

/// The nodes that may author the realm's documents, as one configuration names
/// them.
fn realm_origins(config: &RealmConfigDocument) -> BTreeSet<NodeId> {
    config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .collect()
}

/// The realm's own nodes, in configuration order. A device asks infrastructure
/// only: another device holds nothing it could serve.
fn realm_peers(config: &RealmConfigDocument, node_id: NodeId) -> Vec<NodeId> {
    config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .filter(|peer| *peer != node_id)
        .collect()
}

/// One document's place in storage.
fn document_read(target: &DocumentTarget) -> (KeySpace, Key) {
    (target.storage_keyspace().to_string(), target.storage_key())
}

fn document_row(target: &DocumentTarget, bytes: Vec<u8>) -> (KeySpace, Key, Value) {
    (
        target.storage_keyspace().to_string(),
        target.storage_key(),
        Value::from(bytes),
    )
}

fn decoded_or_default<T: DeserializeOwned + Default>(row: Option<&Value>) -> T {
    row.and_then(|bytes| postcard::from_bytes(bytes).ok())
        .unwrap_or_default()
}

fn realm_row(key_space: &str, realm_id: RealmId) -> (KeySpace, Key) {
    (key_space.to_string(), Key::from(realm_key(realm_id)))
}

/// What the pending call of an install answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InstallStep {
    Stored,
    Groups,
    NodeInfos,
    Marker,
    Documents,
    Peers,
}

/// The stored copies an install compares the fetched ones with.
#[derive(Debug, Default, PartialEq)]
struct StoredCopies {
    config: Option<Value>,
    authorization: Option<Value>,
    owner: Option<Value>,
    urls: Vec<String>,
    displays: NodeDisplays,
}

/// Writes the fetched copies where every local read already looks for them, in one batch with
/// the marker they were accepted at: a half-installed copy would leave the device judging itself
/// by two ages of the realm at once.
#[derive(Debug, PartialEq)]
struct InstallDocumentsOperation {
    plan: FetchPlan,
    accepted: Accepted,
    behind: u32,
    step: InstallStep,
    stored: StoredCopies,
    groups: Vec<DeviceGroupDocuments>,
    config: Option<RealmConfigDocument>,
}

type Installing = Result<Next<bool>, IoError>;

impl InstallDocumentsOperation {
    fn new(plan: FetchPlan, accepted: Accepted, behind: u32) -> Self {
        Self {
            plan,
            accepted,
            behind,
            step: InstallStep::Stored,
            stored: StoredCopies::default(),
            groups: Vec::new(),
            config: None,
        }
    }

    fn realm(&self) -> DocumentTarget {
        DocumentTarget::RealmConfig {
            realm_id: self.plan.realm_id,
        }
    }

    fn authorization(&self) -> DocumentTarget {
        DocumentTarget::RealmAuthorization {
            realm_id: self.plan.realm_id,
        }
    }

    fn owner(&self) -> DocumentTarget {
        DocumentTarget::User {
            user_id: self.plan.owner,
        }
    }

    fn actor(&self) -> Actor {
        Actor {
            node_id: self.plan.node_id,
            user_id: self.plan.owner,
            realm_id: self.plan.realm_id,
        }
    }

    fn stored_read(&self) -> IoCall {
        let realm_id = self.plan.realm_id;
        let reads = vec![
            document_read(&self.realm()),
            document_read(&self.authorization()),
            document_read(&self.owner()),
            realm_row(MANAGEMENT_URL_KEYSPACE, realm_id),
            realm_row(NODE_NAME_KEYSPACE, realm_id),
            realm_row(NODE_PLACE_KEYSPACE, realm_id),
        ];
        outside_call(IoCall::Read(reads))
    }

    fn keep_stored(&mut self, result: IoResult) {
        let rows = outside_answer(result).and_then(|read| read.values());
        let Ok(rows) = rows else {
            warn!("Failed to read the installed realm documents");
            return;
        };
        let row = |index: usize| rows.get(index).cloned().flatten();
        self.stored = StoredCopies {
            config: row(0),
            authorization: row(1),
            owner: row(2),
            urls: decoded_or_default(row(3).as_ref()),
            displays: NodeDisplays {
                names: decoded_or_default::<BTreeMap<_, _>>(row(4).as_ref()),
                places: decoded_or_default::<BTreeMap<_, _>>(row(5).as_ref()),
            },
        };
    }

    /// Compares the answer with what is stored, then writes what changed.
    fn compare(&mut self) -> Installing {
        let mut config = self.accepted.config.clone();
        let installed = (self.stored.config.as_deref())
            .and_then(|bytes| RealmConfigDocument::from_bytes(bytes).ok());
        if let Some(installed) = installed {
            keep_revocations(&mut config, &installed, unix_timestamp_secs());
        }
        let Ok(bytes) = config.to_bytes(&self.actor()) else {
            warn!("A fetched realm configuration could not be stored");
            return Ok(Next::Done(false));
        };
        let marker = RealmMarker {
            clock: self.accepted.clock.clone(),
            behind: self.behind,
        };
        let Ok(marker_bytes) = postcard::to_allocvec(&marker) else {
            return Ok(Next::Done(false));
        };
        let documents = &self.accepted.documents;
        let stored = &self.stored;
        let unchanged = stored.config.as_deref() == Some(bytes.as_slice())
            && stored.authorization.as_deref() == documents.realm_authorization.as_deref()
            && stored.owner.as_deref() == documents.owner.as_deref()
            && self.groups == documents.groups
            && stored.urls == documents.management_urls
            && stored.displays.names == documents.node_names
            && stored.displays.places == documents.node_places;
        let marker_row = marker_row(self.plan.realm_id, marker_bytes);
        if unchanged {
            // Nothing but the marker moves: writing the documents again would re-register
            // every realm peer on every beat for a copy this device already holds.
            if marker == self.plan.marker {
                return Ok(Next::Done(true));
            }
            debug!("This device already holds the realm documents it fetched");
            self.step = InstallStep::Marker;
            return Ok(Next::Call(IoCall::child(WriteDocumentsOperation::new(
                vec![marker_row],
            ))));
        }
        let Some(writes) = self.document_writes(bytes, marker_row) else {
            return Ok(Next::Done(false));
        };
        self.config = Some(config);
        self.step = InstallStep::Documents;
        Ok(Next::Call(IoCall::child(WriteDocumentsOperation::new(
            writes,
        ))))
    }

    fn document_writes(
        &self,
        config: Vec<u8>,
        marker: (KeySpace, Key, Value),
    ) -> Option<Vec<(KeySpace, Key, Value)>> {
        let documents = &self.accepted.documents;
        let realm_id = self.plan.realm_id;
        let actor = self.actor();
        let mut writes = vec![marker, document_row(&self.realm(), config)];
        if let Some(authorization) = &documents.realm_authorization {
            writes.push(document_row(&self.authorization(), authorization.clone()));
        }
        if let Some(owner) = &documents.owner {
            writes.push(document_row(&self.owner(), owner.clone()));
        }
        for group in &documents.groups {
            let Some(group_writes) = group_doc_writes(group, &actor) else {
                let group_id = group.group.group_id;
                warn!(%group_id, "A fetched group could not be stored");
                return None;
            };
            writes.extend(group_writes);
        }
        let mut encoded =
            |key_space: &str, bytes: Result<Vec<u8>, postcard::Error>, what: &str| match bytes {
                Ok(bytes) => {
                    let (space, key) = realm_row(key_space, realm_id);
                    writes.push((space, key, Value::from(bytes)));
                    true
                }
                Err(_) => {
                    warn!(what, "Fetched realm documents could not be stored");
                    false
                }
            };
        let urls = postcard::to_allocvec(&documents.management_urls);
        let places = postcard::to_allocvec(&documents.node_places);
        let names = postcard::to_allocvec(&documents.node_names);
        let all = encoded(MANAGEMENT_URL_KEYSPACE, urls, "management urls")
            && encoded(NODE_PLACE_KEYSPACE, places, "node places")
            && encoded(NODE_NAME_KEYSPACE, names, "node display names");
        all.then_some(writes)
    }
}

impl Operation for InstallDocumentsOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Installing {
        Ok(Next::Call(self.stored_read()))
    }

    fn next(&mut self, result: IoResult) -> Installing {
        match self.step {
            InstallStep::Stored => {
                self.keep_stored(result);
                self.step = InstallStep::Groups;
                Ok(Next::Call(IoCall::child(installed_group_docs())))
            }
            InstallStep::Groups => {
                self.groups = result.child::<Vec<DeviceGroupDocuments>, IoError>()??;
                self.step = InstallStep::NodeInfos;
                let infos = self.accepted.documents.node_infos.clone();
                Ok(Next::Call(IoCall::child(InstallInfoOperation::new(infos))))
            }
            InstallStep::NodeInfos => match result.child::<(), NodeInfoError>()? {
                Ok(()) => self.compare(),
                Err(error) => {
                    warn!(%error, "Failed to install the fetched node info documents");
                    Ok(Next::Done(false))
                }
            },
            InstallStep::Marker => Ok(Next::Done(written(result)?)),
            InstallStep::Documents => {
                if !written(result)? {
                    return Ok(Next::Done(false));
                }
                // The peer set and the node kinds this device enforces follow the copy it
                // just installed, exactly as they follow a synced one on a realm node.
                let config = self.config.take().ok_or(IoError::Unfinished)?;
                let effect = NetEffect::RefreshDocumentPeers(Box::new(config));
                self.step = InstallStep::Peers;
                Ok(Next::Call(IoCall::from(Effect::Net(effect))))
            }
            InstallStep::Peers => {
                match result.event()? {
                    Event::Net(NetEvent::DocumentPeers(Err(error))) => {
                        warn!(%error, "Failed to apply the fetched realm configuration");
                    }
                    Event::Net(NetEvent::DocumentPeers(Ok(()))) => {}
                    other => {
                        return Err(IoError::Unexpected {
                            expected: "Event::Net(NetEvent::DocumentPeers)",
                            got: other.label(),
                        });
                    }
                }
                let owner = self.plan.owner;
                debug!(%owner, "Installed the realm documents on this device");
                Ok(Next::Done(true))
            }
        }
    }
}

/// Whether a child write of documents went through.
fn written(result: IoResult) -> Result<bool, IoError> {
    Ok(match result.child::<(), DocumentWriteError>()? {
        Ok(()) => true,
        Err(error) => {
            warn!(%error, "Failed to install the fetched realm documents");
            false
        }
    })
}

/// Carries the device's own revocations into the copy it is about to install.
/// A revoked token stays revoked here whatever the answer forgot, and the floor
/// only ever rises.
fn keep_revocations(fetched: &mut RealmConfigDocument, installed: &RealmConfigDocument, now: u64) {
    fetched.revocation_floor = fetched.revocation_floor.max(installed.revocation_floor);
    for entry in &installed.revoked_tokens {
        if !revocation_live(entry.expires_at, now) {
            continue;
        }
        if !fetched
            .revoked_tokens
            .iter()
            .any(|kept| kept.token_hash == entry.token_hash)
        {
            fetched.revoked_tokens.push(entry.clone());
        }
    }
}

/// The group documents this device holds, in the group-id order both the scan and a serving
/// node's listing produce, so a fetched set compares directly. A group whose two rows disagree
/// is treated as absent and reinstalled.
#[derive(Debug, PartialEq)]
pub struct InstalledGroupsOperation {
    groups: Vec<Group>,
    scanned: bool,
}

/// The read of the installed group documents; see [`InstalledGroupsOperation`].
pub(crate) fn installed_group_docs() -> InstalledGroupsOperation {
    InstalledGroupsOperation {
        groups: Vec::new(),
        scanned: false,
    }
}

impl Operation for InstalledGroupsOperation {
    type Output = Vec<DeviceGroupDocuments>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        let page = IoCall::Page {
            space: GROUP_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: GROUP_SCAN_LIMIT,
        };
        Ok(Next::Call(outside_call(page)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        let answer = outside_answer(result);
        if self.scanned {
            let rows = answer.and_then(|read| read.values()).unwrap_or_default();
            let groups = std::mem::take(&mut self.groups);
            let documents = (groups.into_iter().zip(rows))
                .filter_map(|(group, row)| {
                    let authorization = GroupAuthorizationDocument::from_bytes(&row?).ok()?;
                    Some(DeviceGroupDocuments {
                        group,
                        authorization,
                    })
                })
                .collect();
            return Ok(Next::Done(documents));
        }
        let Ok(IoResult::Page { rows, .. }) = answer else {
            return Ok(Next::Done(Vec::new()));
        };
        self.groups = (rows.iter())
            .filter_map(|(_, value)| Group::from_bytes(value).ok())
            .collect();
        self.scanned = true;
        let reads = (self.groups.iter())
            .map(|group| {
                document_read(&DocumentTarget::GroupAuthorization {
                    group_id: group.group_id,
                })
            })
            .collect();
        Ok(Next::Call(
            outside_call(IoCall::Read(reads)),
        ))
    }
}

/// The two rows one group occupies, encoded as a realm node encodes them.
fn group_doc_writes(
    documents: &DeviceGroupDocuments,
    actor: &Actor,
) -> Option<[(String, Key, Value); 2]> {
    let group_id = documents.group.group_id;
    let group = DocumentTarget::Group { group_id };
    let authorization = DocumentTarget::GroupAuthorization { group_id };
    Some([
        (
            group.storage_keyspace().to_string(),
            group.storage_key(),
            Value::from(documents.group.to_bytes(actor).ok()?),
        ),
        (
            authorization.storage_keyspace().to_string(),
            authorization.storage_key(),
            Value::from(documents.authorization.to_bytes(actor).ok()?),
        ),
    ])
}

/// Why fetched documents were not written.
#[derive(Clone, Debug, thiserror::Error, PartialEq)]
#[error("{0}")]
pub struct DocumentWriteError(String);

impl From<IoError> for DocumentWriteError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

/// Writes document rows in one transaction; the rows of a group this device saw deleted stay
/// out, so a deleted group never comes back from a cached copy.
#[derive(Debug, PartialEq)]
struct WriteDocumentsOperation {
    writes: Vec<(KeySpace, Key, Value)>,
    groups: Vec<(KeySpace, Key)>,
    writing: bool,
}

impl WriteDocumentsOperation {
    fn new(writes: Vec<(KeySpace, Key, Value)>) -> Self {
        let groups = (writes.iter())
            .filter(|(space, _, _)| space == GROUP_KEYSPACE)
            .map(|(_, key, _)| (GROUP_DELETE_KEYSPACE.to_string(), key.clone()))
            .collect();
        Self {
            writes,
            groups,
            writing: false,
        }
    }

    fn write(&mut self) -> Result<Next<()>, DocumentWriteError> {
        self.writing = true;
        Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.writes))))
    }
}

impl Operation for WriteDocumentsOperation {
    type Output = ();
    type Error = DocumentWriteError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, DocumentWriteError> {
        match self.groups.is_empty() {
            true => self.write(),
            false => Ok(Next::Call(IoCall::Read(self.groups.clone()))),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, DocumentWriteError> {
        if self.writing {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let mut closed = BTreeSet::new();
        for ((_, key), row) in self.groups.iter().zip(result.values()?) {
            let Some(row) = row else {
                continue;
            };
            let record = GroupDeleteRecord::from_bytes(&row)
                .map_err(|error| DocumentWriteError(error.to_string()))?;
            if record.phase == GroupDeletePhase::Deleted {
                closed.insert(key.clone());
            }
        }
        self.writes.retain(|(space, key, _)| {
            !matches!(space.as_str(), GROUP_KEYSPACE | AUTH_KEYSPACE) || !closed.contains(key)
        });
        self.write()
    }
}

/// Writes one group's documents where every local read already looks for them, so a group the
/// owner just created answers before the next fetch.
#[derive(Debug, PartialEq)]
pub struct InstallGroupOperation {
    writes: Option<WriteDocumentsOperation>,
}

/// The install of one group's documents; see [`InstallGroupOperation`].
pub fn install_group_docs(
    actor: &Actor,
    group: &Group,
    authorization: &GroupAuthorizationDocument,
) -> InstallGroupOperation {
    let documents = DeviceGroupDocuments {
        group: group.clone(),
        authorization: authorization.clone(),
    };
    let writes = group_doc_writes(&documents, actor);
    InstallGroupOperation {
        writes: writes.map(|writes| WriteDocumentsOperation::new(writes.to_vec())),
    }
}

impl Operation for InstallGroupOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        match self.writes.take() {
            Some(write) => Ok(Next::Call(IoCall::child(write))),
            None => Ok(Next::Done(false)),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        Ok(Next::Done(written(result)?))
    }
}

/// The management api urls a realm node last served this device, in the order it served them.
/// A device holds no peer node-info document, so relaying a management-only route has no other
/// target.
#[derive(Debug, PartialEq)]
pub struct ManagementUrlsOperation {
    realm_id: RealmId,
}

/// The read of the installed management urls; see [`ManagementUrlsOperation`].
pub fn installed_management_urls(realm_id: RealmId) -> ManagementUrlsOperation {
    ManagementUrlsOperation { realm_id }
}

impl Operation for ManagementUrlsOperation {
    type Output = Vec<String>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Vec<String>>, IoError> {
        let key = realm_key(self.realm_id);
        Ok(Next::Call(outside_read(MANAGEMENT_URL_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<String>>, IoError> {
        Ok(Next::Done(decoded_or_default(outside_row(result).as_ref())))
    }
}

/// The node display names and map places a realm node last served this device; empty before the
/// first copy.
#[derive(Debug, PartialEq)]
pub struct InstalledDisplaysOperation {
    realm_id: RealmId,
}

/// The read of the installed node displays; see [`InstalledDisplaysOperation`].
pub fn installed_displays(realm_id: RealmId) -> InstalledDisplaysOperation {
    InstalledDisplaysOperation { realm_id }
}

impl Operation for InstalledDisplaysOperation {
    type Output = NodeDisplays;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<NodeDisplays>, IoError> {
        let reads = vec![
            realm_row(NODE_NAME_KEYSPACE, self.realm_id),
            realm_row(NODE_PLACE_KEYSPACE, self.realm_id),
        ];
        Ok(Next::Call(
            outside_call(IoCall::Read(reads)),
        ))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NodeDisplays>, IoError> {
        let rows = (outside_answer(result).and_then(|read| read.values())).unwrap_or_default();
        let row = |index: usize| rows.get(index).cloned().flatten();
        Ok(Next::Done(NodeDisplays {
            names: decoded_or_default(row(0).as_ref()),
            places: decoded_or_default(row(1).as_ref()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use std::sync::Arc;

    async fn note_membership(
        context: &Arc<DriverContext>,
        realm_id: RealmId,
        refused: bool,
        installed: bool,
    ) {
        let note = NoteMembershipOperation::new(realm_id, refused, installed);
        drive(note, context).await.unwrap();
    }

    async fn realm_removed(context: &Arc<DriverContext>, realm_id: RealmId) -> bool {
        drive(super::realm_removed(realm_id), context)
            .await
            .unwrap()
    }

    async fn install_documents(
        context: &Arc<DriverContext>,
        plan: &FetchPlan,
        accepted: Accepted,
        behind: u32,
    ) -> bool {
        let install = InstallDocumentsOperation::new(plan.clone(), accepted, behind);
        drive(install, context).await.unwrap()
    }

    async fn stored(context: &Arc<DriverContext>, key_space: &str, key: Key) -> Option<Value> {
        let read = aruna_core::effects::StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        };
        match context.storage_handle.send_storage_effect(read).await {
            Event::Storage(aruna_core::events::StorageEvent::ReadResult { value, .. }) => value,
            _ => None,
        }
    }

    async fn installed_marker(context: &Arc<DriverContext>, realm_id: RealmId) -> RealmMarker {
        let key = Key::from(realm_key(realm_id));
        marker_of(stored(context, REALM_MARKER_KEYSPACE, key).await)
    }

    async fn read_bytes(context: &Arc<DriverContext>, target: DocumentTarget) -> Option<Vec<u8>> {
        let row = stored(context, target.storage_keyspace(), target.storage_key()).await;
        row.map(|bytes| bytes.as_ref().to_vec())
    }

    async fn write_batch(context: &Arc<DriverContext>, writes: Vec<(String, Key, Value)>) -> bool {
        drive(WriteDocumentsOperation::new(writes), context)
            .await
            .is_ok()
    }

    async fn installed_displays(context: &Arc<DriverContext>, realm_id: RealmId) -> NodeDisplays {
        drive(super::installed_displays(realm_id), context)
            .await
            .unwrap()
    }

    async fn installed_group_docs(context: &Arc<DriverContext>) -> Vec<DeviceGroupDocuments> {
        drive(super::installed_group_docs(), context).await.unwrap()
    }

    async fn install_group_docs(
        context: &Arc<DriverContext>,
        actor: &Actor,
        group: &Group,
        authorization: &GroupAuthorizationDocument,
    ) -> bool {
        let install = super::install_group_docs(actor, group, authorization);
        drive(install, context).await.unwrap()
    }

    use aruna_core::structs::identity::realm::{RealmNodeKind, TokenRevocation};
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([9u8; 32])
    }

    fn clock(entries: &[(u8, u64)]) -> AdminDocumentClock {
        let mut clock = AdminDocumentClock::default();
        for (seed, seq) in entries {
            clock.advance(node(*seed), *seq);
        }
        clock
    }

    fn config(nodes: &[u8]) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 1);
        for seed in nodes {
            config.ensure_node(node(*seed), RealmNodeKind::Server);
        }
        config
    }

    fn answer(nodes: &[u8], seen: &[(u8, u64)]) -> Accepted {
        let config = config(nodes);
        let documents = RealmDocuments {
            realm_config: config
                .to_bytes(&Actor {
                    node_id: node(1),
                    user_id: UserId::nil(realm()),
                    realm_id: realm(),
                })
                .expect("config encodes"),
            realm_authorization: None,
            owner: None,
            groups: Vec::new(),
            node_infos: Vec::new(),
            management_urls: Vec::new(),
            clock: clock(seen),
            node_names: BTreeMap::new(),
            node_places: BTreeMap::new(),
        };
        accept(documents, realm()).expect("the answer is this realm's")
    }

    fn answer_groups(nodes: &[u8], groups: Vec<DeviceGroupDocuments>) -> Accepted {
        let mut accepted = answer(nodes, &[]);
        accepted.documents.groups = groups;
        accepted
    }

    // A copy that has seen less than the installed one is a rollback, whoever
    // serves it: the realm's own lagging node and an evicted one look the same.
    #[test]
    fn refuses_older_copy() {
        let origins = realm_origins(&config(&[1, 2, 3]));
        let installed = clock(&[(1, 7), (2, 3)]);
        assert!(covers(&installed, &installed, &origins));
        assert!(covers(
            &clock(&[(1, 8), (2, 3), (3, 1)]),
            &installed,
            &origins
        ));
        assert!(!covers(&clock(&[(1, 6), (2, 3)]), &installed, &origins));
        assert!(!covers(&clock(&[(1, 7)]), &installed, &origins));
        // Nothing installed yet accepts the first answer.
        assert!(covers(
            &AdminDocumentClock::default(),
            &AdminDocumentClock::default(),
            &origins
        ));
    }

    // Only the realm's own nodes may appear in a marker: a peer that invents an origin,
    // or one the realm has since evicted, must not lock this device out of every copy.
    #[test]
    fn ignores_forged_origins() {
        let realm_nodes = config(&[1, 2]);
        let origins = realm_origins(&realm_nodes);
        let inflated = clock(&[(1, 4), (7, u64::MAX)]);
        assert!(covers(&clock(&[(1, 4)]), &inflated, &origins));
        assert!(
            !trim_clock(&inflated, &origins)
                .origins
                .contains_key(&node(7))
        );

        // A copy is stored under the trimmed clock, so nothing forged is kept.
        let accepted = answer(&[1, 2], &[(1, 4), (7, u64::MAX)]);
        assert_eq!(accepted.clock, clock(&[(1, 4)]));
    }

    // One refusal round proves nothing, two report removal, and a working fetch clears it.
    #[tokio::test]
    async fn reports_realm_removal() {
        let (_dir, context) = device(&config(&[1, 2])).await;
        assert!(membership_refused(
            "connection lost: closed by peer: realm membership (code 0)"
        ));
        assert!(membership_refused(
            "connection lost: closed by peer: realm admission (code 0)"
        ));
        assert!(!membership_refused("connection lost: closed"));

        note_membership(&context, realm(), true, false).await;
        assert!(!realm_removed(&context, realm()).await);
        note_membership(&context, realm(), false, false).await;
        note_membership(&context, realm(), true, false).await;
        assert!(realm_removed(&context, realm()).await);
        note_membership(&context, realm(), false, true).await;
        assert!(!realm_removed(&context, realm()).await);
    }

    // A marker can only be too high because a peer lied, so the realm agreeing
    // against it is what makes it wrong - and only after it keeps saying so.
    #[test]
    fn rebases_after_agreement() {
        assert_eq!(rebase_after(0), (false, 1));
        assert_eq!(rebase_after(1), (false, 2));
        assert_eq!(rebase_after(2), (true, 0));
    }

    // One lagging node among fresh ones is not the realm disagreeing: the fresh
    // copy is taken and nothing is ever re-based on the lagging one.
    #[test]
    fn prefers_fresh_copy() {
        let installed = clock(&[(1, 5)]);
        let mut selection = Selection::default();

        assert!(!selection.offer(answer(&[1, 2], &[(1, 3)]), &installed));
        assert!(selection.offer(answer(&[1, 2], &[(1, 6)]), &installed));
        assert_eq!(
            selection.current.expect("the fresh copy is taken").clock,
            clock(&[(1, 6)])
        );

        // Different older answers do not corroborate one another.
        let mut agreed = Selection::default();
        assert!(!agreed.offer(answer(&[1, 2], &[(1, 2)]), &installed));
        assert!(!agreed.offer(answer(&[1, 2], &[(1, 4)]), &installed));
        assert!(agreed.current.is_none());
        assert!(agreed.behind.is_none());

        // Two peers returning the same bundle may repair an inflated marker.
        assert!(!agreed.offer(answer(&[1, 2], &[(1, 4)]), &installed));
        assert_eq!(
            agreed.behind.expect("a candidate to re-base on").clock,
            clock(&[(1, 4)])
        );
    }

    async fn device(config: &RealmConfigDocument) -> (tempfile::TempDir, Arc<DriverContext>) {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().expect("path"))
            .expect("storage opens");
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        });
        let target = DocumentTarget::RealmConfig { realm_id: realm() };
        let bytes = config
            .to_bytes(&Actor {
                node_id: node(1),
                user_id: UserId::nil(realm()),
                realm_id: realm(),
            })
            .expect("config encodes");
        write_batch(
            &context,
            vec![(
                target.storage_keyspace().to_string(),
                target.storage_key(),
                Value::from(bytes),
            )],
        )
        .await;
        (dir, context)
    }

    // A copy that changed nothing still moves the marker: the device saw more
    // of the realm than the marker said, and the next answer is judged by that.
    #[tokio::test]
    async fn advances_unchanged_marker() {
        let (_dir, context) = device(&config(&[1, 2])).await;
        let plan = FetchPlan {
            realm_id: realm(),
            node_id: node(1),
            owner: UserId::nil(realm()),
            auth: AuthContext {
                user_id: UserId::nil(realm()),
                realm_id: realm(),
                path_restrictions: None,
                session: None,
            },
            peers: Vec::new(),
            marker: RealmMarker {
                clock: clock(&[(1, 4)]),
                behind: 2,
            },
        };
        let stored = read_bytes(&context, DocumentTarget::RealmConfig { realm_id: realm() })
            .await
            .expect("the device holds a configuration");

        assert!(install_documents(&context, &plan, answer(&[1, 2], &[(1, 6)]), 0).await);

        assert_eq!(
            installed_marker(&context, realm()).await,
            RealmMarker {
                clock: clock(&[(1, 6)]),
                behind: 0
            }
        );
        assert_eq!(
            read_bytes(&context, DocumentTarget::RealmConfig { realm_id: realm() }).await,
            Some(stored),
            "the documents themselves are not rewritten"
        );
    }

    // A device runs no document sync, so its realm listing shows only the names and places a
    // copy brought.
    #[tokio::test]
    async fn installs_node_names() {
        let (_dir, context) = device(&config(&[1, 2])).await;
        let plan = FetchPlan {
            realm_id: realm(),
            node_id: node(1),
            owner: UserId::nil(realm()),
            auth: AuthContext {
                user_id: UserId::nil(realm()),
                realm_id: realm(),
                path_restrictions: None,
                session: None,
            },
            peers: Vec::new(),
            marker: RealmMarker::default(),
        };
        let names = BTreeMap::from([(node(2), "Lab laptop".to_string())]);
        let place = aruna_core::structs::identity::realm::NodePlace {
            name: Some("Giessen".to_string()),
            coordinates: None,
        };
        let places = BTreeMap::from([(node(2), place)]);
        let mut named = answer(&[1, 2], &[(1, 6)]);
        named.documents.node_names = names.clone();
        named.documents.node_places = places.clone();
        assert!(install_documents(&context, &plan, named, 0).await);
        let installed = installed_displays(&context, realm()).await;
        assert_eq!((installed.names, installed.places), (names, places));

        // A later copy with the name cleared must clear it here too.
        let plan = FetchPlan {
            marker: installed_marker(&context, realm()).await,
            ..plan
        };
        assert!(install_documents(&context, &plan, answer(&[1, 2], &[(1, 7)]), 0).await);
        assert_eq!(
            installed_displays(&context, realm()).await,
            NodeDisplays::default()
        );
    }

    fn group_docs(seed: u128, display_name: &str) -> DeviceGroupDocuments {
        let group_id = Ulid::from(seed);
        let owner = UserId::nil(realm());
        DeviceGroupDocuments {
            group: Group {
                display_name: display_name.to_string(),
                group_id,
                realm_id: realm(),
                roles: Default::default(),
                owner,
            },
            authorization: GroupAuthorizationDocument::default_group_doc(owner, realm(), group_id),
        }
    }

    fn owner_actor() -> Actor {
        Actor {
            node_id: node(1),
            user_id: UserId::nil(realm()),
            realm_id: realm(),
        }
    }

    // A group the owner created on this device must answer at once, from the
    // same two rows a realm node holds, and a repeat must not duplicate it.
    #[tokio::test]
    async fn installs_created_group() {
        let (_dir, context) = device(&config(&[1])).await;
        let actor = owner_actor();

        let second = group_docs(2, "second");
        assert!(install_group_docs(&context, &actor, &second.group, &second.authorization).await);
        let first = group_docs(1, "first");
        assert!(install_group_docs(&context, &actor, &first.group, &first.authorization).await);
        let renamed = group_docs(2, "renamed");
        assert!(install_group_docs(&context, &actor, &renamed.group, &renamed.authorization).await);

        assert_eq!(
            installed_group_docs(&context).await,
            vec![first, renamed],
            "one pair of rows per group, in group-id order"
        );
    }

    #[tokio::test]
    async fn cache_stays_deleted() {
        use aruna_core::structs::identity::group_delete::{
            GroupDeletePhase, GroupDeletePlan, GroupDeleteRecord,
        };
        let (_dir, context) = device(&config(&[1])).await;
        let mut documents = group_docs(1, "Deleted");
        let owner = UserId::local(Ulid::from(3), realm());
        documents.group.owner = owner;
        documents.authorization =
            GroupAuthorizationDocument::default_group_doc(owner, realm(), documents.group.group_id);
        let actor = Actor {
            node_id: node(1),
            user_id: owner,
            realm_id: realm(),
        };
        let record = GroupDeleteRecord {
            plan: GroupDeletePlan {
                request_id: Ulid::from(2),
                group_id: documents.group.group_id,
                realm_id: realm(),
                owner,
                requested_by: owner,
                coordinator: node(1),
                nodes: BTreeSet::from([node(1)]),
            },
            phase: GroupDeletePhase::Deleted,
            deleted_by: Some(owner),
            event: None,
        };
        assert!(
            write_batch(
                &context,
                vec![(
                    aruna_core::keyspaces::GROUP_DELETE_KEYSPACE.to_string(),
                    documents.group.group_id.to_bytes().into(),
                    record.to_bytes().unwrap().into()
                )]
            )
            .await
        );
        assert!(
            install_group_docs(&context, &actor, &documents.group, &documents.authorization).await
        );
        assert!(installed_group_docs(&context).await.is_empty());
        assert!(
            read_bytes(
                &context,
                DocumentTarget::GroupAuthorization {
                    group_id: documents.group.group_id
                }
            )
            .await
            .is_none()
        );
    }

    // An unversioned group snapshot cannot prove that an absent group was
    // removed rather than still replicating to the serving node.
    #[tokio::test]
    async fn keeps_unlisted_groups() {
        let (_dir, context) = device(&config(&[1])).await;
        let actor = owner_actor();
        let kept = group_docs(1, "kept");
        let dropped = group_docs(2, "dropped");
        let realm_auth = DocumentTarget::RealmAuthorization { realm_id: realm() };
        assert!(
            write_batch(
                &context,
                vec![(
                    realm_auth.storage_keyspace().to_string(),
                    realm_auth.storage_key(),
                    Value::from(vec![7u8; 8]),
                )],
            )
            .await
        );
        assert!(install_group_docs(&context, &actor, &kept.group, &kept.authorization).await);
        assert!(install_group_docs(&context, &actor, &dropped.group, &dropped.authorization).await);
        let plan = FetchPlan {
            realm_id: realm(),
            node_id: node(1),
            owner: UserId::nil(realm()),
            auth: AuthContext {
                user_id: UserId::nil(realm()),
                realm_id: realm(),
                path_restrictions: None,
                session: None,
            },
            peers: Vec::new(),
            marker: RealmMarker::default(),
        };

        assert!(
            install_documents(&context, &plan, answer_groups(&[1], vec![kept.clone()]), 0).await
        );

        assert_eq!(
            installed_group_docs(&context).await,
            vec![kept, dropped.clone()]
        );
        assert!(
            read_bytes(
                &context,
                DocumentTarget::Group {
                    group_id: dropped.group.group_id
                }
            )
            .await
            .is_some()
        );
        assert_eq!(
            read_bytes(&context, realm_auth).await,
            Some(vec![7u8; 8]),
            "the realm authorization row is not a group row"
        );
    }

    // A fetch needs the transport that names this device: owner and peers alone never do.
    #[test]
    fn fetch_needs_transport() {
        let source = FetchSource::Peers {
            owner: UserId::nil(realm()),
            peers: vec![node(2)],
        };
        let mut fetch = FetchDocumentsOperation::new(None, source, Duration::from_secs(1));
        assert_eq!(fetch.first(), Ok(Next::Done(false)));
    }

    fn revoked(revoked: &[(&str, u64)], floor: u64) -> RealmConfigDocument {
        let mut config = config(&[1]);
        config.revocation_floor = floor;
        config.revoked_tokens = revoked
            .iter()
            .map(|(hash, expires_at)| TokenRevocation {
                token_hash: (*hash).to_string(),
                expires_at: *expires_at,
            })
            .collect();
        config
    }

    // A revocation this device already holds must survive a copy that lost it,
    // and an expired one must not come back with it.
    #[test]
    fn keeps_local_revocations() {
        let now = 1_000;
        let installed = revoked(&[("live", now + 60), ("gone", now - 60)], 900);
        let mut fetched = revoked(&[("other", now + 60)], 800);

        keep_revocations(&mut fetched, &installed, now);

        let hashes: Vec<&str> = fetched
            .revoked_tokens
            .iter()
            .map(|entry| entry.token_hash.as_str())
            .collect();
        assert!(
            hashes.contains(&"live"),
            "a live revocation may not be lost"
        );
        assert!(hashes.contains(&"other"));
        assert!(!hashes.contains(&"gone"), "an expired entry stays pruned");
        assert_eq!(fetched.revocation_floor, 900);
    }
}
