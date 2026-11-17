//! Reads realm vault secrets: management nodes open them, other nodes fetch them once and cache
//! the current revision in their node vault. Management nodes also serve both to their peers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::compute::SecretBytes;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::NODE_VAULT_KEYSPACE;
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::Operation;
use aruna_core::realm_vault::{
    FetchScope, RealmSealed, RealmSecretRef, RealmVaultError, RealmVaultRecord,
};
use aruna_core::structs::execution::setting::{SettingChannel, validate_setting};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::types::{Effects, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::debug;
use ulid::Ulid;

use crate::document_repository::read_effect;
use crate::driver::DriverContext;
use crate::forward::fetch::{Answer, Budget, Fetched, HolderFetchOperation, HolderRecord};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::node_vault::{delete_secret, parse_secret, read_secret, write_secret};
use crate::realm::mutate_placement::{is_management, node_kind};

/// Cache rows read per page while pruning.
const PRUNE_PAGE: usize = 256;
/// The time a fetch may spend across the management nodes, split evenly between them.
const FETCH_DEADLINE: Duration = Duration::from_secs(60);

/// Why a secret is not available now. Every case may pass once replication or a peer catches up.
#[derive(Debug, Error, PartialEq)]
pub enum RealmSecretError {
    #[error("the realm vault entry is not replicated to this node yet")]
    NotReplicated,
    #[error("no management node handed out the secret: {0}")]
    Unavailable(String),
    #[error(transparent)]
    Vault(#[from] RealmVaultError),
    #[error("the node vault cache failed: {0}")]
    Cache(String),
}

/// Reads the secret `reference` names, when it is the current revision in the config: opened with
/// the realm keys this node holds, else from the node vault cache, else fetched from the
/// management nodes in config order and cached.
#[derive(Debug, PartialEq)]
pub struct ReadSecretOperation {
    reference: RealmSecretRef,
    sealed: Option<RealmSealed>,
    managers: Vec<NodeId>,
    state: ReadState,
    output: Option<Result<SecretBytes, RealmSecretError>>,
}

#[derive(Debug, PartialEq)]
enum ReadState {
    Init,
    Open,
    Cache,
    Fetch(Box<HolderFetchOperation<SecretBytes>>),
    Write(SecretBytes),
    Done,
}

impl ReadSecretOperation {
    pub fn new(config: &RealmConfigDocument, reference: RealmSecretRef) -> Self {
        let sealed = current_record(config, reference).map(|record| record_sealed(config, record));
        Self {
            reference,
            sealed,
            managers: managers(config),
            state: ReadState::Init,
            output: None,
        }
    }

    fn finish(&mut self, result: Result<SecretBytes, RealmSecretError>) -> Effects {
        self.state = ReadState::Done;
        self.output = Some(result);
        smallvec![]
    }

    fn opened(&mut self, result: Result<SecretBytes, RealmVaultError>) -> Effects {
        match result {
            Err(RealmVaultError::NoKey) => {
                self.state = ReadState::Cache;
                smallvec![read_secret(cache_entry(self.reference), None)]
            }
            opened => self.finish(opened.map_err(RealmSecretError::from)),
        }
    }

    fn cached(&mut self, event: Event) -> Effects {
        let entry = cache_entry(self.reference);
        let reference = self.reference;
        match parse_secret(event, entry, |bytes| Ok(cached_secret(bytes, reference))) {
            Ok(Some(Some(secret))) => self.finish(Ok(secret)),
            Ok(_) => {
                let request = MetadataTransportMessage::RealmSecret {
                    entry: reference.entry,
                    revision: reference.revision,
                };
                let mut fetch = HolderFetchOperation::new(
                    &self.managers,
                    &request,
                    FETCH_DEADLINE,
                    Budget::Split,
                );
                let effects = fetch.start();
                self.fetching(fetch, effects)
            }
            Err(error) => self.finish(Err(RealmSecretError::Cache(error.to_string()))),
        }
    }

    fn fetching(&mut self, fetch: HolderFetchOperation<SecretBytes>, effects: Effects) -> Effects {
        if !fetch.is_complete() {
            self.state = ReadState::Fetch(Box::new(fetch));
            return effects;
        }
        match fetch.finalize() {
            Ok(Fetched::Found { value, .. }) => {
                let row = [
                    &[self.reference.entry.purpose.tag()][..],
                    &self.reference.revision.to_bytes(),
                    value.expose(),
                ];
                let entry = cache_entry(self.reference);
                let write = write_secret(entry, SecretBytes::new(row.concat()), None);
                self.state = ReadState::Write(value);
                smallvec![write]
            }
            Ok(Fetched::Unavailable(reason)) => {
                self.finish(Err(RealmSecretError::Unavailable(reason)))
            }
            Ok(Fetched::NotFound | Fetched::Denied) => self.finish(Err(
                RealmSecretError::Unavailable("no management node handed it out".to_string()),
            )),
            Err(error) => self.finish(Err(RealmSecretError::Unavailable(error.to_string()))),
        }
    }
}

impl Operation for ReadSecretOperation {
    type Output = SecretBytes;
    type Error = RealmSecretError;

    fn start(&mut self) -> Effects {
        let Some(sealed) = self.sealed.take() else {
            return self.finish(Err(RealmSecretError::NotReplicated));
        };
        self.state = ReadState::Open;
        smallvec![Effect::Storage(StorageEffect::RealmOpen(Box::new(sealed)))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::replace(&mut self.state, ReadState::Done), event) {
            (ReadState::Open, Event::Storage(StorageEvent::RealmOpened { result })) => {
                self.opened(result)
            }
            (ReadState::Cache, event) => self.cached(event),
            (ReadState::Fetch(mut fetch), event) => {
                let effects = fetch.step(event);
                self.fetching(*fetch, effects)
            }
            (ReadState::Write(secret), Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.finish(Ok(secret))
            }
            (_, event) => self.finish(Err(RealmSecretError::Cache(event.label().to_string()))),
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<SecretBytes, RealmSecretError> {
        self.output.unwrap_or(Err(RealmSecretError::Unavailable(
            "the read did not finish".to_string(),
        )))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// The management nodes of `config`, in config order.
fn managers(config: &RealmConfigDocument) -> Vec<NodeId> {
    (config.nodes.iter())
        .filter(|node| node.kind == RealmNodeKind::Management)
        .filter_map(|node| node.node_id.parse::<NodeId>().ok())
        .collect()
}

impl HolderRecord for SecretBytes {
    const LABEL: &'static str = "realm secret";

    fn read(message: MetadataTransportMessage) -> Answer<Self> {
        match message {
            MetadataTransportMessage::RealmSecretResult { result: Ok(secret) } => {
                Answer::Found(secret)
            }
            _ => Answer::Skip,
        }
    }
}

fn current_record(
    config: &RealmConfigDocument,
    reference: RealmSecretRef,
) -> Option<&RealmVaultRecord> {
    (config.vault.iter())
        .find(|record| record.entry == reference.entry && record.revision == reference.revision)
}

fn cache_entry(reference: RealmSecretRef) -> VaultEntry {
    VaultEntry::new(VaultPurpose::RealmSecret, reference.entry.id)
}

/// A cache row is `purpose tag || revision || secret`; another purpose or revision is a miss.
fn cached_secret(bytes: &[u8], reference: RealmSecretRef) -> Option<SecretBytes> {
    let (tag, rest) = bytes.split_first()?;
    let (revision, secret) = rest.split_first_chunk::<16>()?;
    (*tag == reference.entry.purpose.tag() && Ulid::from_bytes(*revision) == reference.revision)
        .then(|| SecretBytes::new(secret.to_vec()))
}

fn check_fetch(
    config: &RealmConfigDocument,
    local: NodeId,
    peer: NodeId,
    request: RealmSecretRef,
) -> Result<&RealmVaultRecord, MetadataReadError> {
    if !is_management(config, local) {
        return Err(MetadataReadError::Unavailable);
    }
    let scope = (request.entry.purpose.fetch_scope()).ok_or(MetadataReadError::Forbidden)?;
    if !node_kind(config, peer).is_some_and(|kind| scope.allows(&kind)) {
        return Err(MetadataReadError::Forbidden);
    }
    let current = (config.vault.iter()).find(|record| record.entry == request.entry);
    match current {
        Some(record) if record.revision == request.revision => Ok(record),
        // An older revision is never handed out; a newer one has not reached this node yet.
        Some(record) if record.revision > request.revision => Err(MetadataReadError::Forbidden),
        _ => Err(MetadataReadError::Unavailable),
    }
}

/// A node without the realm keys cannot answer; a secret that does not open is refused.
fn setting_error(error: RealmVaultError) -> MetadataReadError {
    match error {
        RealmVaultError::NoKey => MetadataReadError::Unavailable,
        _ => MetadataReadError::Forbidden,
    }
}

fn record_sealed(config: &RealmConfigDocument, record: &RealmVaultRecord) -> RealmSealed {
    RealmSealed::Record {
        realm_id: config.realm_id,
        record: record.clone(),
    }
}

/// The sealed secret of a valid setting document; an off setting has none.
pub(crate) fn sealed_setting(
    config: &RealmConfigDocument,
    peer: NodeId,
    target: &DocumentTarget,
    bytes: &[u8],
) -> Result<RealmSealed, MetadataReadError> {
    infrastructure_peer(config, peer)?;
    let head = validate_setting(target, bytes).map_err(|_| MetadataReadError::Forbidden)?;
    let (channel, owner, id) =
        SettingChannel::of_target(target).ok_or(MetadataReadError::Forbidden)?;
    let sealed = channel
        .sealed(bytes)
        .map_err(|_| MetadataReadError::Forbidden)?;
    let sealed = sealed
        .filter(|_| head.on)
        .ok_or(MetadataReadError::NotFound)?;
    Ok(RealmSealed::Setting {
        realm_id: config.realm_id,
        channel,
        owner,
        id,
        sealed,
    })
}

/// What a peer asks a management node to open.
#[derive(Debug, PartialEq)]
pub enum ServeRequest {
    /// The current revision of a realm vault entry, for a peer in the purpose's fetch scope.
    Secret(RealmSecretRef),
    /// The sealed secret of a setting document, for an infrastructure peer of a delivery.
    Setting {
        target: DocumentTarget,
        bytes: Vec<u8>,
    },
}

#[derive(Debug, PartialEq)]
enum ServeState {
    Init,
    Config,
    Open,
    Done,
}

/// The management side of a secret request. Only a node that holds the realm keys answers, and
/// only a peer the realm config allows; a refusal is part of the answer, never a failure.
#[derive(Debug, PartialEq)]
pub struct ServeSecretOperation {
    realm_id: RealmId,
    local: NodeId,
    peer: NodeId,
    request: ServeRequest,
    state: ServeState,
    output: Option<Result<SecretAnswer, MetadataReadError>>,
}

/// The secret a peer gets, or why it is refused.
pub type SecretAnswer = Result<SecretBytes, MetadataReadError>;

impl ServeSecretOperation {
    pub fn new(realm_id: RealmId, local: NodeId, peer: NodeId, request: ServeRequest) -> Self {
        Self {
            realm_id,
            local,
            peer,
            request,
            state: ServeState::Init,
            output: None,
        }
    }

    fn answer(&mut self, answer: SecretAnswer) -> Effects {
        self.state = ServeState::Done;
        self.output = Some(Ok(answer));
        smallvec![]
    }

    fn config(&mut self, value: Option<Value>) -> Effects {
        let config = value.and_then(|value| RealmConfigDocument::from_bytes(&value).ok());
        let Some(config) = config else {
            return self.answer(Err(MetadataReadError::Unavailable));
        };
        let sealed = match &self.request {
            ServeRequest::Secret(request) => check_fetch(&config, self.local, self.peer, *request)
                .map(|record| record_sealed(&config, record)),
            ServeRequest::Setting { target, bytes } if is_management(&config, self.local) => {
                sealed_setting(&config, self.peer, target, bytes)
            }
            ServeRequest::Setting { .. } => Err(MetadataReadError::Unavailable),
        };
        match sealed {
            Ok(sealed) => {
                self.state = ServeState::Open;
                smallvec![Effect::Storage(StorageEffect::RealmOpen(Box::new(sealed)))]
            }
            Err(error) => self.answer(Err(error)),
        }
    }

    fn opened(&mut self, result: Result<SecretBytes, RealmVaultError>) -> Effects {
        let answer = match &self.request {
            ServeRequest::Secret(request) => {
                if result.is_ok() {
                    debug!(peer = %self.peer, entry = ?request.entry, revision = %request.revision, "Served a realm vault entry");
                }
                result.map_err(|_| MetadataReadError::Unavailable)
            }
            ServeRequest::Setting { .. } => result.map_err(setting_error),
        };
        self.answer(answer)
    }
}

impl Operation for ServeSecretOperation {
    type Output = SecretAnswer;
    type Error = MetadataReadError;

    fn start(&mut self) -> Effects {
        self.state = ServeState::Config;
        let config = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        smallvec![read_effect(&config, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::replace(&mut self.state, ServeState::Done), event) {
            (ServeState::Config, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.config(value)
            }
            (ServeState::Config, Event::Storage(StorageEvent::Error { .. })) => {
                self.answer(Err(MetadataReadError::Unavailable))
            }
            (ServeState::Open, Event::Storage(StorageEvent::RealmOpened { result })) => {
                self.opened(result)
            }
            _ => {
                self.output = Some(Err(MetadataReadError::Unavailable));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<SecretAnswer, MetadataReadError> {
        self.output.unwrap_or(Err(MetadataReadError::Unavailable))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Server and management nodes may open settings; user devices never.
fn infrastructure_peer(
    config: &RealmConfigDocument,
    peer: NodeId,
) -> Result<(), MetadataReadError> {
    match node_kind(config, peer) {
        Some(kind) if FetchScope::Infrastructure.allows(&kind) => Ok(()),
        _ => Err(MetadataReadError::Forbidden),
    }
}

/// Deletes cached secrets whose entry or revision is no longer current in `config`.
pub async fn prune_secret_cache(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
) -> Result<usize, String> {
    let prefix = vec![VaultPurpose::RealmSecret.tag()];
    let mut start = None;
    let mut pruned = 0;
    loop {
        let page = StorageEffect::Iter {
            key_space: NODE_VAULT_KEYSPACE.to_string(),
            prefix: Some(prefix.clone().into()),
            start: start.take(),
            limit: PRUNE_PAGE,
            txn_id: None,
        };
        let (keys, next) = match ctx.storage_handle.send_storage_effect(page).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values.into_iter().map(|(key, _)| key), next_start_after),
            other => return Err(format!("node vault page: {}", other.label())),
        };
        for key in keys {
            let Some(id) = key.get(1..).and_then(|id| <[u8; 16]>::try_from(id).ok()) else {
                continue;
            };
            let entry = VaultEntry::new(VaultPurpose::RealmSecret, Ulid::from_bytes(id));
            let event = ctx
                .storage_handle
                .send_effect(read_secret(entry, None))
                .await;
            let current = parse_secret(event, entry, |bytes| Ok(cached_current(config, id, bytes)))
                .map_err(|error| error.to_string())?;
            if current == Some(true) {
                continue;
            }
            match ctx
                .storage_handle
                .send_effect(delete_secret(entry, None))
                .await
            {
                Event::Storage(StorageEvent::DeleteResult { .. }) => pruned += 1,
                other => return Err(format!("node vault delete: {}", other.label())),
            }
        }
        match next {
            Some(after) => start = Some(IterStart::After(after)),
            None => return Ok(pruned),
        }
    }
}

fn cached_current(config: &RealmConfigDocument, id: [u8; 16], bytes: &[u8]) -> bool {
    let mut fetchable =
        (config.vault.iter()).filter(|record| record.entry.purpose.fetch_scope().is_some());
    fetchable.any(|record| {
        let reference = RealmSecretRef {
            entry: record.entry,
            revision: record.revision,
        };
        record.entry.id == Ulid::from_bytes(id) && cached_secret(bytes, reference).is_some()
    })
}

#[cfg(test)]
#[path = "realm_vault_tests.rs"]
mod tests;
