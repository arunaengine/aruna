//! Fetches one registered Profile revision for validation: from this node when it holds the
//! Profile, from the Profile's holders over the validation channel, or as its owner on a device.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::placement::read_holder_sets;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use super::holder_reads::collect_profile_export;
use super::read::{READ_FANOUT_LIMIT, READ_PEER_TIMEOUT, RoutedExportOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::fetch::{FanoutOperation, FetchError, PeerAnswer};
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export_types::{
    ExportMetadataRequest, ExportMetadataResult, RoCrateExportView,
};
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::live_records::{LiveRecordOperation, LiveRecordsError};
use crate::metadata::profile::rules::PROFILE_DOCUMENT_PREFIX;
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::metadata::raw_events::RawReadError;
use crate::metadata::revision_read::{MetadataRawView, RawDisplayOperation};
use crate::realm::peer_trust::is_sync_eligible;

type ExportTurn = Result<Next<ExportMetadataResult>, MetadataReadError>;

#[derive(Debug, Clone, Copy, PartialEq)]
enum LocalStage {
    Record,
    Raw,
}

/// Serves one registered Profile with no caller at all. The `profiles/` path, the graph IRI and
/// the revision fence are the whole authorization; every other document is reported as not found.
#[derive(Debug, PartialEq)]
pub struct ProfileLocalOperation {
    realm_id: RealmId,
    profile_id: Ulid,
    expected_revision: Ulid,
    stage: LocalStage,
    record: Option<MetadataRegistryRecord>,
}

impl ProfileLocalOperation {
    pub fn new(realm_id: RealmId, profile_id: Ulid, expected_revision: Ulid) -> Self {
        Self {
            realm_id,
            profile_id,
            expected_revision,
            stage: LocalStage::Record,
            record: None,
        }
    }

    fn record_read(&mut self, result: IoResult) -> ExportTurn {
        let record = result
            .child::<Option<MetadataRegistryRecord>, LiveRecordsError>()?
            .map_err(|_| MetadataReadError::Unavailable)?
            .ok_or(MetadataReadError::NotFound)?;
        if record.realm_id != self.realm_id
            || record.document_id != self.profile_id
            || !record.document_path.starts_with(PROFILE_DOCUMENT_PREFIX)
            || record.graph_iri != MetadataRegistryRecord::graph_iri_for(self.profile_id)
        {
            return Err(MetadataReadError::NotFound);
        }
        if record.last_event_id != self.expected_revision {
            return Err(MetadataReadError::Unavailable);
        }
        self.record = Some(record);
        self.stage = LocalStage::Raw;
        Ok(Next::Call(IoCall::child(RawDisplayOperation::new(
            self.profile_id,
        ))))
    }

    fn raw_read(&mut self, result: IoResult) -> ExportTurn {
        let raw = result
            .child::<Option<MetadataRawView>, RawReadError>()?
            .map_err(|_| MetadataReadError::Unavailable)?
            .ok_or(MetadataReadError::NotFound)?;
        if raw.revision.winning_event_id != self.expected_revision {
            return Err(MetadataReadError::Unavailable);
        }
        let record = self.record.take().ok_or(MetadataReadError::Unavailable)?;
        let dataset_digest = raw.revision.dataset_digest;
        Ok(Next::Done(ExportMetadataResult::Raw {
            record,
            raw,
            dataset_digest,
        }))
    }
}

impl Operation for ProfileLocalOperation {
    type Output = ExportMetadataResult;
    type Error = MetadataReadError;

    fn first(&mut self) -> ExportTurn {
        Ok(Next::Call(IoCall::child(LiveRecordOperation::new(
            self.profile_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> ExportTurn {
        match self.stage {
            LocalStage::Record => self.record_read(result),
            LocalStage::Raw => self.raw_read(result),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ExportStage {
    /// Reading the realm config, which names the holders and the channel's config digest.
    Config,
    /// This node serves its own copy, as a holder or without a network.
    Local,
    /// Asking the other holders at once.
    Peers,
    /// A device asks as its owner.
    Owner,
}

/// One registered Profile revision, from any holder that serves exactly that revision.
#[derive(Debug, PartialEq)]
pub struct ProfileExportOperation {
    local_node: Option<NodeId>,
    realm_id: RealmId,
    profile_id: Ulid,
    expected_revision: Ulid,
    stage: ExportStage,
    peers: Vec<NodeId>,
    message: Option<MetadataTransportMessage>,
    holder_count: usize,
    answers: Vec<(NodeId, Result<ExportMetadataResult, MetadataReadError>)>,
}

impl ProfileExportOperation {
    /// `local_node` is this node on the network; a node without one serves only its own copy.
    pub fn new(
        local_node: Option<NodeId>,
        realm_id: RealmId,
        profile_id: Ulid,
        expected_revision: Ulid,
    ) -> Self {
        Self {
            local_node,
            realm_id,
            profile_id,
            expected_revision,
            stage: ExportStage::Config,
            peers: Vec::new(),
            message: None,
            holder_count: 0,
            answers: Vec::new(),
        }
    }

    fn local(&self) -> IoCall {
        IoCall::child(ProfileLocalOperation::new(
            self.realm_id,
            self.profile_id,
            self.expected_revision,
        ))
    }

    fn config_read(&mut self, result: IoResult) -> ExportTurn {
        let config = result
            .child::<RealmConfigDocument, GetConfigError>()?
            .map_err(|_| MetadataReadError::Unavailable)?;
        let local_node = self.local_node.ok_or(MetadataReadError::Unavailable)?;
        if !is_sync_eligible(&config, local_node) {
            return self.ask_as_owner(&config, local_node);
        }
        let config_digest = config
            .digest()
            .map_err(|_| MetadataReadError::Unavailable)?;
        let placement = resolve_metadata_id(&config, self.realm_id, None, self.profile_id)
            .map_err(|_| MetadataReadError::Unavailable)?;
        let holders =
            read_holder_sets(&config, &placement).map_err(|_| MetadataReadError::Unavailable)?;
        self.holder_count = holders.len();
        self.message = Some(MetadataTransportMessage::ForwardExportProfile {
            config_digest,
            profile_id: self.profile_id,
            expected_revision: self.expected_revision,
        });
        let holds = holders.contains(&local_node);
        self.peers = holders
            .into_iter()
            .filter(|holder| *holder != local_node)
            .collect();
        if holds {
            self.stage = ExportStage::Local;
            return Ok(Next::Call(self.local()));
        }
        self.ask_peers()
    }

    /// A device never joins the validation channel: it asks a holder as its owner, so it learns
    /// nothing its owner may not read.
    fn ask_as_owner(&mut self, config: &RealmConfigDocument, local_node: NodeId) -> ExportTurn {
        let owner = crate::realm::mutate_placement::node_kind(config, local_node)
            .and_then(|kind| kind.owner())
            .ok_or(MetadataReadError::Unavailable)?;
        let auth = AuthContext {
            user_id: owner,
            realm_id: self.realm_id,
            path_restrictions: None,
            session: None,
        };
        let request = ExportMetadataRequest {
            document_id: self.profile_id,
            auth: Some(auth.clone()),
            view: RoCrateExportView::Raw,
            limit: None,
            offset: None,
            after: None,
        };
        let token = Some(AuthToken::internal(auth));
        self.stage = ExportStage::Owner;
        Ok(Next::Call(IoCall::child(RoutedExportOperation::new(
            Some(local_node),
            self.realm_id,
            request,
            token,
            u64::MAX,
        ))))
    }

    fn ask_peers(&mut self) -> ExportTurn {
        let Some(message) = self.message.clone() else {
            return Err(MetadataReadError::Unavailable);
        };
        if self.peers.is_empty() {
            return self.collect();
        }
        let requests = self
            .peers
            .iter()
            .map(|peer| (*peer, message.clone()))
            .collect();
        self.stage = ExportStage::Peers;
        let fanout = FanoutOperation::new(requests, READ_FANOUT_LIMIT, READ_PEER_TIMEOUT);
        Ok(Next::Call(IoCall::child(fanout)))
    }

    fn peers_read(&mut self, result: IoResult) -> ExportTurn {
        let answers = result
            .child::<Vec<PeerAnswer>, FetchError>()?
            .map_err(|_| MetadataReadError::Unavailable)?;
        for (peer, answer) in answers {
            self.answers.push((peer, export_answer(answer)));
        }
        self.collect()
    }

    fn collect(&mut self) -> ExportTurn {
        let answers = std::mem::take(&mut self.answers);
        collect_profile_export(answers, self.holder_count, false).map(Next::Done)
    }
}

/// One holder's export answer; anything but an export answer leaves the holder unavailable.
fn export_answer(
    answer: Result<MetadataTransportMessage, String>,
) -> Result<ExportMetadataResult, MetadataReadError> {
    match answer {
        Ok(MetadataTransportMessage::ExportAnswer { result }) => result.map(|export| *export),
        Ok(_) | Err(_) => Err(MetadataReadError::Unavailable),
    }
}

impl Operation for ProfileExportOperation {
    type Output = ExportMetadataResult;
    type Error = MetadataReadError;

    fn first(&mut self) -> ExportTurn {
        if self.local_node.is_none() {
            self.stage = ExportStage::Local;
            self.holder_count = 1;
            return Ok(Next::Call(self.local()));
        }
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> ExportTurn {
        match self.stage {
            ExportStage::Config => self.config_read(result),
            ExportStage::Local => {
                let local = result.child::<ExportMetadataResult, MetadataReadError>()?;
                match (self.local_node, self.message.is_some()) {
                    // Without a network this node's own copy is the whole answer.
                    (None, _) | (_, false) => local.map(Next::Done),
                    (Some(local_node), true) => {
                        self.answers.push((local_node, local));
                        self.ask_peers()
                    }
                }
            }
            ExportStage::Peers => self.peers_read(result),
            ExportStage::Owner => result
                .child::<ExportMetadataResult, MetadataApiError>()?
                .map_err(crate::forward::transport::read_error)
                .map(Next::Done),
        }
    }
}

#[cfg(test)]
#[path = "profile_export_tests.rs"]
mod tests;
