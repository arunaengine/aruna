//! Resolves metadata paths by picking holder nodes, forwarding lookups and merging replies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::ChildOutcome;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::operation::{Operation, boxed_suboperation};

use super::super::forward::holder_reads::{AuthFailure, ReadDecision, reduce_holder_reads};
use super::list::{CandidateScan, CandidatesOperation};
use super::path_types::select_path_holders;
use super::path_types::{
    MetadataLookupRequest, MetadataLookupResult, PathHolderSelection, PathShardView,
};
use super::path_types::{merge_path_views, reduce_path_candidates};
use super::read_access::{ReadAccess, ReadAccessOperation};
use super::{
    AuthContext, AuthToken, DISTRIBUTED_QUERY_DEADLINE, GroupId, HashSet, MetaResourceId,
    MetadataApiError, MetadataPathCandidate, MetadataPathResolution, MetadataReadError,
    MetadataRegistryRecord, MetadataTransportMessage, NodeId, PathClaimRecord, PlacementRef,
    QUERY_FANOUT_LIMIT, QUERY_MAX_NODES, REGISTRY_CANDIDATE_LIMIT, ROLE_NODE, RealmConfigDocument,
    RealmId, holds_placement, meta_bucket_subject, neg_log2_q48, registry_placement,
    registry_placement_for, registry_strategy, selector_hash,
};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::fetch::{FanoutOperation, FetchError, PeerAnswer, read_answer};
use crate::metadata::protocol::encode_message;

use aruna_core::StructuredId;

pub(super) fn select_forward_peers(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: GroupId,
    normalized: &str,
    local_node: NodeId,
) -> Result<Vec<NodeId>, MetadataApiError> {
    let mut subject = meta_bucket_subject(realm_id, group_id, normalized);
    subject.extend_from_slice(local_node.as_bytes());
    let mut ranked = Vec::with_capacity(QUERY_MAX_NODES);
    for node in config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
    {
        let Ok(peer) = node.node_id.parse::<NodeId>() else {
            continue;
        };
        if peer == local_node || ranked.iter().any(|(candidate, _)| *candidate == peer) {
            continue;
        }
        let score = neg_log2_q48(selector_hash(ROLE_NODE, &subject, peer.as_bytes()));
        if ranked.len() < QUERY_MAX_NODES {
            ranked.push((peer, score));
            continue;
        }
        let Some((worst_index, (worst_peer, worst_score))) =
            ranked.iter().enumerate().max_by(|(_, left), (_, right)| {
                left.1
                    .cmp(&right.1)
                    .then_with(|| left.0.as_bytes().cmp(right.0.as_bytes()))
            })
        else {
            continue;
        };
        if score < *worst_score
            || (score == *worst_score && peer.as_bytes() < worst_peer.as_bytes())
        {
            ranked[worst_index] = (peer, score);
        }
    }
    ranked.sort_unstable_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| left.0.as_bytes().cmp(right.0.as_bytes()))
    });
    Ok(ranked.into_iter().map(|(peer, _)| peer).collect())
}

pub(super) fn reduce_path_response(
    success: Option<MetadataLookupResult>,
    auth_error: Option<MetadataReadError>,
    divergent: bool,
    not_found: bool,
    unavailable: bool,
) -> Result<MetadataLookupResult, MetadataApiError> {
    let conflict = divergent || (success.is_some() && (not_found || unavailable));
    match reduce_holder_reads(
        success,
        auth_error,
        not_found,
        conflict,
        unavailable,
        AuthFailure::Fatal,
    ) {
        ReadDecision::Success(result) => Ok(result),
        ReadDecision::NotFound => Err(MetadataApiError::NotFound),
        ReadDecision::Auth(MetadataReadError::Unauthorized) => Err(MetadataApiError::Unauthorized),
        ReadDecision::Auth(MetadataReadError::Forbidden) => Err(MetadataApiError::Forbidden),
        ReadDecision::Auth(_) | ReadDecision::Unavailable => {
            Err(MetadataApiError::ServiceUnavailable)
        }
    }
}

pub(super) fn validate_path_resolution(
    realm_id: RealmId,
    group_id: GroupId,
    document_path: &str,
    resolution: &MetadataPathResolution,
) -> Result<(), MetadataApiError> {
    let normalized = MetadataRegistryRecord::normalize_document_path(document_path);
    let winner_id = MetaResourceId::from_bytes(resolution.winner.document_id.to_bytes())
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    if resolution.winner.realm_id != realm_id
        || resolution.winner.group_id != group_id
        || resolution.winner.document_path != normalized
        || resolution.winner.graph_iri
            != MetadataRegistryRecord::graph_iri_for(resolution.winner.document_id)
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let mut seen = HashSet::from([winner_id]);
    for conflict in &resolution.conflicts {
        let conflict = MetaResourceId::from_bytes(conflict.to_bytes())
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        if !seen.insert(conflict) {
            return Err(MetadataApiError::ServiceUnavailable);
        }
    }
    Ok(())
}

pub(super) fn validate_path_candidate(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: GroupId,
    document_path: &str,
    candidate: &MetadataPathCandidate,
) -> Result<PlacementRef, MetadataApiError> {
    if candidate.claim.requested_path != document_path {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let placement = registry_placement_for(config, group_id, candidate.claim.document_id.as_ulid());
    if let Some(record) = candidate.record.as_ref()
        && (record.realm_id != realm_id
            || record.group_id != group_id
            || record.document_id != candidate.claim.document_id.as_ulid()
            || record.establishing_event_id != candidate.claim.establishing_event_id
            || record.document_path != document_path
            || record.graph_iri != MetadataRegistryRecord::graph_iri_for(record.document_id)
            || record.permission_path
                != MetadataRegistryRecord::permission_path_for(
                    &realm_id,
                    group_id,
                    document_path,
                    record.document_id,
                )
            || registry_placement(config, record) != placement)
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(placement)
}

type Candidates = Vec<MetadataPathCandidate>;

#[derive(Debug, PartialEq)]
enum LocalStage {
    Claims,
    Config,
    Access,
}

/// The claims of one group path this node holds, in claim order, each with its record when the
/// caller may read it. With `local_node`, only claims whose registry placement this node holds
/// count.
#[derive(Debug, PartialEq)]
pub(crate) struct LocalPathOperation {
    realm_id: RealmId,
    group_id: GroupId,
    document_path: String,
    auth: Option<AuthContext>,
    local_node: Option<NodeId>,
    stage: LocalStage,
    records: Vec<MetadataRegistryRecord>,
}

impl LocalPathOperation {
    pub(crate) fn new(
        realm_id: RealmId,
        (group_id, document_path): (GroupId, &str),
        auth: Option<AuthContext>,
        local_node: Option<NodeId>,
    ) -> Self {
        Self {
            realm_id,
            group_id,
            document_path: document_path.to_string(),
            auth,
            local_node,
            stage: LocalStage::Claims,
            records: Vec::new(),
        }
    }

    fn access(&mut self) -> Result<Next<Candidates>, MetadataApiError> {
        self.stage = LocalStage::Access;
        let access = ReadAccessOperation::new(self.realm_id, self.auth.clone(), &self.records);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn candidates(&mut self, access: &ReadAccess) -> Result<Candidates, MetadataApiError> {
        let mut candidates = std::mem::take(&mut self.records)
            .into_iter()
            .map(|record| {
                let document_id = MetaResourceId::from_bytes(record.document_id.to_bytes())
                    .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
                let claim = PathClaimRecord {
                    document_id,
                    establishing_event_id: record.establishing_event_id,
                    requested_path: record.document_path.clone(),
                };
                let record = access.shows(&record).then_some(record);
                Ok(MetadataPathCandidate { claim, record })
            })
            .collect::<Result<Candidates, MetadataApiError>>()?;
        candidates.sort_by_key(|candidate| {
            let claim = &candidate.claim;
            (claim.document_id, claim.establishing_event_id)
        });
        Ok(candidates)
    }
}

impl Operation for LocalPathOperation {
    type Output = Candidates;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<Candidates>, MetadataApiError> {
        let scan = CandidateScan::Claims {
            realm_id: self.realm_id,
        };
        let claims = CandidatesOperation::new(Some(self.group_id), scan);
        Ok(Next::Call(IoCall::child(claims)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Candidates>, MetadataApiError> {
        match self.stage {
            LocalStage::Claims => {
                let mut records =
                    result.child::<Vec<MetadataRegistryRecord>, MetadataApiError>()??;
                records.retain(|record| record.document_path == self.document_path);
                self.records = records;
                if self.local_node.is_none() {
                    return self.access();
                }
                self.stage = LocalStage::Config;
                let config = GetConfigOperation::new(self.realm_id);
                Ok(Next::Call(IoCall::child(config)))
            }
            LocalStage::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|_| MetadataApiError::ServiceUnavailable)?;
                registry_strategy(&config).ok_or(MetadataApiError::ServiceUnavailable)?;
                if let Some(node) = self.local_node {
                    self.records.retain(|record| {
                        holds_placement(&config, &registry_placement(&config, record), node)
                    });
                }
                self.access()
            }
            LocalStage::Access => {
                let access = result.child::<ReadAccess, MetadataApiError>()??;
                Ok(Next::Done(self.candidates(&access)?))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum LookupStage {
    Local,
    Config,
    Forward,
    Holders,
}

/// Resolves a group path to its winning document under one deadline. Without a local node, or
/// for a local lookup, this node's claims decide. A node outside the sync set forwards the
/// resolution to ranked peers; a sync node asks the holders of every shard the path may sit on,
/// itself included, and merges their claims.
#[derive(Debug, PartialEq)]
pub struct LookupPathOperation {
    realm_id: RealmId,
    request: MetadataLookupRequest,
    auth_token: Option<AuthToken>,
    local_node: Option<NodeId>,
    local_only: bool,
    normalized: String,
    started_ms: Option<u64>,
    now_ms: u64,
    stage: LookupStage,
    config: Option<RealmConfigDocument>,
    holders: Vec<PathHolderSelection>,
    replica_counts: Vec<usize>,
}

impl LookupPathOperation {
    pub fn new(
        realm_id: RealmId,
        request: MetadataLookupRequest,
        auth_token: Option<AuthToken>,
        local_node: Option<NodeId>,
    ) -> Self {
        let normalized = MetadataRegistryRecord::normalize_document_path(&request.document_path);
        Self {
            realm_id,
            request,
            auth_token,
            local_node,
            local_only: false,
            normalized,
            started_ms: None,
            now_ms: 0,
            stage: LookupStage::Local,
            config: None,
            holders: Vec::new(),
            replica_counts: Vec::new(),
        }
    }

    /// Resolves from this node's claims only, as a holder answers a forwarded resolution.
    pub(crate) fn local(
        realm_id: RealmId,
        request: MetadataLookupRequest,
        local_node: Option<NodeId>,
    ) -> Self {
        Self {
            local_only: true,
            ..Self::new(realm_id, request, None, local_node)
        }
    }

    fn remaining(&self) -> Duration {
        let spent = self
            .now_ms
            .saturating_sub(self.started_ms.unwrap_or(self.now_ms));
        DISTRIBUTED_QUERY_DEADLINE.saturating_sub(Duration::from_millis(spent))
    }

    fn local_path(&self) -> LocalPathOperation {
        let path = (self.request.group_id, self.normalized.as_str());
        let auth = self.request.auth.clone();
        LocalPathOperation::new(self.realm_id, path, auth, self.local_node)
    }

    fn configured(
        &mut self,
        config: RealmConfigDocument,
        local_node: NodeId,
    ) -> Result<Next<MetadataLookupResult>, MetadataApiError> {
        let unavailable = |_| MetadataApiError::ServiceUnavailable;
        let config_digest = config.digest().map_err(unavailable)?;
        let trusted_origin = (config.nodes.iter())
            .any(|node| node.node_id == local_node.to_string() && node.kind.is_sync_eligible());
        if !trusted_origin {
            return self.forward(&config, local_node, config_digest);
        }
        let strategy = registry_strategy(&config).ok_or(MetadataApiError::ServiceUnavailable)?;
        if strategy.shard_count == 0 {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        let auth_token = (self.auth_token.clone())
            .or_else(|| self.request.auth.clone().map(AuthToken::internal));
        let (holders, replica_counts) = select_path_holders(
            &config,
            self.realm_id,
            self.request.group_id,
            &self.normalized,
            strategy.strategy_id,
            strategy.shard_count,
            strategy.replica_count,
            local_node,
        )?;
        let timeout = self.remaining();
        let mut members = Vec::with_capacity(holders.len());
        for selection in &holders {
            if selection.node_id == local_node {
                members.push(Effect::SubOperation(boxed_suboperation(
                    self.local_path(),
                    |result| Event::Child(ChildOutcome::new(result)),
                )));
                continue;
            }
            let message = MetadataTransportMessage::ForwardPathLookup {
                auth_token: auth_token.clone(),
                group_id: self.request.group_id,
                document_path: self.normalized.clone(),
                config_digest,
            };
            let frame = encode_message(&message).map_err(MetadataApiError::Internal)?;
            let request = MetadataRequestEffect {
                peer: selection.node_id,
                frame,
                timeout,
            };
            members.push(Effect::Net(NetEffect::MetadataRequest(Box::new(request))));
        }
        self.config = Some(config);
        self.holders = holders;
        self.replica_counts = replica_counts;
        self.stage = LookupStage::Holders;
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: QUERY_FANOUT_LIMIT,
            form: GroupForm::All {
                deadline: Some(timeout),
            },
        })))
    }

    fn forward(
        &mut self,
        config: &RealmConfigDocument,
        local_node: NodeId,
        config_digest: [u8; 32],
    ) -> Result<Next<MetadataLookupResult>, MetadataApiError> {
        if self.request.auth.is_some() && self.auth_token.is_none() {
            return Err(MetadataApiError::Unauthorized);
        }
        let group_id = self.request.group_id;
        let peers = select_forward_peers(
            config,
            self.realm_id,
            group_id,
            &self.normalized,
            local_node,
        )?;
        if peers.is_empty() {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        let requests = (peers.into_iter())
            .map(|peer| {
                let message = MetadataTransportMessage::ForwardPathResolution {
                    auth_token: self.auth_token.clone(),
                    group_id,
                    document_path: self.request.document_path.clone(),
                    config_digest,
                };
                (peer, message)
            })
            .collect();
        self.stage = LookupStage::Forward;
        let fanout = FanoutOperation::new(requests, QUERY_FANOUT_LIMIT, self.remaining());
        Ok(Next::Call(IoCall::child(fanout)))
    }

    fn forwarded(
        &self,
        answers: Vec<PeerAnswer>,
    ) -> Result<MetadataLookupResult, MetadataApiError> {
        let mut auth_error = None;
        let mut divergent = false;
        let mut not_found = false;
        let mut unavailable = false;
        let mut success: Option<MetadataLookupResult> = None;
        for (_, answer) in answers {
            match answer {
                Ok(MetadataTransportMessage::ForwardedPathResolution { result: Ok(result) }) => {
                    let (group_id, path) = (self.request.group_id, &self.request.document_path);
                    if validate_path_resolution(self.realm_id, group_id, path, &result).is_err() {
                        unavailable = true;
                        continue;
                    }
                    let candidate = MetadataLookupResult {
                        winner: result.winner,
                        conflicts: result.conflicts,
                    };
                    if success.as_ref().is_some_and(|current| {
                        current.winner != candidate.winner
                            || current.conflicts != candidate.conflicts
                    }) {
                        divergent = true;
                    } else {
                        success.get_or_insert(candidate);
                    }
                }
                Ok(MetadataTransportMessage::ForwardedPathResolution {
                    result:
                        Err(
                            error
                            @ (MetadataReadError::Unauthorized | MetadataReadError::Forbidden),
                        ),
                }) => {
                    auth_error.get_or_insert(error);
                }
                Ok(MetadataTransportMessage::ForwardedPathResolution {
                    result: Err(MetadataReadError::NotFound),
                }) => not_found = true,
                _ => unavailable = true,
            }
        }
        reduce_path_response(success, auth_error, divergent, not_found, unavailable)
    }

    fn holder_answer(
        selection: &PathHolderSelection,
        member: GroupResult,
    ) -> Result<Candidates, MetadataApiError> {
        let local = matches!(&member, GroupResult::Done(event) if matches!(event.as_ref(), Event::Child(_)));
        if local {
            let GroupResult::Done(event) = member else {
                return Err(MetadataApiError::ServiceUnavailable);
            };
            let Event::Child(outcome) = *event else {
                return Err(MetadataApiError::ServiceUnavailable);
            };
            return (outcome.take::<Candidates, MetadataApiError>())
                .ok_or(MetadataApiError::ServiceUnavailable)?;
        }
        match read_answer(selection.node_id, Some(member)) {
            Ok(MetadataTransportMessage::ForwardedPathLookup { result: Ok(result) }) => Ok(result),
            Ok(MetadataTransportMessage::ForwardedPathLookup {
                result: Err(MetadataReadError::Unauthorized),
            }) => Err(MetadataApiError::Unauthorized),
            Ok(MetadataTransportMessage::ForwardedPathLookup {
                result: Err(MetadataReadError::Forbidden),
            }) => Err(MetadataApiError::Forbidden),
            _ => Err(MetadataApiError::ServiceUnavailable),
        }
    }

    fn merged(
        &mut self,
        results: Vec<GroupResult>,
    ) -> Result<MetadataLookupResult, MetadataApiError> {
        let config = self
            .config
            .take()
            .ok_or(MetadataApiError::ServiceUnavailable)?;
        let (realm_id, group_id) = (self.realm_id, self.request.group_id);
        let mut views = Vec::new();
        let mut auth_error = None;
        let mut failed = false;
        let mut only_auth = true;
        let mut candidate_count = 0usize;
        let mut results = results.into_iter();
        for selection in std::mem::take(&mut self.holders) {
            let member = results.next().unwrap_or(GroupResult::Refused);
            match Self::holder_answer(&selection, member) {
                Ok(returned) => {
                    candidate_count = candidate_count.saturating_add(returned.len());
                    if candidate_count > REGISTRY_CANDIDATE_LIMIT {
                        return Err(MetadataApiError::ServiceUnavailable);
                    }
                    only_auth = false;
                    let shards = selection.shards;
                    let mut partitions = shards.iter().map(|_| Vec::new()).collect::<Vec<_>>();
                    for candidate in returned {
                        let path = &self.normalized;
                        let placement =
                            validate_path_candidate(&config, realm_id, group_id, path, &candidate)?;
                        let index = (shards.binary_search(&placement.shard))
                            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
                        partitions[index].push(candidate);
                    }
                    views.extend(
                        (shards.into_iter().zip(partitions))
                            .map(|(shard, candidates)| PathShardView { shard, candidates }),
                    );
                }
                Err(error @ (MetadataApiError::Unauthorized | MetadataApiError::Forbidden)) => {
                    failed = true;
                    auth_error.get_or_insert(error);
                }
                Err(_) => {
                    failed = true;
                    only_auth = false;
                }
            }
        }
        if failed {
            if only_auth && let Some(error) = auth_error {
                return Err(error);
            }
            return Err(MetadataApiError::ServiceUnavailable);
        }
        let candidates = merge_path_views(&self.replica_counts, views)?;
        reduce_path_candidates(candidates)
    }
}

impl Operation for LookupPathOperation {
    type Output = MetadataLookupResult;
    type Error = MetadataApiError;

    fn stamp(&mut self, now_ms: u64) {
        self.started_ms.get_or_insert(now_ms);
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<MetadataLookupResult>, MetadataApiError> {
        if self.normalized.is_empty() {
            return Err(MetadataApiError::BadRequest);
        }
        if self.local_only || self.local_node.is_none() {
            self.stage = LookupStage::Local;
            return Ok(Next::Call(IoCall::child(self.local_path())));
        }
        self.stage = LookupStage::Config;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MetadataLookupResult>, MetadataApiError> {
        match self.stage {
            LookupStage::Local => {
                let candidates = result.child::<Candidates, MetadataApiError>()??;
                Ok(Next::Done(reduce_path_candidates(candidates)?))
            }
            LookupStage::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|_| MetadataApiError::ServiceUnavailable)?;
                let local_node = self
                    .local_node
                    .ok_or(MetadataApiError::ServiceUnavailable)?;
                self.configured(config, local_node)
            }
            LookupStage::Forward => {
                let answers = result.child::<Vec<PeerAnswer>, FetchError>()?;
                let answers = answers.map_err(|_| MetadataApiError::ServiceUnavailable)?;
                Ok(Next::Done(self.forwarded(answers)?))
            }
            LookupStage::Holders => {
                let IoResult::Group(GroupEvent::Finished { results }) = result else {
                    return Err(MetadataApiError::ServiceUnavailable);
                };
                Ok(Next::Done(self.merged(results)?))
            }
        }
    }
}

#[cfg(test)]
#[path = "path_tests.rs"]
mod call_tests;
