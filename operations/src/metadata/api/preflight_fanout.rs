//! Runs one reference preflight page across the realm: resolves its targets, asks every node
//! for the references it holds and merges their answers under a signed cursor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::cursor::SEARCH_CURSOR_CONTEXT;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::event;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use tracing::Span;

use super::distributed_types::record_preflight_node;
use super::fanout_types::{
    MetadataFanoutOperation, MetadataFanoutScope, RealmNodeDiscovery, forwarded_bearer,
};
use super::node_fanout::{
    FanoutParts, NodeCalls, NodeFanoutOperation, discovery_answer, discovery_call, peer_answer,
    peer_request,
};
use super::preflight::{
    PreflightCursorPlan, PreflightPlan, ResolvedPreflightTargets, assemble_preflight_execution,
    plan_preflight_request,
};
use super::preflight_local::references_preflight_local;
use super::preflight_targets::ResolveTargetsOperation;
use super::preflight_types::{
    MetadataResolvedTarget, ReferenceExecution, ReferenceNodeExecution, ReferenceNodeRequest,
    ReferenceRequest, preflight_fingerprint,
};
use super::search_fanout::{cursor_signers, resume_cursor};
use super::{
    ApiQueryMode, DISTRIBUTED_QUERY_DEADLINE, MAX_PAGINATION_DEPTH, MetadataApiError,
    MetadataReadError, map_read_error,
};
use crate::forward::transport::read_error;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::search_cursor::{SearchCursor, resume_fetch_limit};

/// How the preflight fanout asks each node, deeper for nodes a cursor resumes.
struct PreflightCalls {
    realm_id: RealmId,
    auth: AuthContext,
    auth_token: Option<AuthToken>,
    targets: Vec<MetadataResolvedTarget>,
    s3_endpoint: Option<String>,
    resume: HashMap<NodeId, u32>,
    page_size: usize,
}

impl PreflightCalls {
    fn request(&self, node: NodeId) -> ReferenceNodeRequest {
        let limit = resume_fetch_limit(&self.resume, node, self.page_size, MAX_PAGINATION_DEPTH);
        ReferenceNodeRequest {
            targets: self.targets.clone(),
            limit,
        }
    }
}

impl NodeCalls for PreflightCalls {
    type Part = ReferenceNodeExecution;

    fn local(&self, node: NodeId) -> Effect {
        let local = references_preflight_local(
            (self.realm_id, node),
            Some(self.auth.clone()),
            self.request(node),
            self.s3_endpoint.clone(),
        );
        Effect::SubOperation(boxed_suboperation(local, |result| {
            Event::Child(ChildOutcome::new(result))
        }))
    }

    fn remote(&self, node: NodeId, timeout: Duration) -> Effect {
        let message = MetadataTransportMessage::ReferencePreflight {
            auth_token: self.auth_token.clone(),
            request: Box::new(self.request(node)),
        };
        peer_request(node, &message, timeout)
    }

    fn read(
        &self,
        node: NodeId,
        local: bool,
        event: Event,
    ) -> Result<ReferenceNodeExecution, MetadataReadError> {
        if local {
            let Event::Child(outcome) = event else {
                return Err(MetadataReadError::Unavailable);
            };
            let part = outcome.take::<ReferenceNodeExecution, MetadataApiError>();
            return part
                .ok_or(MetadataReadError::Unavailable)?
                .map_err(read_error);
        }
        match peer_answer(node, event)? {
            MetadataTransportMessage::ReferencePreflightResults { result } => {
                result.map(|part| *part)
            }
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    fn record(&self, span: &Span, result: &Result<ReferenceNodeExecution, MetadataReadError>) {
        record_preflight_node(span, result);
    }
}

/// A page waiting for its cursor signature.
#[derive(Debug, PartialEq)]
struct Unsigned {
    execution: ReferenceExecution,
    cursor: SearchCursor,
}

enum PreflightStage {
    Start,
    Targets,
    Discovery(ResolvedPreflightTargets),
    Fanout(ResolvedPreflightTargets, PreflightCursorPlan),
    Sign(Box<Unsigned>),
}

/// One page of a reference preflight across the realm, or on this node in local mode.
pub struct ReferencesPreflightOperation {
    realm_id: RealmId,
    local_node: NodeId,
    request: Option<ReferenceRequest>,
    plan: Option<PreflightPlan>,
    deadline_ms: Option<u64>,
    now_ms: u64,
    stage: PreflightStage,
}

/// The preflight a caller asked for.
pub fn references_preflight(
    realm_id: RealmId,
    local_node: NodeId,
    request: ReferenceRequest,
) -> ReferencesPreflightOperation {
    ReferencesPreflightOperation {
        realm_id,
        local_node,
        request: Some(request),
        plan: None,
        deadline_ms: None,
        now_ms: 0,
        stage: PreflightStage::Start,
    }
}

type PreflightTurn = Result<Next<ReferenceExecution>, MetadataApiError>;

impl ReferencesPreflightOperation {
    fn plan(&self) -> Result<&PreflightPlan, MetadataApiError> {
        (self.plan.as_ref())
            .ok_or_else(|| MetadataApiError::Internal("preflight not planned".to_string()))
    }

    fn deadline(&self) -> u64 {
        self.deadline_ms.unwrap_or(self.now_ms)
    }

    fn mode(&self) -> Result<ApiQueryMode, MetadataApiError> {
        Ok(self.plan()?.mode.unwrap_or(ApiQueryMode::Distributed))
    }

    /// Reads the cursor the plan carries, if any, then asks the nodes.
    fn resolved(&mut self, resolved: ResolvedPreflightTargets) -> PreflightTurn {
        let plan = self.plan()?;
        let fingerprint = preflight_fingerprint(&resolved.targets, plan.mode);
        if plan.cursor.is_none() {
            let cursor = PreflightCursorPlan {
                fingerprint,
                watermark: None,
                resume: HashMap::new(),
                target_nodes: plan.target_nodes.clone(),
                discovery_failed: false,
            };
            return self.fanout(resolved, cursor);
        }
        // Check the full realm because capped fan-out may omit the cursor's signer.
        let mut signers = match (self.mode()?, plan.target_nodes.clone()) {
            (ApiQueryMode::Local, _) => {
                return self.resumed(resolved, vec![self.local_node], None);
            }
            (ApiQueryMode::Distributed, Some(nodes)) => nodes,
            (ApiQueryMode::Distributed, None) => {
                let wait = Duration::from_millis(self.deadline().saturating_sub(self.now_ms));
                self.stage = PreflightStage::Discovery(resolved);
                return Ok(Next::Call(discovery_call(
                    self.realm_id,
                    self.local_node,
                    wait,
                )));
            }
        };
        signers.push(self.local_node);
        self.resumed(resolved, signers, None)
    }

    fn resumed(
        &mut self,
        resolved: ResolvedPreflightTargets,
        signers: Vec<NodeId>,
        discovery: Option<RealmNodeDiscovery>,
    ) -> PreflightTurn {
        let plan = self.plan()?;
        let fingerprint = preflight_fingerprint(&resolved.targets, plan.mode);
        let raw = plan.cursor.as_deref().unwrap_or_default();
        let asked = (plan.target_nodes.as_deref(), self.mode()?, self.local_node);
        let resumed = resume_cursor((raw, &signers), fingerprint, asked, discovery)?;
        let cursor = PreflightCursorPlan {
            fingerprint,
            watermark: Some(resumed.watermark),
            resume: resumed.resume,
            target_nodes: Some(resumed.nodes),
            discovery_failed: resumed.discovery_failed,
        };
        self.fanout(resolved, cursor)
    }

    fn fanout(
        &mut self,
        resolved: ResolvedPreflightTargets,
        cursor: PreflightCursorPlan,
    ) -> PreflightTurn {
        let plan = self.plan()?;
        let auth_token = forwarded_bearer(plan.bearer_token.as_deref())?
            .or_else(|| Some(AuthToken::internal(plan.auth.clone())));
        let calls = PreflightCalls {
            realm_id: self.realm_id,
            auth: plan.auth.clone(),
            auth_token,
            targets: resolved.targets.clone(),
            s3_endpoint: plan.s3_endpoint.clone(),
            resume: cursor.resume.clone(),
            page_size: plan.page_size,
        };
        let scope =
            MetadataFanoutScope::new(plan.mode, cursor.target_nodes.clone(), plan.allow_partial)
                .with_subject(cursor.fingerprint)
                .with_discovery_failed(cursor.discovery_failed);
        let fanout = NodeFanoutOperation::new(
            (self.realm_id, self.local_node),
            scope,
            MetadataFanoutOperation::ReferencePreflight,
            calls,
            map_read_error,
        )
        .until(self.deadline());
        self.stage = PreflightStage::Fanout(resolved, cursor);
        Ok(Next::Call(IoCall::child(fanout)))
    }
}

impl Operation for ReferencesPreflightOperation {
    type Output = ReferenceExecution;
    type Error = MetadataApiError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
        let budget = DISTRIBUTED_QUERY_DEADLINE.as_millis() as u64;
        self.deadline_ms
            .get_or_insert(now_ms.saturating_add(budget));
    }

    fn first(&mut self) -> PreflightTurn {
        let request = (self.request.take())
            .ok_or_else(|| MetadataApiError::Internal("preflight already started".to_string()))?;
        let (plan, target) = plan_preflight_request(self.realm_id, request)?;
        let node = (self.realm_id, self.local_node);
        let endpoint = plan.s3_endpoint.clone();
        let resolve = ResolveTargetsOperation::new(node, plan.auth.clone(), target, endpoint);
        self.plan = Some(plan);
        self.stage = PreflightStage::Targets;
        Ok(Next::Call(IoCall::child(resolve)))
    }

    fn next(&mut self, result: IoResult) -> PreflightTurn {
        match std::mem::replace(&mut self.stage, PreflightStage::Start) {
            PreflightStage::Start => Err(MetadataApiError::Internal(
                "preflight not started".to_string(),
            )),
            PreflightStage::Targets => {
                let resolved = result.child::<ResolvedPreflightTargets, MetadataApiError>()??;
                self.resolved(resolved)
            }
            PreflightStage::Discovery(resolved) => {
                let discovery = discovery_answer(result, self.local_node);
                let fingerprint = preflight_fingerprint(&resolved.targets, self.plan()?.mode);
                let (signers, discovery) = cursor_signers(discovery, self.local_node, &fingerprint);
                self.resumed(resolved, signers, Some(discovery))
            }
            PreflightStage::Fanout(resolved, cursor) => {
                let (parts, stats) =
                    result.child::<FanoutParts<ReferenceNodeExecution>, MetadataApiError>()??;
                let plan = self.plan()?;
                let (execution, next) = assemble_preflight_execution(
                    self.local_node,
                    resolved,
                    plan,
                    cursor,
                    parts,
                    stats,
                )?;
                let Some(cursor) = next else {
                    return Ok(Next::Done(execution));
                };
                let bytes = (cursor.signing_bytes(SEARCH_CURSOR_CONTEXT))
                    .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
                self.stage = PreflightStage::Sign(Box::new(Unsigned { execution, cursor }));
                let sign = Effect::Net(NetEffect::CursorSign(bytes));
                Ok(Next::Call(IoCall::Effect(Box::new(sign))))
            }
            PreflightStage::Sign(unsigned) => {
                let Ok(Event::Net(NetEvent::CursorSigned(Some(signature)))) = event(result) else {
                    return Err(MetadataApiError::Internal(
                        "net handle unavailable for preflight cursor signing".to_string(),
                    ));
                };
                let Unsigned {
                    mut execution,
                    mut cursor,
                } = *unsigned;
                cursor.signature = signature;
                let encoded = (cursor.encode())
                    .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
                execution.next_cursor = Some(encoded);
                Ok(Next::Done(execution))
            }
        }
    }
}

#[cfg(test)]
#[path = "preflight_fanout_tests.rs"]
mod call_tests;
