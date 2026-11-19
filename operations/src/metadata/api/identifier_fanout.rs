//! Asks every realm node's reverse index for one external identifier, since each node indexes
//! only the mappings it holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use tracing::Span;

use super::fanout_types::{MetadataFanoutOperation, MetadataFanoutScope, fanout_bearer};
use super::node_fanout::{
    FanoutParts, NodeCalls, NodeFanoutOperation, child_part, peer_answer, peer_request,
};
use super::{ApiQueryMode, MetadataApiError, MetadataReadError, map_read_error};
use crate::metadata::identifier_lookup::{IdentifierLookupOperation, IdentifierQuery};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::secondary_ids::{IdentifierMatch, merge_matches};

type Matches = Vec<IdentifierMatch>;

/// How the lookup asks each node's reverse index.
#[derive(Debug, PartialEq)]
struct LookupCalls {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    auth_token: Option<AuthToken>,
    query: IdentifierQuery,
}

impl NodeCalls for LookupCalls {
    type Part = Matches;

    fn local(&self, _node: NodeId) -> Effect {
        let lookup =
            IdentifierLookupOperation::new(self.realm_id, self.auth.clone(), self.query.clone());
        Effect::SubOperation(boxed_suboperation(lookup, |result| {
            Event::Child(ChildOutcome::new(result))
        }))
    }

    fn remote(&self, node: NodeId, timeout: Duration) -> Effect {
        let message = MetadataTransportMessage::LookupIdentifier {
            auth_token: self.auth_token.clone(),
            kind: self.query.kind,
            value: self.query.value.clone(),
            endpoint: self.query.endpoint.clone(),
        };
        peer_request(node, &message, timeout)
    }

    fn read(&self, node: NodeId, local: bool, event: Event) -> Result<Matches, MetadataReadError> {
        if local {
            return child_part::<Matches, MetadataApiError>(event);
        }
        match peer_answer(node, event)? {
            MetadataTransportMessage::IdentifierMatches { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    fn record(&self, span: &Span, result: &Result<Matches, MetadataReadError>) {
        match result {
            Ok(matches) => {
                span.record("result", "ok");
                span.record("hit_count", matches.len() as u64);
            }
            Err(_) => {
                span.record("result", "error");
            }
        }
    }
}

/// The merged readable matches of every node, and whether any node failed to answer.
#[derive(Debug, PartialEq)]
pub struct IdentifierFanoutOperation {
    realm_id: RealmId,
    local_node: NodeId,
    calls: Option<LookupCalls>,
}

/// The lookup of `query` across the realm for a caller with `auth` here and `bearer` to forward.
pub fn lookup_identifier_distributed(
    (realm_id, local_node): (RealmId, NodeId),
    (auth, bearer): (Option<AuthContext>, Option<String>),
    query: IdentifierQuery,
) -> IdentifierFanoutOperation {
    let calls = LookupCalls {
        realm_id,
        auth,
        auth_token: fanout_bearer(bearer.as_deref()),
        query,
    };
    IdentifierFanoutOperation {
        realm_id,
        local_node,
        calls: Some(calls),
    }
}

impl Operation for IdentifierFanoutOperation {
    type Output = (Matches, bool);
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<(Matches, bool)>, MetadataApiError> {
        let calls = (self.calls.take())
            .ok_or_else(|| MetadataApiError::Internal("lookup already started".to_string()))?;
        let scope = MetadataFanoutScope::new(Some(ApiQueryMode::Distributed), None, true);
        let fanout = NodeFanoutOperation::new(
            (self.realm_id, self.local_node),
            scope,
            MetadataFanoutOperation::IdentifierLookup,
            calls,
            map_read_error,
        );
        Ok(Next::Call(IoCall::child(fanout)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<(Matches, bool)>, MetadataApiError> {
        let (parts, stats) = result.child::<FanoutParts<Matches>, MetadataApiError>()??;
        let matches = merge_matches(parts.into_iter().flat_map(|(_, matches)| matches));
        Ok(Next::Done((
            matches,
            stats.nodes_failed > 0 || stats.discovery_failed,
        )))
    }
}

#[cfg(test)]
#[path = "identifier_fanout_tests.rs"]
mod call_tests;
