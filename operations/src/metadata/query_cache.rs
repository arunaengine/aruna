//! Derives the query cache keys that keep each caller scope's results apart.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;

const TAG_EAGER: u8 = 1;
const TAG_LAZY: u8 = 2;
const TAG_FANOUT: u8 = 3;
const NO_CREDENTIAL: &[u8] = b"aruna.metadata.query.anonymous";

// Length prefixes keep concatenated components unambiguous, so no two
// different component lists can hash to the same key.
fn push_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

/// Incremental digest over the graph IRIs a query is allowed to evaluate.
#[derive(Default)]
pub(super) struct ScopeDigest(blake3::Hasher);

impl ScopeDigest {
    pub(super) fn push(&mut self, graph_iri: &str) {
        push_bytes(&mut self.0, graph_iri.as_bytes());
    }

    pub(super) fn finish(self) -> [u8; 32] {
        *self.0.finalize().as_bytes()
    }
}

/// Digest of an unordered graph set.
pub(super) fn graphs_digest(graph_iris: &[String]) -> [u8; 32] {
    let mut graphs = graph_iris.iter().map(String::as_str).collect::<Vec<_>>();
    graphs.sort_unstable();
    graphs.dedup();
    let mut digest = ScopeDigest::default();
    for graph in graphs {
        digest.push(graph);
    }
    digest.finish()
}

/// Anonymous queries use a fixed digest; authenticated queries bypass caching.
pub(super) fn credential_digest(
    auth: Option<&AuthContext>,
    bearer_token: Option<&str>,
) -> Option<[u8; 32]> {
    if auth.is_some() || bearer_token.is_some() {
        return None;
    }
    let mut hasher = blake3::Hasher::new();
    push_bytes(&mut hasher, NO_CREDENTIAL);
    push_bytes(&mut hasher, NO_CREDENTIAL);
    Some(*hasher.finalize().as_bytes())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct QueryCacheKey([u8; 32]);

impl QueryCacheKey {
    /// The key as an effect carries it.
    pub(super) fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy)]
pub(super) enum LocalScopeKind {
    /// Caller-supplied graph filter: exactly the named graphs are evaluated.
    Eager,
    /// No graph filter: the digest covers the resolved visible graph set.
    Lazy,
}

impl LocalScopeKind {
    fn tag(self) -> u8 {
        match self {
            Self::Eager => TAG_EAGER,
            Self::Lazy => TAG_LAZY,
        }
    }
}

/// Key for a locally evaluated query: an entry is shared only between callers
/// whose authorization resolved to the identical graph set, so a hit can never
/// expose a graph the caller could not have evaluated itself.
pub(super) fn local_key(kind: LocalScopeKind, scope: &[u8; 32], sparql: &str) -> QueryCacheKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[kind.tag()]);
    push_bytes(&mut hasher, scope);
    push_bytes(&mut hasher, sparql.as_bytes());
    QueryCacheKey(*hasher.finalize().as_bytes())
}

pub(super) struct RemoteKeyInput<'a> {
    pub(super) distributed: bool,
    /// Realm the request resolved against; the credential digest alone cannot
    /// separate realms, so it must not depend on one process serving one realm.
    pub(super) realm_id: RealmId,
    pub(super) credential: &'a [u8; 32],
    pub(super) graph_iris: Option<&'a [String]>,
    pub(super) sparql: &'a str,
    pub(super) allow_partial: bool,
    pub(super) target_nodes: Option<&'a [NodeId]>,
}

/// Key for a merged fan-out result. Remote partitions authorize on the
/// forwarded credential, so entries are partitioned by realm and credential
/// digest and never shared across callers.
pub(super) fn remote_key(input: &RemoteKeyInput<'_>) -> QueryCacheKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[
        TAG_FANOUT,
        u8::from(input.distributed),
        u8::from(input.allow_partial),
    ]);
    push_bytes(&mut hasher, input.realm_id.as_bytes());
    push_bytes(&mut hasher, input.credential);
    match input.graph_iris {
        Some(graph_iris) => {
            hasher.update(&[1u8]);
            push_bytes(&mut hasher, &graphs_digest(graph_iris));
        }
        None => {
            hasher.update(&[0u8]);
        }
    }
    push_bytes(&mut hasher, input.sparql.as_bytes());
    match input.target_nodes {
        Some(nodes) => {
            hasher.update(&[1u8]);
            let mut nodes = nodes.iter().map(NodeId::as_bytes).collect::<Vec<_>>();
            nodes.sort_unstable();
            nodes.dedup();
            hasher.update(&(nodes.len() as u64).to_le_bytes());
            for node in nodes {
                push_bytes(&mut hasher, node);
            }
        }
        None => {
            hasher.update(&[0u8]);
        }
    }
    QueryCacheKey(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    fn remote(realm: RealmId) -> QueryCacheKey {
        remote_key(&RemoteKeyInput {
            distributed: true,
            realm_id: realm,
            credential: &[9u8; 32],
            graph_iris: None,
            sparql: "ASK { ?s ?p ?o }",
            allow_partial: false,
            target_nodes: None,
        })
    }

    #[test]
    fn realm_splits_key() {
        // One process serving one realm must not be the only thing keeping
        // fan-out entries apart.
        assert_ne!(remote(RealmId([1u8; 32])), remote(RealmId([2u8; 32])));
        assert_eq!(remote(RealmId([1u8; 32])), remote(RealmId([1u8; 32])));
    }

    #[test]
    fn auth_skips_cache() {
        let auth = AuthContext::anonymous(RealmId([1u8; 32]));
        assert!(credential_digest(None, None).is_some());
        assert!(credential_digest(Some(&auth), None).is_none());
        assert!(credential_digest(None, Some("token")).is_none());
        assert!(credential_digest(Some(&auth), Some("token")).is_none());
    }

    #[test]
    fn scope_changes_key() {
        let visible = ["urn:a".to_string(), "urn:b".to_string()];
        let narrowed = ["urn:a".to_string()];
        let sparql = "SELECT ?s WHERE { ?s ?p ?o }";
        let wide = local_key(LocalScopeKind::Lazy, &graphs_digest(&visible), sparql);
        let narrow = local_key(LocalScopeKind::Lazy, &graphs_digest(&narrowed), sparql);
        assert_ne!(wide, narrow);
        // Order and duplicates must not split the entry for one scope.
        let reordered = [
            "urn:b".to_string(),
            "urn:a".to_string(),
            "urn:a".to_string(),
        ];
        assert_eq!(
            wide,
            local_key(LocalScopeKind::Lazy, &graphs_digest(&reordered), sparql)
        );
        assert_ne!(
            wide,
            local_key(LocalScopeKind::Eager, &graphs_digest(&visible), sparql)
        );
    }
}
