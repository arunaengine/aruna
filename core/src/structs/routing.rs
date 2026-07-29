use crate::errors::ConversionError;
use crate::structs::{BackendRef, ResolvedBackend};
use crate::types::{GroupId, UserId};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

/// Upper bound of a storage class identifier. The same vocabulary names node
/// backend classes, routing targets and derived node labels.
pub const STORAGE_CLASS_MAX_LEN: usize = 32;

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum RoutingError {
    #[error("backend {0} is not registered on this node")]
    UnknownBackend(BackendRef),
    #[error("group-defined storage backends are disabled on this node")]
    GroupEgressDisabled,
    #[error("duplicate routing rule for prefix `{0}`")]
    DuplicateRule(String),
    #[error("invalid storage class `{0}`")]
    InvalidClass(String),
}

/// A storage class identifier is `[a-z0-9-]` of 1..=32 characters.
pub fn validate_storage_class(class: &str) -> Result<(), RoutingError> {
    let valid = !class.is_empty()
        && class.len() <= STORAGE_CLASS_MAX_LEN
        && class
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    valid
        .then_some(())
        .ok_or_else(|| RoutingError::InvalidClass(class.to_string()))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RoutingTarget {
    Backend(BackendRef),
    Class(String),
}

/// Who authored a rule. Operator rules reach every class in the node's table;
/// tenant rules only reach the classes the operator opened to them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuleSource {
    Tenant,
    Operator,
}

/// A rule scoped to one bucket. `key_prefix` is a literal S3 key prefix, or the
/// whole key when `exact`; an empty non-exact prefix is the bucket default.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageRoutingRule {
    pub key_prefix: String,
    pub exact: bool,
    pub target: RoutingTarget,
}

/// Group-wide default target, one record per group.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupStorageRouting {
    pub group_id: GroupId,
    pub default_target: Option<RoutingTarget>,
    pub updated_at: SystemTime,
    pub updated_by: UserId,
}

impl GroupStorageRouting {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Operator-authored all-groups rule from the node's backends file. An unset
/// field matches everything; set fields all have to match.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeRoutingRule {
    pub group: Option<GroupId>,
    pub bucket: Option<String>,
    pub key_prefix: Option<String>,
    pub target: RoutingTarget,
}

impl NodeRoutingRule {
    fn matches(&self, group: GroupId, bucket: &str, key: &str) -> bool {
        self.group.is_none_or(|scope| scope == group)
            && self.bucket.as_deref().is_none_or(|scope| scope == bucket)
            && self
                .key_prefix
                .as_deref()
                .is_none_or(|prefix| key.starts_with(prefix))
    }

    fn specificity(&self) -> (usize, usize) {
        let fields = usize::from(self.group.is_some())
            + usize::from(self.bucket.is_some())
            + usize::from(self.key_prefix.is_some());
        (fields, self.key_prefix.as_ref().map_or(0, String::len))
    }
}

/// One row of a node's class table, without credentials.
#[derive(Clone, Debug, Eq, PartialEq)]
struct CatalogEntry {
    class: Option<String>,
    allow_tenants: bool,
}

/// The node's registered backends and the group's own backend ids, without any
/// credentials, so an operation can turn a class into a concrete backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendCatalog {
    backends: BTreeMap<String, CatalogEntry>,
    default_name: String,
    group_backends: BTreeSet<Ulid>,
    serve_group_backends: bool,
}

impl BackendCatalog {
    pub fn new(default_name: impl Into<String>) -> Self {
        Self {
            backends: BTreeMap::new(),
            default_name: default_name.into(),
            group_backends: BTreeSet::new(),
            serve_group_backends: true,
        }
    }

    pub fn with_backend(mut self, name: impl Into<String>, class: Option<String>) -> Self {
        self.backends.insert(
            name.into(),
            CatalogEntry {
                class,
                allow_tenants: true,
            },
        );
        self
    }

    /// A backend whose class is reserved for operator rules and the node
    /// default; tenant rules naming that class miss instead of binding.
    pub fn with_reserved(mut self, name: impl Into<String>, class: Option<String>) -> Self {
        self.backends.insert(
            name.into(),
            CatalogEntry {
                class,
                allow_tenants: false,
            },
        );
        self
    }

    pub fn with_group(mut self, backend_id: Ulid) -> Self {
        self.group_backends.insert(backend_id);
        self
    }

    /// Mirrors the node's `[egress] serve_group_backends = false` narrowing.
    pub fn without_group_egress(mut self) -> Self {
        self.serve_group_backends = false;
        self
    }

    pub fn default_name(&self) -> &str {
        &self.default_name
    }

    pub fn class_of(&self, name: &str) -> Option<&str> {
        self.backends
            .get(name)
            .and_then(|entry| entry.class.as_deref())
    }

    /// Whether tenant-authored rules may target this backend's class.
    pub fn allows_tenants(&self, name: &str) -> bool {
        self.backends
            .get(name)
            .is_some_and(|entry| entry.allow_tenants)
    }

    pub fn default_backend(&self) -> Result<ResolvedBackend, RoutingError> {
        self.resolve_node(&self.default_name)
    }

    fn resolve_node(&self, name: &str) -> Result<ResolvedBackend, RoutingError> {
        let entry = self
            .backends
            .get(name)
            .ok_or_else(|| RoutingError::UnknownBackend(BackendRef::Node(name.to_string())))?;
        Ok(ResolvedBackend::new(
            BackendRef::Node(name.to_string()),
            entry.class.clone(),
        ))
    }

    /// Turns a rule target into a concrete backend. A named backend is binding
    /// and its absence is an error; a class is a preference and a node that
    /// does not offer it answers `None`, a miss the caller continues past.
    pub fn resolve_target(
        &self,
        target: &RoutingTarget,
        source: RuleSource,
    ) -> Result<Option<ResolvedBackend>, RoutingError> {
        match target {
            RoutingTarget::Backend(BackendRef::Node(name)) => self.resolve_node(name).map(Some),
            RoutingTarget::Backend(BackendRef::Group(id)) => {
                if !self.serve_group_backends {
                    return Err(RoutingError::GroupEgressDisabled);
                }
                if !self.group_backends.contains(id) {
                    return Err(RoutingError::UnknownBackend(BackendRef::Group(*id)));
                }
                Ok(Some(ResolvedBackend::new(BackendRef::Group(*id), None)))
            }
            RoutingTarget::Class(class) => Ok(self
                .backends
                .iter()
                .find(|(_, entry)| {
                    entry.class.as_deref() == Some(class.as_str())
                        && (entry.allow_tenants || source == RuleSource::Operator)
                })
                .map(|(name, entry)| {
                    ResolvedBackend::new(BackendRef::Node(name.clone()), entry.class.clone())
                })),
        }
    }
}

/// The node-wide half of routing: operator rules plus the backend catalog.
/// Cloned out of node state per write, never re-read from disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeRouting {
    pub rules: Vec<NodeRoutingRule>,
    pub catalog: BackendCatalog,
}

impl Default for NodeRouting {
    fn default() -> Self {
        Self {
            rules: Vec::new(),
            catalog: BackendCatalog::new(BackendRef::DEFAULT_NODE_NAME)
                .with_backend(BackendRef::DEFAULT_NODE_NAME, None),
        }
    }
}

impl NodeRouting {
    pub fn snapshot(&self, group_id: GroupId) -> RoutingSnapshot {
        RoutingSnapshot::new(group_id, self.catalog.clone()).with_node_rules(self.rules.clone())
    }
}

/// Everything resolution needs, assembled by the caller before the operation
/// starts so `start`/`step` stay free of I/O.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutingSnapshot {
    pub group_id: GroupId,
    pub node_rules: Vec<NodeRoutingRule>,
    pub catalog: BackendCatalog,
    pub group_default: Option<RoutingTarget>,
    pub bucket_rules: Vec<StorageRoutingRule>,
}

impl RoutingSnapshot {
    pub fn new(group_id: GroupId, catalog: BackendCatalog) -> Self {
        Self {
            group_id,
            node_rules: Vec::new(),
            catalog,
            group_default: None,
            bucket_rules: Vec::new(),
        }
    }

    /// Snapshot of a node with only the implicit default backend.
    pub fn single(group_id: GroupId) -> Self {
        Self::new(group_id, NodeRouting::default().catalog)
    }

    pub fn with_node_rules(mut self, rules: Vec<NodeRoutingRule>) -> Self {
        self.node_rules = rules;
        self
    }

    pub fn with_group_default(mut self, target: Option<RoutingTarget>) -> Self {
        self.group_default = target;
        self
    }

    pub fn with_bucket_rules(mut self, rules: Vec<StorageRoutingRule>) -> Self {
        self.bucket_rules = rules;
        self
    }
}

/// One rule that could decide a write, with the provenance class lookup needs.
struct Candidate<'a> {
    target: &'a RoutingTarget,
    source: RuleSource,
}

/// Every rule that could apply, most specific first: exact key, longest bucket
/// prefix, bucket default, group default, node rules. Stable sorts keep record
/// and file order as the last tie-breaker, so the ladder is a total order.
fn candidates<'a>(snapshot: &'a RoutingSnapshot, bucket: &str, key: &str) -> Vec<Candidate<'a>> {
    let tenant = |target| Candidate {
        target,
        source: RuleSource::Tenant,
    };
    let mut ordered: Vec<Candidate<'a>> = snapshot
        .bucket_rules
        .iter()
        .filter(|rule| rule.exact && rule.key_prefix == key)
        .map(|rule| tenant(&rule.target))
        .collect();

    // An empty prefix is the bucket default, so longest-prefix covers it too.
    let mut prefixed: Vec<&StorageRoutingRule> = snapshot
        .bucket_rules
        .iter()
        .filter(|rule| !rule.exact && key.starts_with(&rule.key_prefix))
        .collect();
    prefixed.sort_by_key(|rule| Reverse(rule.key_prefix.len()));
    ordered.extend(prefixed.into_iter().map(|rule| tenant(&rule.target)));

    ordered.extend(snapshot.group_default.as_ref().map(tenant));

    let mut scoped: Vec<&NodeRoutingRule> = snapshot
        .node_rules
        .iter()
        .filter(|rule| rule.matches(snapshot.group_id, bucket, key))
        .collect();
    scoped.sort_by_key(|rule| Reverse(rule.specificity()));
    ordered.extend(scoped.into_iter().map(|rule| Candidate {
        target: &rule.target,
        source: RuleSource::Operator,
    }));

    ordered
}

/// A class this node does not offer reroutes the write, so record it rather
/// than let the specificity ladder degrade silently.
fn warn_missed(missed: &[&str], chosen: &ResolvedBackend) {
    if missed.is_empty() {
        return;
    }
    tracing::warn!(
        missed_classes = ?missed,
        backend = %chosen.backend,
        storage_class = ?chosen.storage_class,
        "storage class not offered on this node, routing fell through"
    );
}

/// Picks the target for one write. Candidates are tried in strict specificity
/// order and the first that resolves wins; a named backend that does not
/// resolve aborts, while a class this node does not offer is only a miss.
pub fn resolve_backend(
    snapshot: &RoutingSnapshot,
    bucket: &str,
    key: &str,
) -> Result<ResolvedBackend, RoutingError> {
    let mut missed = Vec::new();
    for candidate in candidates(snapshot, bucket, key) {
        if let Some(resolved) = snapshot
            .catalog
            .resolve_target(candidate.target, candidate.source)?
        {
            warn_missed(&missed, &resolved);
            return Ok(resolved);
        }
        if let RoutingTarget::Class(class) = candidate.target {
            missed.push(class.as_str());
        }
    }

    let resolved = snapshot.catalog.default_backend()?;
    warn_missed(&missed, &resolved);
    Ok(resolved)
}

/// Rejects two rules in one scope sharing `(exact, key_prefix)`, so the
/// specificity ladder never has to break a tie at resolution time.
pub fn validate_rule_set(rules: &[StorageRoutingRule]) -> Result<(), RoutingError> {
    let mut seen = BTreeSet::new();
    for rule in rules {
        if !seen.insert((rule.exact, rule.key_prefix.as_str())) {
            return Err(RoutingError::DuplicateRule(rule.key_prefix.clone()));
        }
        if let RoutingTarget::Class(class) = &rule.target {
            validate_storage_class(class)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BackendCatalog, NodeRoutingRule, RoutingError, RoutingSnapshot, RoutingTarget,
        StorageRoutingRule, resolve_backend, validate_rule_set, validate_storage_class,
    };
    use crate::structs::{BackendRef, ResolvedBackend};
    use ulid::Ulid;

    fn catalog() -> BackendCatalog {
        BackendCatalog::new("default")
            .with_backend("default", None)
            .with_backend("cold", Some("cold".to_string()))
            .with_backend("tape", Some("archive".to_string()))
    }

    fn snapshot() -> RoutingSnapshot {
        RoutingSnapshot::new(Ulid::from_bytes([1u8; 16]), catalog())
    }

    fn rule(prefix: &str, exact: bool, backend: &str) -> StorageRoutingRule {
        StorageRoutingRule {
            key_prefix: prefix.to_string(),
            exact,
            target: RoutingTarget::Backend(BackendRef::Node(backend.to_string())),
        }
    }

    fn class_rule(prefix: &str, class: &str) -> StorageRoutingRule {
        StorageRoutingRule {
            key_prefix: prefix.to_string(),
            exact: false,
            target: RoutingTarget::Class(class.to_string()),
        }
    }

    fn node(backend: &str) -> ResolvedBackend {
        ResolvedBackend::new(
            BackendRef::Node(backend.to_string()),
            catalog().class_of(backend).map(str::to_string),
        )
    }

    #[test]
    fn uses_node_default() {
        assert_eq!(
            resolve_backend(&snapshot(), "b", "k").unwrap(),
            node("default")
        );
    }

    #[test]
    fn exact_beats_prefix() {
        let snapshot = snapshot().with_bucket_rules(vec![
            rule("archive/", false, "cold"),
            rule("archive/one", true, "tape"),
        ]);

        assert_eq!(
            resolve_backend(&snapshot, "b", "archive/one").unwrap(),
            node("tape")
        );
        assert_eq!(
            resolve_backend(&snapshot, "b", "archive/two").unwrap(),
            node("cold")
        );
    }

    #[test]
    fn exact_needs_key() {
        let snapshot = snapshot().with_bucket_rules(vec![rule("archive/", true, "tape")]);

        assert_eq!(
            resolve_backend(&snapshot, "b", "archive/one").unwrap(),
            node("default")
        );
        assert_eq!(
            resolve_backend(&snapshot, "b", "archive/").unwrap(),
            node("tape")
        );
    }

    #[test]
    fn longest_prefix_wins() {
        let snapshot = snapshot().with_bucket_rules(vec![
            rule("", false, "default"),
            rule("a/", false, "cold"),
            rule("a/b/", false, "tape"),
        ]);

        assert_eq!(
            resolve_backend(&snapshot, "b", "a/b/x").unwrap(),
            node("tape")
        );
        assert_eq!(
            resolve_backend(&snapshot, "b", "a/x").unwrap(),
            node("cold")
        );
        assert_eq!(
            resolve_backend(&snapshot, "b", "z").unwrap(),
            node("default")
        );
    }

    #[test]
    fn bucket_beats_group() {
        let snapshot = snapshot()
            .with_bucket_rules(vec![rule("", false, "cold")])
            .with_group_default(Some(RoutingTarget::Backend(BackendRef::Node(
                "tape".to_string(),
            ))));

        assert_eq!(resolve_backend(&snapshot, "b", "k").unwrap(), node("cold"));
    }

    #[test]
    fn group_beats_node() {
        let snapshot = snapshot()
            .with_group_default(Some(RoutingTarget::Class("cold".to_string())))
            .with_node_rules(vec![NodeRoutingRule {
                group: None,
                bucket: None,
                key_prefix: None,
                target: RoutingTarget::Backend(BackendRef::Node("tape".to_string())),
            }]);

        assert_eq!(resolve_backend(&snapshot, "b", "k").unwrap(), node("cold"));
    }

    #[test]
    fn ranks_node_rules() {
        // More set fields wins; equal fields fall back to the longer prefix.
        let group = Ulid::from_bytes([1u8; 16]);
        let snapshot = snapshot().with_node_rules(vec![
            NodeRoutingRule {
                group: None,
                bucket: None,
                key_prefix: Some("a/".to_string()),
                target: RoutingTarget::Backend(BackendRef::Node("default".to_string())),
            },
            NodeRoutingRule {
                group: None,
                bucket: None,
                key_prefix: Some("a/b/".to_string()),
                target: RoutingTarget::Backend(BackendRef::Node("cold".to_string())),
            },
            NodeRoutingRule {
                group: Some(group),
                bucket: Some("b".to_string()),
                key_prefix: None,
                target: RoutingTarget::Backend(BackendRef::Node("tape".to_string())),
            },
        ]);

        assert_eq!(
            resolve_backend(&snapshot, "b", "a/b/x").unwrap(),
            node("tape")
        );
        assert_eq!(
            resolve_backend(&snapshot, "other", "a/b/x").unwrap(),
            node("cold")
        );
        assert_eq!(
            resolve_backend(&snapshot, "other", "a/x").unwrap(),
            node("default")
        );
    }

    #[test]
    fn filters_node_scope() {
        let snapshot = snapshot().with_node_rules(vec![NodeRoutingRule {
            group: Some(Ulid::from_bytes([2u8; 16])),
            bucket: None,
            key_prefix: None,
            target: RoutingTarget::Backend(BackendRef::Node("cold".to_string())),
        }]);

        assert_eq!(
            resolve_backend(&snapshot, "b", "k").unwrap(),
            node("default")
        );
    }

    #[test]
    fn class_target_resolves() {
        let snapshot = snapshot().with_bucket_rules(vec![StorageRoutingRule {
            key_prefix: String::new(),
            exact: false,
            target: RoutingTarget::Class("archive".to_string()),
        }]);

        assert_eq!(resolve_backend(&snapshot, "b", "k").unwrap(), node("tape"));
    }

    #[test]
    fn missing_class_falls_through() {
        // A class this node does not offer is a miss, not a failure.
        let snapshot = snapshot().with_bucket_rules(vec![class_rule("", "glacier")]);

        assert_eq!(
            resolve_backend(&snapshot, "b", "k").unwrap(),
            node("default")
        );
    }

    #[test]
    fn miss_tries_next_rule() {
        // The miss skips one rung only; the next-most-specific rule decides.
        let snapshot = snapshot().with_bucket_rules(vec![
            StorageRoutingRule {
                key_prefix: "a/one".to_string(),
                exact: true,
                target: RoutingTarget::Class("glacier".to_string()),
            },
            class_rule("a/", "cold"),
            rule("", false, "tape"),
        ]);

        assert_eq!(
            resolve_backend(&snapshot, "b", "a/one").unwrap(),
            node("cold")
        );
    }

    #[test]
    fn miss_reaches_node_rules() {
        let snapshot = snapshot()
            .with_group_default(Some(RoutingTarget::Class("glacier".to_string())))
            .with_node_rules(vec![NodeRoutingRule {
                group: None,
                bucket: None,
                key_prefix: None,
                target: RoutingTarget::Backend(BackendRef::Node("tape".to_string())),
            }]);

        assert_eq!(resolve_backend(&snapshot, "b", "k").unwrap(), node("tape"));
    }

    #[test]
    fn reserved_class_skips_tenants() {
        // `allow_tenants = false` hides the class from tenant rules only.
        let catalog = BackendCatalog::new("default")
            .with_backend("default", None)
            .with_reserved("tape", Some("archive".to_string()));
        let snapshot = RoutingSnapshot::new(Ulid::from_bytes([1u8; 16]), catalog)
            .with_bucket_rules(vec![class_rule("", "archive")]);

        assert_eq!(
            resolve_backend(&snapshot, "b", "k").unwrap(),
            ResolvedBackend::new(BackendRef::Node("default".to_string()), None)
        );

        let operator = snapshot.clone().with_node_rules(vec![NodeRoutingRule {
            group: None,
            bucket: None,
            key_prefix: None,
            target: RoutingTarget::Class("archive".to_string()),
        }]);

        assert_eq!(
            resolve_backend(&operator, "b", "k").unwrap(),
            ResolvedBackend::new(
                BackendRef::Node("tape".to_string()),
                Some("archive".to_string())
            )
        );
    }

    #[test]
    fn unknown_backend_fails() {
        let snapshot = snapshot().with_bucket_rules(vec![rule("", false, "ghost")]);

        assert_eq!(
            resolve_backend(&snapshot, "b", "k"),
            Err(RoutingError::UnknownBackend(BackendRef::Node(
                "ghost".to_string()
            )))
        );
    }

    #[test]
    fn binding_failure_aborts() {
        // A broken named backend wins over any less specific rule that works.
        let snapshot = snapshot()
            .with_bucket_rules(vec![rule("a/one", true, "ghost"), class_rule("a/", "cold")]);

        assert_eq!(
            resolve_backend(&snapshot, "b", "a/one"),
            Err(RoutingError::UnknownBackend(BackendRef::Node(
                "ghost".to_string()
            )))
        );
    }

    #[test]
    fn missing_default_fails() {
        let snapshot =
            RoutingSnapshot::new(Ulid::from_bytes([1u8; 16]), BackendCatalog::new("gone"));

        assert_eq!(
            resolve_backend(&snapshot, "b", "k"),
            Err(RoutingError::UnknownBackend(BackendRef::Node(
                "gone".to_string()
            )))
        );
    }

    #[test]
    fn group_target_resolves() {
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let snapshot = RoutingSnapshot::new(
            Ulid::from_bytes([1u8; 16]),
            catalog().with_group(backend_id),
        )
        .with_group_default(Some(RoutingTarget::Backend(BackendRef::Group(backend_id))));

        assert_eq!(
            resolve_backend(&snapshot, "b", "k").unwrap(),
            ResolvedBackend::new(BackendRef::Group(backend_id), None)
        );
    }

    #[test]
    fn unknown_group_fails() {
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let snapshot = snapshot()
            .with_group_default(Some(RoutingTarget::Backend(BackendRef::Group(backend_id))));

        assert_eq!(
            resolve_backend(&snapshot, "b", "k"),
            Err(RoutingError::UnknownBackend(BackendRef::Group(backend_id)))
        );
    }

    #[test]
    fn group_egress_disabled() {
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let snapshot = RoutingSnapshot::new(
            Ulid::from_bytes([1u8; 16]),
            catalog().with_group(backend_id).without_group_egress(),
        )
        .with_group_default(Some(RoutingTarget::Backend(BackendRef::Group(backend_id))));

        assert_eq!(
            resolve_backend(&snapshot, "b", "k"),
            Err(RoutingError::GroupEgressDisabled)
        );
    }

    #[test]
    fn rejects_duplicate_rules() {
        assert_eq!(
            validate_rule_set(&[rule("a/", false, "cold"), rule("a/", false, "tape")]),
            Err(RoutingError::DuplicateRule("a/".to_string()))
        );
        validate_rule_set(&[rule("a/", false, "cold"), rule("a/", true, "tape")]).unwrap();
    }

    #[test]
    fn validates_class_names() {
        for ok in ["hot", "cold-2", "a", &"x".repeat(32)] {
            validate_storage_class(ok).unwrap();
        }
        for bad in ["", "Hot", "co ld", "cold_2", &"x".repeat(33)] {
            assert_eq!(
                validate_storage_class(bad),
                Err(RoutingError::InvalidClass(bad.to_string()))
            );
        }
    }

    #[test]
    fn key_bytes_differ() {
        let id = Ulid::from_bytes([5u8; 16]);
        assert_eq!(
            BackendRef::node_default().key_bytes(),
            b"n:default".to_vec()
        );
        assert_eq!(
            BackendRef::Group(id).key_bytes(),
            format!("g:{id}").into_bytes()
        );
        assert_ne!(
            BackendRef::Node(id.to_string()).key_bytes(),
            BackendRef::Group(id).key_bytes()
        );
    }
}
