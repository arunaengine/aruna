//! Defines placement policies, selectors and advertised subjects with their validation limits.
//! Decides whether a subject satisfies the policy refs on a governed object.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::NodeId;
use crate::structs::placement::record::{DEFAULT_LOCATION, LabelMatch, MAX_LOCATION_LEN};
use crate::types::GroupId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use ulid::Ulid;

pub mod attachment;
pub mod document;

/// Domain separator for the canonical placement-policy digest.
pub const POLICY_DIGEST_DOMAIN: &[u8] = b"aruna-placement-policy-v2";
/// Domain separator for the advertised placement-subject digest.
pub const SUBJECT_DIGEST_DOMAIN: &[u8] = b"aruna-placement-subject-v1";
/// Maximum policy name length in bytes, after trimming.
pub const MAX_POLICY_LEN: usize = 128;
/// Maximum OR-arms in one policy; a residency rule needs few alternatives.
pub const MAX_POLICY_SELECTORS: usize = 32;
/// Maximum ANDed label matches inside one selector.
pub const MAX_SELECTOR_LABELS: usize = 16;
/// Maximum label key length, above any realistic node or worker label key.
pub const MAX_LABEL_LEN: usize = 128;
/// Maximum label value length; an empty value is a valid label.
pub const MAX_VALUE_LEN: usize = 256;
/// Maximum executor kind length, matching the short wire kinds nodes advertise.
pub const MAX_KIND_LEN: usize = 32;
/// Maximum labels one advertised subject carries, above any realistic node or
/// worker label set.
pub const MAX_SUBJECT_LABELS: usize = 32;
/// Maximum policy refs on one governed version or registered copy. Refs
/// intersect, so a longer set can only be more restrictive than this bound.
pub const MAX_POLICY_REFS: usize = 8;
/// Raw refs one canonical set may be built from. A union of two governed sets
/// stays acceptable, while a larger input is rejected before it is allocated.
pub const MAX_REF_INPUT: usize = 2 * MAX_POLICY_REFS;

#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
pub enum PlacementPolicyError {
    #[error("policy id must not be nil")]
    NilPolicyId,
    #[error("owner group id must not be nil")]
    NilOwnerGroup,
    #[error("policy name must be 1..={MAX_POLICY_LEN} bytes")]
    InvalidName,
    #[error("policy must define 1..={MAX_POLICY_SELECTORS} selectors")]
    SelectorCount,
    #[error("selector must constrain at least one subject attribute")]
    EmptySelector,
    #[error("selector must define at most {MAX_SELECTOR_LABELS} label matches")]
    LabelCount,
    #[error("label key must be 1..={MAX_LABEL_LEN} bytes, value at most {MAX_VALUE_LEN}")]
    InvalidLabel,
    #[error("subject carries at most {MAX_SUBJECT_LABELS} labels")]
    SubjectLabelCount,
    #[error("label key {key} is present with more than one spelling")]
    AmbiguousLabel { key: String },
    #[error("selector location must be 1..={MAX_LOCATION_LEN} bytes")]
    InvalidLocation,
    #[error("selector executor kind must be 1..={MAX_KIND_LEN} bytes")]
    InvalidExecutorKind,
    #[error("policy document is not in canonical form")]
    NotCanonical,
    #[error("a governed record carries at most {MAX_POLICY_REFS} policy refs")]
    RefCount,
    #[error("one evaluation resolves at most {MAX_REF_INPUT} policies")]
    ResolutionCount,
    #[error("policy {policy_id} is referenced with two different digests")]
    ConflictingRefs { policy_id: Ulid },
    /// A definition change must mint a new id, so a second document under a
    /// known id is refused instead of replacing the rule holders already serve.
    #[error("policy {policy_id} already exists with different canonical bytes")]
    PolicyIdReuse { policy_id: Ulid },
}

/// Typed physical residency constraint. Evaluated only after realm/group CEL
/// authorization has already allowed the operation: a matching selector can
/// never grant access CEL denied.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct PlacementPolicy {
    pub policy_id: Ulid,
    pub name: String,
    /// Group that owns the rule; `None` is realm-wide. An owned rule may only
    /// be referenced by that group's own buckets and objects, and its
    /// publication is authorized against that group's admin path.
    pub owner_group_id: Option<GroupId>,
    /// OR across selectors; fields inside one selector are ANDed.
    pub allowed: Vec<PlacementSelector>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct PlacementSelector {
    pub node_id: Option<NodeId>,
    pub location: Option<String>,
    pub labels: Vec<LabelMatch>,
    pub executor_kind: Option<String>,
}

/// Travels with every governed version and registered copy, so a reader learns
/// which policy to resolve without a global dataset-to-policy catalog.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlacementPolicyRef {
    pub policy_id: Ulid,
    pub digest: [u8; 32],
}

/// The storage or execution subject a policy is evaluated against.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct PlacementSubject {
    pub node_id: NodeId,
    /// Node-local counter incremented whenever any policy-relevant field below
    /// changes. A receipt stores it so a later subject change cannot be replayed
    /// as a placement credential.
    pub generation: u64,
    pub location: String,
    pub labels: BTreeMap<String, String>,
    pub executor_kind: Option<String>,
    /// True only for a backend whose workload runs on the controller host.
    pub local_to_controller: bool,
}

/// Canonical matching view of a subject. Only this form reaches a selector, so
/// evaluation never depends on untrimmed or empty attribute spellings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NormalizedSubject {
    node_id: NodeId,
    location: String,
    labels: BTreeMap<String, String>,
    executor_kind: Option<String>,
}

/// A validated policy in canonical form. Only this shape resolves a ref, so a
/// malformed document neither matches a subject nor mints an authoritative ref.
/// Deliberately not serializable: decoded bytes must pass `verify` again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedPolicy(PlacementPolicy);

/// Local resolution state of one referenced policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyResolution {
    /// Canonical policy bytes this node has verified and cached.
    Known(VerifiedPolicy),
    /// Holders were consulted and could not supply the document.
    Unresolved,
}

/// Outcome of evaluating a governed ref set against one subject.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementDecision {
    Allowed,
    /// Refs this node has never resolved; the caller fetches them and retries.
    Required {
        refs: Vec<PlacementPolicyRef>,
    },
    /// Referenced policies that could not be obtained at all.
    Unavailable {
        policy_ids: Vec<Ulid>,
    },
    /// Resolved bytes do not hash to the referenced digest.
    DigestMismatch {
        refs: Vec<PlacementPolicyRef>,
    },
    /// A resolved document failed validation; a malformed policy is never
    /// reinterpreted as a grant.
    Invalid {
        policy_ids: Vec<Ulid>,
    },
    Denied {
        policy_ids: Vec<Ulid>,
    },
    /// The refs, resolutions, or subject exceed a declared bound or are
    /// ambiguous, so nothing was evaluated at all.
    InvalidInput {
        reason: PlacementPolicyError,
    },
}

impl PlacementPolicy {
    /// Validates the definition and stores it in canonical form. A definition
    /// change must mint a new policy id, because the digest identifies bytes.
    pub fn new(
        policy_id: Ulid,
        name: String,
        allowed: Vec<PlacementSelector>,
    ) -> Result<Self, PlacementPolicyError> {
        let policy = Self {
            policy_id,
            name,
            owner_group_id: None,
            allowed,
        };
        policy.validate()?;
        Ok(policy.canonical())
    }

    /// Binds the rule to one group. Only that group's buckets may reference it,
    /// and only that group's admins may publish it.
    pub fn owned_by(self, owner_group_id: GroupId) -> Result<Self, PlacementPolicyError> {
        if owner_group_id.is_nil() {
            return Err(PlacementPolicyError::NilOwnerGroup);
        }
        Ok(Self {
            owner_group_id: Some(owner_group_id),
            ..self
        })
    }

    pub fn validate(&self) -> Result<(), PlacementPolicyError> {
        if self.policy_id.is_nil() {
            return Err(PlacementPolicyError::NilPolicyId);
        }
        if self
            .owner_group_id
            .is_some_and(|group_id| group_id.is_nil())
        {
            return Err(PlacementPolicyError::NilOwnerGroup);
        }
        let name = self.name.trim();
        if name.is_empty() || name.len() > MAX_POLICY_LEN {
            return Err(PlacementPolicyError::InvalidName);
        }
        if self.allowed.is_empty() || self.allowed.len() > MAX_POLICY_SELECTORS {
            return Err(PlacementPolicyError::SelectorCount);
        }
        for selector in &self.allowed {
            selector.validate()?;
        }
        Ok(())
    }

    /// Trimmed fields with selectors and labels sorted and deduplicated, so one
    /// definition has exactly one encoding regardless of authoring order.
    pub fn canonical(&self) -> Self {
        let mut keyed = self
            .allowed
            .iter()
            .map(|selector| {
                let selector = selector.canonical();
                let mut key = Vec::new();
                encode_selector(&selector, &mut key);
                (key, selector)
            })
            .collect::<Vec<_>>();
        keyed.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        keyed.dedup_by(|left, right| left.0 == right.0);
        Self {
            policy_id: self.policy_id,
            name: self.name.trim().to_string(),
            owner_group_id: self.owner_group_id,
            allowed: keyed.into_iter().map(|(_, selector)| selector).collect(),
        }
    }

    /// The one encoding the digest is taken over; also the byte identity a
    /// holder compares before accepting a document under an existing id.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let canonical = self.canonical();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&canonical.policy_id.to_bytes());
        write_field(&mut bytes, Some(&canonical.name));
        match canonical.owner_group_id {
            Some(group_id) => {
                bytes.push(1);
                bytes.extend_from_slice(&group_id.to_bytes());
            }
            None => bytes.push(0),
        }
        bytes.extend_from_slice(&(canonical.allowed.len() as u64).to_le_bytes());
        for selector in &canonical.allowed {
            encode_selector(selector, &mut bytes);
        }
        bytes
    }

    /// Private: an authoritative ref may only be minted from a document that
    /// passed [`VerifiedPolicy::verify`].
    fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(POLICY_DIGEST_DOMAIN);
        hasher.update(&self.canonical_bytes());
        *hasher.finalize().as_bytes()
    }

    /// A subject satisfies the policy when any selector matches; a policy
    /// without selectors allows nothing. Private for the same reason as
    /// [`PlacementPolicy::digest`].
    fn allows(&self, subject: &NormalizedSubject) -> bool {
        self.allowed
            .iter()
            .any(|selector| selector.matches(subject))
    }
}

impl VerifiedPolicy {
    /// Accepts a decoded or locally authored document only when it is valid and
    /// already canonical, so one ref resolves to exactly one encoding.
    pub fn verify(policy: PlacementPolicy) -> Result<Self, PlacementPolicyError> {
        let verified = Self(policy);
        verified.revalidate()?;
        Ok(verified)
    }

    /// Boundary re-check for a document that arrived from a cache or a peer;
    /// construction alone never makes a document trusted.
    pub fn revalidate(&self) -> Result<(), PlacementPolicyError> {
        self.0.validate()?;
        if self.0 != self.0.canonical() {
            return Err(PlacementPolicyError::NotCanonical);
        }
        Ok(())
    }

    pub fn policy(&self) -> &PlacementPolicy {
        &self.0
    }

    pub fn digest(&self) -> [u8; 32] {
        self.0.digest()
    }

    pub fn policy_ref(&self) -> PlacementPolicyRef {
        PlacementPolicyRef {
            policy_id: self.0.policy_id,
            digest: self.0.digest(),
        }
    }

    pub fn allows(&self, subject: &NormalizedSubject) -> bool {
        self.0.allows(subject)
    }
}

impl PlacementSelector {
    pub fn validate(&self) -> Result<(), PlacementPolicyError> {
        if let Some(location) = self.location.as_deref() {
            let location = location.trim();
            if location.is_empty() || location.len() > MAX_LOCATION_LEN {
                return Err(PlacementPolicyError::InvalidLocation);
            }
        }
        if let Some(kind) = self.executor_kind.as_deref() {
            let kind = kind.trim();
            if kind.is_empty() || kind.len() > MAX_KIND_LEN {
                return Err(PlacementPolicyError::InvalidExecutorKind);
            }
        }
        if self.labels.len() > MAX_SELECTOR_LABELS {
            return Err(PlacementPolicyError::LabelCount);
        }
        for label in &self.labels {
            let key = label.key.trim();
            if key.is_empty()
                || key.len() > MAX_LABEL_LEN
                || label.value.trim().len() > MAX_VALUE_LEN
            {
                return Err(PlacementPolicyError::InvalidLabel);
            }
        }
        if !self.constrains() {
            return Err(PlacementPolicyError::EmptySelector);
        }
        Ok(())
    }

    pub fn canonical(&self) -> Self {
        let mut labels = self
            .labels
            .iter()
            .map(|label| LabelMatch {
                key: label.key.trim().to_string(),
                value: label.value.trim().to_string(),
            })
            .collect::<Vec<_>>();
        labels.sort_unstable_by(|left, right| {
            (&left.key, &left.value).cmp(&(&right.key, &right.value))
        });
        labels.dedup();
        Self {
            node_id: self.node_id,
            location: trimmed(self.location.as_deref()),
            labels,
            executor_kind: trimmed(self.executor_kind.as_deref()),
        }
    }

    /// Every present field must match. An unconstrained selector never matches,
    /// so an invalid document cannot silently allow every subject.
    pub fn matches(&self, subject: &NormalizedSubject) -> bool {
        if !self.constrains() {
            return false;
        }
        if let Some(node_id) = self.node_id
            && node_id != subject.node_id
        {
            return false;
        }
        if let Some(location) = trimmed(self.location.as_deref())
            && location != subject.location
        {
            return false;
        }
        for label in &self.labels {
            if subject.labels.get(label.key.trim()).map(String::as_str) != Some(label.value.trim())
            {
                return false;
            }
        }
        if let Some(kind) = trimmed(self.executor_kind.as_deref())
            && subject.executor_kind.as_deref() != Some(kind.as_str())
        {
            return false;
        }
        true
    }

    fn constrains(&self) -> bool {
        self.node_id.is_some()
            || trimmed(self.location.as_deref()).is_some()
            || !self.labels.is_empty()
            || trimmed(self.executor_kind.as_deref()).is_some()
    }
}

impl PlacementPolicyRef {
    /// Sorted, deduplicated ref set stored on a governed record. The raw input
    /// is bounded before it is copied, and two digests for one policy id
    /// contradict policy immutability and fail closed.
    pub fn canonical_set(refs: &[Self]) -> Result<Vec<Self>, PlacementPolicyError> {
        if refs.len() > MAX_REF_INPUT {
            return Err(PlacementPolicyError::RefCount);
        }
        let mut canonical = refs.to_vec();
        canonical.sort_unstable();
        canonical.dedup();
        if canonical.len() > MAX_POLICY_REFS {
            return Err(PlacementPolicyError::RefCount);
        }
        for pair in canonical.windows(2) {
            if pair[0].policy_id == pair[1].policy_id {
                return Err(PlacementPolicyError::ConflictingRefs {
                    policy_id: pair[0].policy_id,
                });
            }
        }
        Ok(canonical)
    }
}

impl PlacementSubject {
    /// Bounds every advertised attribute before it is evaluated or hashed. Two
    /// label keys that trim to one key are ambiguous rather than merged.
    pub fn validate(&self) -> Result<(), PlacementPolicyError> {
        if self.location.trim().len() > MAX_LOCATION_LEN {
            return Err(PlacementPolicyError::InvalidLocation);
        }
        if self.labels.len() > MAX_SUBJECT_LABELS {
            return Err(PlacementPolicyError::SubjectLabelCount);
        }
        let mut keys = BTreeSet::new();
        for (key, value) in &self.labels {
            let key = key.trim();
            if key.is_empty() || key.len() > MAX_LABEL_LEN || value.trim().len() > MAX_VALUE_LEN {
                return Err(PlacementPolicyError::InvalidLabel);
            }
            if !keys.insert(key) {
                return Err(PlacementPolicyError::AmbiguousLabel {
                    key: key.to_string(),
                });
            }
        }
        if let Some(kind) = self.executor_kind.as_deref() {
            let kind = kind.trim();
            if kind.is_empty() || kind.len() > MAX_KIND_LEN {
                return Err(PlacementPolicyError::InvalidExecutorKind);
            }
        }
        Ok(())
    }

    /// `local_to_controller` is a transport detail rather than a residency
    /// attribute, so it is deliberately absent from the matching view.
    pub fn normalized(&self) -> Result<NormalizedSubject, PlacementPolicyError> {
        self.validate()?;
        Ok(NormalizedSubject {
            node_id: self.node_id,
            location: trimmed(Some(self.location.as_str()))
                .unwrap_or_else(|| DEFAULT_LOCATION.to_string()),
            labels: self
                .labels
                .iter()
                .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
                .collect(),
            executor_kind: trimmed(self.executor_kind.as_deref()),
        })
    }

    /// Binds the generation and the execution-site model to the matching
    /// attributes, so a receipt's stored digest detects a subject that changed
    /// underneath it.
    pub fn digest(&self) -> Result<[u8; 32], PlacementPolicyError> {
        let normalized = self.normalized()?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(SUBJECT_DIGEST_DOMAIN);
        hasher.update(&self.generation.to_le_bytes());
        hasher.update(&[u8::from(self.local_to_controller)]);
        hasher.update(normalized.node_id.as_bytes());
        hasher.update(&(normalized.location.len() as u64).to_le_bytes());
        hasher.update(normalized.location.as_bytes());
        hasher.update(&(normalized.labels.len() as u64).to_le_bytes());
        for (key, value) in &normalized.labels {
            hasher.update(&(key.len() as u64).to_le_bytes());
            hasher.update(key.as_bytes());
            hasher.update(&(value.len() as u64).to_le_bytes());
            hasher.update(value.as_bytes());
        }
        match &normalized.executor_kind {
            Some(kind) => {
                hasher.update(&[1]);
                hasher.update(&(kind.len() as u64).to_le_bytes());
                hasher.update(kind.as_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
        Ok(*hasher.finalize().as_bytes())
    }
}

/// Pure placement evaluation: every ref must allow the subject, and inside one
/// policy any selector may match. An empty ref set is unrestricted data; inputs
/// are bounded before allocation and an unverifiable document blocks, never grants.
pub fn evaluate_placement(
    refs: &[PlacementPolicyRef],
    resolved: &BTreeMap<Ulid, PolicyResolution>,
    subject: &PlacementSubject,
) -> PlacementDecision {
    if resolved.len() > MAX_REF_INPUT {
        return PlacementDecision::InvalidInput {
            reason: PlacementPolicyError::ResolutionCount,
        };
    }
    let refs = match PlacementPolicyRef::canonical_set(refs) {
        Ok(refs) => refs,
        Err(reason) => return PlacementDecision::InvalidInput { reason },
    };
    let subject = match subject.normalized() {
        Ok(subject) => subject,
        Err(reason) => return PlacementDecision::InvalidInput { reason },
    };
    // The canonical ref set is sorted, deduplicated, and free of conflicting
    // policy ids, so every collection below is bounded and deterministic.
    let mut required = Vec::new();
    let mut unavailable = Vec::new();
    let mut mismatched = Vec::new();
    let mut invalid = Vec::new();
    let mut denied = Vec::new();
    for policy_ref in &refs {
        match resolved.get(&policy_ref.policy_id) {
            None => required.push(*policy_ref),
            Some(PolicyResolution::Unresolved) => unavailable.push(policy_ref.policy_id),
            Some(PolicyResolution::Known(policy)) => {
                if policy.revalidate().is_err() {
                    invalid.push(policy_ref.policy_id);
                } else if policy.policy().policy_id != policy_ref.policy_id
                    || policy.digest() != policy_ref.digest
                {
                    mismatched.push(*policy_ref);
                } else if !policy.allows(&subject) {
                    denied.push(policy_ref.policy_id);
                }
            }
        }
    }
    // Every outcome below blocks the operation; the order only decides which reason is reported, and an
    // incomplete evaluation must not be sold as a definitive denial.
    if !invalid.is_empty() {
        return PlacementDecision::Invalid {
            policy_ids: invalid,
        };
    }
    if !mismatched.is_empty() {
        return PlacementDecision::DigestMismatch { refs: mismatched };
    }
    if !unavailable.is_empty() {
        return PlacementDecision::Unavailable {
            policy_ids: unavailable,
        };
    }
    if !required.is_empty() {
        return PlacementDecision::Required { refs: required };
    }
    if !denied.is_empty() {
        return PlacementDecision::Denied { policy_ids: denied };
    }
    PlacementDecision::Allowed
}

fn trimmed(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Length-prefixed optional field, so distinct fields cannot collide across
/// encoding boundaries.
fn write_field(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&(value.len() as u64).to_le_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        None => out.push(0),
    }
}

fn encode_selector(selector: &PlacementSelector, out: &mut Vec<u8>) {
    match selector.node_id {
        Some(node_id) => {
            out.push(1);
            out.extend_from_slice(node_id.as_bytes());
        }
        None => out.push(0),
    }
    write_field(out, selector.location.as_deref());
    write_field(out, selector.executor_kind.as_deref());
    out.extend_from_slice(&(selector.labels.len() as u64).to_le_bytes());
    for label in &selector.labels {
        write_field(out, Some(&label.key));
        write_field(out, Some(&label.value));
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
