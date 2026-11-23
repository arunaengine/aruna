//! Registry-owned realm state: listing status, granted w3id namespace and note. Realms never
//! sign it; only a registry admin changes it, and every change leaves an audit record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Longest granted namespace, the w3id path segment below `aruna/`.
pub const MAX_NAMESPACE_LEN: usize = 64;
/// First segments under `aruna/` that Aruna identifiers already use.
const RESERVED: [&str; 7] = [
    "data", "profile", "profiles", "terms", "invenio", "dev", "x",
];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RealmStatus {
    /// Shown in the listing, not resolved through w3id.org.
    #[default]
    Listed,
    /// Shown first and allowed as a resolver target.
    Approved,
    /// Neither shown nor resolved.
    Hidden,
    /// Neither shown nor resolved; final, so old identifiers keep explaining themselves.
    Retired,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminState {
    pub status: RealmStatus,
    /// Sub-namespace below `https://w3id.org/aruna/` granted to this realm.
    pub namespace: Option<String>,
    pub note: String,
}

impl AdminState {
    /// Whether the public listing shows the realm.
    pub fn listed(&self) -> bool {
        matches!(self.status, RealmStatus::Listed | RealmStatus::Approved)
    }

    /// Whether the resolver may send identifiers to the realm.
    pub fn approved(&self) -> bool {
        self.status == RealmStatus::Approved
    }

    /// The one resolver rule: only approved realms are targets, retired ones are gone.
    pub fn may_resolve(&self) -> ResolveGate {
        match self.status {
            RealmStatus::Approved => ResolveGate::Allowed,
            RealmStatus::Retired => ResolveGate::Retired,
            RealmStatus::Listed | RealmStatus::Hidden => ResolveGate::Refused,
        }
    }
}

/// What a resolver route may do with an identifier of a realm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolveGate {
    Allowed,
    /// Answer 410, so old identifiers explain themselves.
    Retired,
    Refused,
}

/// One admin change: who made it, when, and the state before and after.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub at: u64,
    pub actor: String,
    pub before: AdminState,
    pub after: AdminState,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdminError {
    #[error("a retired realm keeps its status and namespace")]
    Retired,
    #[error(
        "namespace must be 1 to 64 characters of a-z, 0-9, '.', '_' or '-', starting with a-z or 0-9"
    )]
    BadNamespace,
    #[error("namespace '{0}' is reserved for Aruna identifiers")]
    ReservedNamespace(String),
    #[error("namespace '{0}' is granted to another realm")]
    NamespaceTaken(String),
}

/// Checks `next` against `current`; `taken` tells whether another realm holds a namespace.
pub fn transition(
    current: &AdminState,
    next: AdminState,
    taken: impl Fn(&str) -> bool,
) -> Result<AdminState, AdminError> {
    if current.status == RealmStatus::Retired
        && (next.status != RealmStatus::Retired || next.namespace != current.namespace)
    {
        return Err(AdminError::Retired);
    }
    let namespace = next.namespace.filter(|namespace| !namespace.is_empty());
    if let Some(namespace) = &namespace
        && current.namespace.as_ref() != Some(namespace)
    {
        check_namespace(namespace)?;
        if taken(namespace) {
            return Err(AdminError::NamespaceTaken(namespace.clone()));
        }
    }
    Ok(AdminState { namespace, ..next })
}

fn check_namespace(namespace: &str) -> Result<(), AdminError> {
    let mut chars = namespace.chars();
    let first_ok = chars
        .next()
        .is_some_and(|first| first.is_ascii_lowercase() || first.is_ascii_digit());
    let rest_ok = chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c));
    if !first_ok || !rest_ok || namespace.len() > MAX_NAMESPACE_LEN {
        return Err(AdminError::BadNamespace);
    }
    // A 26 character alphanumeric segment would read as a document ULID.
    let ulid_shaped = namespace.len() == 26 && namespace.chars().all(|c| c.is_ascii_alphanumeric());
    if RESERVED.contains(&namespace) || ulid_shaped {
        return Err(AdminError::ReservedNamespace(namespace.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(status: RealmStatus, namespace: Option<&str>) -> AdminState {
        AdminState {
            status,
            namespace: namespace.map(str::to_string),
            note: String::new(),
        }
    }

    fn free(_: &str) -> bool {
        false
    }

    #[test]
    fn statuses_change_freely() {
        // Every status except retired can move to every other one.
        use RealmStatus::*;
        for from in [Listed, Approved, Hidden] {
            for to in [Listed, Approved, Hidden, Retired] {
                let next = transition(&state(from, None), state(to, None), free).unwrap();
                assert_eq!(next.status, to);
            }
        }
        assert!(state(Approved, None).approved() && state(Approved, None).listed());
        assert!(!state(Hidden, None).listed() && !state(Retired, None).listed());
        assert_eq!(state(Approved, None).may_resolve(), ResolveGate::Allowed);
        assert_eq!(state(Retired, None).may_resolve(), ResolveGate::Retired);
        assert_eq!(state(Listed, None).may_resolve(), ResolveGate::Refused);
        assert_eq!(state(Hidden, None).may_resolve(), ResolveGate::Refused);
    }

    #[test]
    fn retired_is_final() {
        // Only the note of a retired realm changes; its namespace stays reserved.
        let retired = state(RealmStatus::Retired, Some("giessen"));
        for status in [
            RealmStatus::Listed,
            RealmStatus::Approved,
            RealmStatus::Hidden,
        ] {
            let next = state(status, Some("giessen"));
            assert_eq!(transition(&retired, next, free), Err(AdminError::Retired));
        }
        let cleared = state(RealmStatus::Retired, None);
        assert_eq!(
            transition(&retired, cleared, free),
            Err(AdminError::Retired)
        );
        let mut noted = retired.clone();
        noted.note = "debug reinstall".to_string();
        assert_eq!(transition(&retired, noted.clone(), free), Ok(noted));
    }

    #[test]
    fn namespace_rules() {
        let listed = state(RealmStatus::Listed, None);
        let with = |namespace: &str| state(RealmStatus::Approved, Some(namespace));
        assert!(transition(&listed, with("giessen-1.a_b"), free).is_ok());
        for bad in ["Giessen", "-a", "a/b", "a b", &"a".repeat(65)] {
            assert_eq!(
                transition(&listed, with(bad), free),
                Err(AdminError::BadNamespace),
                "{bad}"
            );
        }
        for reserved in ["data", "terms", "x", "01jmetadata0123456789abcde"] {
            assert!(matches!(
                transition(&listed, with(reserved), free),
                Err(AdminError::ReservedNamespace(_))
            ));
        }
        // An empty namespace clears the grant.
        let cleared = transition(&with("giessen"), with(""), free).unwrap();
        assert_eq!(cleared.namespace, None);
    }

    #[test]
    fn namespace_unique() {
        // A namespace held by another realm is refused; keeping one's own is not a conflict.
        let taken = |namespace: &str| namespace == "giessen";
        let listed = state(RealmStatus::Listed, None);
        let next = state(RealmStatus::Approved, Some("giessen"));
        assert_eq!(
            transition(&listed, next.clone(), taken),
            Err(AdminError::NamespaceTaken("giessen".to_string()))
        );
        assert_eq!(transition(&next, next.clone(), taken), Ok(next));
    }
}
