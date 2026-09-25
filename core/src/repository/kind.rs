//! One table entry per repository kind that publishes: what it can do, needs and maps.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::LazyLock;

use super::rules::{Rules, load};
use super::{Capabilities, RepositoryError, invenio};
use crate::metadata::{INVENIO_PROFILE_IRI, ZENODO_PROFILE_IRI};
use crate::structs::execution::harvest::RepositoryConnectorKind;
use crate::structs::secondary_id::SecondaryIdKind;

/// A built-in requirement Profile with its SHACL shapes as Turtle sources.
#[derive(Debug)]
pub struct RequirementProfile {
    pub iri: &'static str,
    pub name: &'static str,
    pub shapes: &'static [&'static str],
}

/// What generic code knows about a repository kind; its adapter does the rest.
pub struct KindDescriptor {
    pub kind: RepositoryConnectorKind,
    pub capabilities: Capabilities,
    pub profiles: &'static [RequirementProfile],
    /// The Profile a repository at the endpoint needs, one of `profiles`.
    pub profile: fn(&str) -> &'static str,
    rules: &'static LazyLock<Result<Rules, String>>,
    /// Refuses a record id that is unsafe to use in a request path.
    pub validate_id: fn(&str) -> Result<(), RepositoryError>,
}

const DATACITE: &str = include_str!("datacite.ttl");
const PUBLISHER: &str = include_str!("publisher.ttl");

static INVENIO_RULES: LazyLock<Result<Rules, String>> =
    LazyLock::new(|| load(include_str!("invenio.json")));

static KINDS: [KindDescriptor; 1] = [KindDescriptor {
    kind: RepositoryConnectorKind::Invenio,
    capabilities: Capabilities {
        drafts: true,
        reserve_identifier: true,
        versions: true,
        review: true,
        pull: true,
        search: true,
        import: true,
        release_date: false,
        identifier_kind: SecondaryIdKind::Doi,
    },
    profiles: &[
        RequirementProfile {
            iri: ZENODO_PROFILE_IRI,
            name: "Zenodo record",
            shapes: &[DATACITE],
        },
        RequirementProfile {
            iri: INVENIO_PROFILE_IRI,
            name: "InvenioRDM record",
            shapes: &[DATACITE, PUBLISHER],
        },
    ],
    profile: invenio::requirement_profile,
    rules: &INVENIO_RULES,
    validate_id: invenio::validate_id,
}];

impl KindDescriptor {
    /// The kind's embedded mapping rules.
    pub fn rules(&self) -> Result<&'static Rules, RepositoryError> {
        self.rules
            .as_ref()
            .map_err(|_| RepositoryError("invalid embedded mapping rules"))
    }
}

/// Every kind that publishes, in listing order.
pub fn kinds() -> &'static [KindDescriptor] {
    &KINDS
}

/// The descriptor of a kind that publishes; `None` for kinds that only harvest.
pub fn descriptor(kind: RepositoryConnectorKind) -> Option<&'static KindDescriptor> {
    KINDS.iter().find(|descriptor| descriptor.kind == kind)
}

/// The built-in requirement Profile with this IRI.
pub fn builtin_profile(iri: &str) -> Option<&'static RequirementProfile> {
    KINDS
        .iter()
        .flat_map(|descriptor| descriptor.profiles)
        .find(|profile| profile.iri == iri)
}
