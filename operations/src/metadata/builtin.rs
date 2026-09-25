//! Serves the SHACL shapes that ship inside the node binary for built-in profiles.
//! These profiles need no realm document, so their revision is a constant, not an event id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::metadata::CRATE_PROFILE_IRI;
use aruna_core::repository::builtin_profile;

/// Revision reported for every built-in Profile: the shapes change only when
/// the node binary does, so there is nothing per-realm to pin.
pub(crate) const BUILTIN_REVISION: &str = "builtin";

const RUN_CRATE_SHAPES: &str = include_str!("process_run.ttl");

/// The embedded SHACL Turtle sources for `iri`, when the node ships shapes for it.
pub fn builtin_shapes(iri: &str) -> Option<&'static [&'static str]> {
    if iri == CRATE_PROFILE_IRI {
        return Some(&[RUN_CRATE_SHAPES]);
    }
    builtin_profile(iri).map(|profile| profile.shapes)
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use oxttl::TurtleParser;

    #[test]
    fn shapes_parse() {
        let repository = aruna_core::repository::kinds()
            .iter()
            .flat_map(|kind| kind.profiles)
            .map(|profile| profile.iri);
        for iri in std::iter::once(CRATE_PROFILE_IRI).chain(repository) {
            for shapes in builtin_shapes(iri).expect("embedded shapes") {
                let triples = TurtleParser::new()
                    .for_slice(shapes.as_bytes())
                    .collect::<Result<Vec<_>, _>>()
                    .expect("the embedded Turtle must parse");
                assert!(!triples.is_empty());
            }
        }
        assert!(builtin_shapes("https://w3id.org/ro/wfrun/process/0.4").is_none());
    }
}
