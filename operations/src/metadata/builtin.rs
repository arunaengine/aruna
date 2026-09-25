//! Serves the SHACL shapes that ship inside the node binary for built-in profiles.
//! These profiles need no realm document, so their revision is a constant, not an event id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::metadata::{CRATE_PROFILE_IRI, INVENIO_PROFILE_IRI, ZENODO_PROFILE_IRI};

/// Revision reported for every built-in Profile: the shapes change only when
/// the node binary does, so there is nothing per-realm to pin.
pub(crate) const BUILTIN_REVISION: &str = "builtin";

const RUN_CRATE_SHAPES: &str = include_str!("process_run.ttl");
const DATACITE_SHAPES: &str = include_str!("datacite.ttl");
const PUBLISHER_SHAPES: &str = include_str!("publisher.ttl");

/// The embedded SHACL Turtle sources for `iri`, when the node ships shapes for it.
pub(crate) fn builtin_shapes(iri: &str) -> Option<&'static [&'static str]> {
    match iri {
        CRATE_PROFILE_IRI => Some(&[RUN_CRATE_SHAPES]),
        ZENODO_PROFILE_IRI => Some(&[DATACITE_SHAPES]),
        INVENIO_PROFILE_IRI => Some(&[DATACITE_SHAPES, PUBLISHER_SHAPES]),
        _ => None,
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use oxttl::TurtleParser;

    #[test]
    fn shapes_parse() {
        for iri in [CRATE_PROFILE_IRI, ZENODO_PROFILE_IRI, INVENIO_PROFILE_IRI] {
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
