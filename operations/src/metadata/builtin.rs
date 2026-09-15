//! Profiles the node validates from embedded shapes, with no realm document.
//! Their shapes ship with the binary, so a revision is a constant rather than a
//! registry event id, and no registry row backs them.

use aruna_core::metadata::CRATE_PROFILE_IRI;

/// Revision reported for every built-in Profile: the shapes change only when
/// the node binary does, so there is nothing per-realm to pin.
pub(crate) const BUILTIN_REVISION: &str = "builtin";

const RUN_CRATE_SHAPES: &str = include_str!("process_run.ttl");

/// The embedded SHACL Turtle for `iri`, when the node ships shapes for it.
pub(crate) fn builtin_shapes(iri: &str) -> Option<&'static str> {
    (iri == CRATE_PROFILE_IRI).then_some(RUN_CRATE_SHAPES)
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use oxttl::TurtleParser;

    #[test]
    fn shapes_parse() {
        let shapes = builtin_shapes(CRATE_PROFILE_IRI).expect("embedded shapes");
        let triples = TurtleParser::new()
            .for_slice(shapes.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .expect("the embedded Turtle must parse");
        assert!(!triples.is_empty());
        assert!(builtin_shapes("https://w3id.org/ro/wfrun/process/0.4").is_none());
    }
}
