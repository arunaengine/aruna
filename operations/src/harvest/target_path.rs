//! The metadata path budget a harvest source must satisfy, shared by source
//! creation and the harvest job so a source is only accepted when every record
//! it can ever yield has a landing path.

/// Budget for a harvested document's normalized metadata path.
pub const HARVEST_PATH_BYTES: usize = 512;
/// `b3-` plus 64 hex characters: the shortest segment any identifier can take.
pub const DIGEST_SEGMENT_BYTES: usize = 67;

/// Canonical form of a harvest target prefix, or `None` when no record could
/// land under it.
///
/// Surrounding whitespace and slashes are not part of the prefix, and a prefix
/// that leaves less than one full digest segment of the path budget is refused
/// outright rather than failing every record later.
pub fn normalize_target_prefix(prefix: &str) -> Option<String> {
    let prefix = prefix.trim().trim_matches('/').trim();
    if prefix.is_empty() {
        return None;
    }
    (HARVEST_PATH_BYTES.saturating_sub(prefix.len() + 1) >= DIGEST_SEGMENT_BYTES)
        .then(|| prefix.to_string())
}

/// Whether a prefix is blank rather than merely too long, so a caller can name
/// the reason it was refused.
pub fn prefix_is_blank(prefix: &str) -> bool {
    prefix.trim().trim_matches('/').trim().is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    // padding and slashes are not part of the prefix
    #[test]
    fn padding_is_trimmed() {
        for raw in [
            "imported/zenodo",
            " imported/zenodo ",
            "/imported/zenodo/",
            "  /imported/zenodo/  ",
        ] {
            assert_eq!(
                normalize_target_prefix(raw).as_deref(),
                Some("imported/zenodo")
            );
        }
    }

    #[test]
    fn blank_prefixes_refused() {
        for raw in ["", " ", "\t\n", "/", "///", "  //  "] {
            assert!(normalize_target_prefix(raw).is_none());
            assert!(prefix_is_blank(raw));
        }
    }

    /// One byte on either side of the budget: the longest accepted prefix still
    /// leaves an exact digest segment, and one more byte leaves too little.
    #[test]
    fn budget_boundary_exact() {
        let longest = HARVEST_PATH_BYTES - DIGEST_SEGMENT_BYTES - 1;
        assert_eq!(
            normalize_target_prefix(&"p".repeat(longest)).map(|prefix| prefix.len()),
            Some(longest)
        );
        assert!(normalize_target_prefix(&"p".repeat(longest + 1)).is_none());
        assert!(!prefix_is_blank(&"p".repeat(longest + 1)));
    }

    /// The budget applies to the canonical prefix, not the padded input.
    #[test]
    fn padding_is_free() {
        let longest = HARVEST_PATH_BYTES - DIGEST_SEGMENT_BYTES - 1;
        let padded = format!("  /{}/  ", "p".repeat(longest));
        assert_eq!(
            normalize_target_prefix(&padded).map(|prefix| prefix.len()),
            Some(longest)
        );
    }
}
