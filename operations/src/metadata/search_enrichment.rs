use oxrdf::Term;

// Schema.org predicates craqle indexes into its full-text field. Enrichment
// reads the same literals so snippets align with what search matched on.
const SCHEMA_NAME: &str = "http://schema.org/name";
const SCHEMA_DESCRIPTION: &str = "http://schema.org/description";
const SCHEMA_KEYWORDS: &str = "http://schema.org/keywords";
const SCHEMA_IDENTIFIER: &str = "http://schema.org/identifier";

const SNIPPET_MAX_LEN: usize = 160;

/// Human-readable title for a search hit. Always returns a non-empty string,
/// falling back through the schema name, the document path or subject IRI.
pub(crate) fn hit_title(
    properties: &[(String, Term)],
    document_path: &str,
    subject_iri: &str,
) -> String {
    if let Some(name) = first_literal(properties, SCHEMA_NAME) {
        let name = name.trim();
        if !name.is_empty() {
            return name.to_string();
        }
    }

    if subject_iri.is_empty() || subject_iri == "./" {
        if !document_path.is_empty() {
            return document_path.to_string();
        }
    } else if let Some(segment) = last_path_segment(subject_iri) {
        return segment.to_string();
    }

    if !subject_iri.is_empty() {
        return subject_iri.to_string();
    }
    document_path.to_string()
}

/// Query-relevant snippet windowed around the first matching token, or a prefix
/// of the indexed literals when no token matches. `None` when there is no text.
pub(crate) fn hit_snippet(properties: &[(String, Term)], query: &str) -> Option<String> {
    let mut text = String::new();
    for predicate in [
        SCHEMA_NAME,
        SCHEMA_DESCRIPTION,
        SCHEMA_KEYWORDS,
        SCHEMA_IDENTIFIER,
    ] {
        for (property, object) in properties {
            if property != predicate {
                continue;
            }
            if let Term::Literal(literal) = object {
                let value = literal.value().trim();
                if value.is_empty() {
                    continue;
                }
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str(value);
            }
        }
    }
    if text.is_empty() {
        return None;
    }

    let tokens = query_tokens(query);
    if let Some(window) = snippet_window(&text, &tokens, SNIPPET_MAX_LEN) {
        return Some(window);
    }
    Some(prefix_snippet(&text, SNIPPET_MAX_LEN))
}

fn first_literal<'a>(properties: &'a [(String, Term)], predicate: &str) -> Option<&'a str> {
    properties.iter().find_map(|(property, object)| {
        if property != predicate {
            return None;
        }
        match object {
            Term::Literal(literal) => Some(literal.value()),
            _ => None,
        }
    })
}

fn last_path_segment(subject_iri: &str) -> Option<&str> {
    let trimmed = subject_iri.trim_end_matches('/');
    let segment = trimmed.rsplit(['/', '#']).next()?;
    if segment.is_empty() {
        None
    } else {
        Some(segment)
    }
}

fn query_tokens(query: &str) -> Vec<String> {
    let cleaned: String = query
        .chars()
        .map(|c| {
            if "+-&|!(){}[]^\"~*?:\\/".contains(c) {
                ' '
            } else {
                c
            }
        })
        .collect();
    cleaned
        .split_whitespace()
        .filter(|token| !matches!(*token, "AND" | "OR" | "NOT"))
        .map(|token| token.to_ascii_lowercase())
        .filter(|token| !token.is_empty())
        .collect()
}

fn snippet_window(text: &str, tokens: &[String], max_len: usize) -> Option<String> {
    let haystack = text.to_ascii_lowercase();
    let mut best: Option<(usize, usize)> = None;
    for token in tokens {
        if let Some(index) = haystack.find(token.as_str()) {
            match best {
                Some((start, _)) if start <= index => {}
                _ => best = Some((index, token.len())),
            }
        }
    }
    let (match_start, match_len) = best?;
    let match_end = match_start + match_len;

    let context = max_len.saturating_sub(match_len);
    let lead = context / 2;
    let mut start = match_start.saturating_sub(lead);
    let mut end = (match_end + (context - lead)).min(text.len());
    while start > 0 && !text.is_char_boundary(start) {
        start -= 1;
    }
    while end < text.len() && !text.is_char_boundary(end) {
        end += 1;
    }

    let mut snippet = String::new();
    if start > 0 {
        snippet.push('…');
    }
    snippet.push_str(&text[start..end]);
    if end < text.len() {
        snippet.push('…');
    }
    Some(snippet)
}

fn prefix_snippet(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        return text.to_string();
    }
    let mut end = max_len;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    use oxrdf::{Literal, NamedNode};

    fn literal(predicate: &str, value: &str) -> (String, Term) {
        (
            predicate.to_string(),
            Term::Literal(Literal::new_simple_literal(value)),
        )
    }

    fn named(predicate: &str, iri: &str) -> (String, Term) {
        (
            predicate.to_string(),
            Term::NamedNode(NamedNode::new_unchecked(iri)),
        )
    }

    #[test]
    fn title_prefers_schema_name_literal() {
        let properties = vec![literal(SCHEMA_NAME, "Public Dataset")];
        assert_eq!(
            hit_title(&properties, "datasets/public", "./"),
            "Public Dataset"
        );
    }

    #[test]
    fn title_falls_back_to_document_path_for_root_subject() {
        let properties = vec![named(SCHEMA_NAME, "https://example.org/thing")];
        assert_eq!(
            hit_title(&properties, "datasets/public", "./"),
            "datasets/public"
        );
    }

    #[test]
    fn title_falls_back_to_subject_segment() {
        let properties = Vec::new();
        assert_eq!(
            hit_title(&properties, "datasets/public", "./data/file-7.txt"),
            "file-7.txt"
        );
        assert_eq!(
            hit_title(
                &properties,
                "datasets/public",
                "https://example.org/graph#entity"
            ),
            "entity"
        );
    }

    #[test]
    fn title_is_never_empty() {
        assert_eq!(hit_title(&[], "", ""), "");
        assert_eq!(hit_title(&[], "datasets/x", ""), "datasets/x");
    }

    #[test]
    fn snippet_windows_around_first_match() {
        let text = "alpha ".repeat(40) + "needle tail content here";
        let properties = vec![literal(SCHEMA_DESCRIPTION, &text)];
        let snippet = hit_snippet(&properties, "needle").unwrap();
        assert!(snippet.contains("needle"));
        assert!(snippet.starts_with('…'));
        assert!(snippet.chars().count() <= SNIPPET_MAX_LEN + 4);
    }

    #[test]
    fn snippet_match_at_start_has_no_leading_ellipsis() {
        let properties = vec![literal(SCHEMA_NAME, "Public Dataset about climate")];
        let snippet = hit_snippet(&properties, "Public").unwrap();
        assert!(snippet.starts_with("Public"));
    }

    #[test]
    fn snippet_is_char_boundary_safe() {
        let text = "☃".repeat(120) + "needle";
        let properties = vec![literal(SCHEMA_DESCRIPTION, &text)];
        let snippet = hit_snippet(&properties, "needle").unwrap();
        assert!(snippet.contains("needle"));
        // Slicing on a non-boundary would have panicked before we got here.
        assert!(snippet.starts_with('…'));
    }

    #[test]
    fn snippet_strips_query_syntax_tokens() {
        let properties = vec![literal(SCHEMA_DESCRIPTION, "the quick brown fox jumps")];
        let snippet = hit_snippet(&properties, "\"brown\" AND fox").unwrap();
        assert!(snippet.contains("brown"));
    }

    #[test]
    fn snippet_without_match_returns_prefix() {
        let text = "unrelated ".repeat(40);
        let properties = vec![literal(SCHEMA_DESCRIPTION, &text)];
        let snippet = hit_snippet(&properties, "needle").unwrap();
        assert!(snippet.starts_with("unrelated"));
        assert!(snippet.ends_with('…'));
    }

    #[test]
    fn snippet_without_literals_is_none() {
        let properties = vec![named(SCHEMA_NAME, "https://example.org/x")];
        assert_eq!(hit_snippet(&properties, "needle"), None);
    }
}
