//! Identifier parsers of database entries: regex rules that turn an object key or source URL
//! into links to public accession pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Values keep RFC 3986 unreserved characters and encode everything else.
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// The rules of one database entry, applied in order.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct IdentifierParser {
    pub rules: Vec<IdentifierRule>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct IdentifierRule {
    /// Shown to users, for example "BioSample".
    pub name: String,
    /// A Rust regex with one capture group named `id`.
    pub pattern: String,
    pub target: IdentifierTarget,
    /// The landing page; `{id}` is replaced by the encoded value.
    pub url_template: String,
}

/// What a rule reads: the object key or the recorded source URL.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierTarget {
    Key,
    SourceUrl,
}

/// One identifier found in a key or source URL.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Identifier {
    pub name: String,
    pub value: String,
    pub url: String,
    pub target: IdentifierTarget,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum IdentifierError {
    #[error("rule {index}: the pattern does not compile: {reason}")]
    Pattern { index: usize, reason: String },
    #[error("rule {index}: the pattern has no capture group named `id`")]
    MissingGroup { index: usize },
    #[error("rule {index}: the link template must contain `{{id}}`")]
    MissingPlaceholder { index: usize },
    #[error("rule {index}: the link template must give an http or https URL")]
    InvalidTemplate { index: usize },
}

/// Checks every rule; the regex crate's own size and nesting limits are the only bounds.
pub fn validate_parser(parser: &IdentifierParser) -> Result<(), IdentifierError> {
    CompiledParser::compile(parser).map(|_| ())
}

/// A parser with its patterns compiled, for one read.
#[derive(Clone, Debug)]
pub struct CompiledParser {
    rules: Vec<(Regex, IdentifierRule)>,
}

impl CompiledParser {
    pub fn compile(parser: &IdentifierParser) -> Result<Self, IdentifierError> {
        let mut rules = Vec::with_capacity(parser.rules.len());
        for (index, rule) in parser.rules.iter().enumerate() {
            let regex = Regex::new(&rule.pattern).map_err(|error| IdentifierError::Pattern {
                index,
                reason: error.to_string(),
            })?;
            if !regex.capture_names().any(|name| name == Some("id")) {
                return Err(IdentifierError::MissingGroup { index });
            }
            if !rule.url_template.contains("{id}") {
                return Err(IdentifierError::MissingPlaceholder { index });
            }
            let sample = rule.url_template.replace("{id}", "x");
            if !url::Url::parse(&sample).is_ok_and(|url| matches!(url.scheme(), "http" | "https")) {
                return Err(IdentifierError::InvalidTemplate { index });
            }
            rules.push((regex, rule.clone()));
        }
        Ok(Self { rules })
    }

    /// The first match of each rule, without repeated (name, value) pairs, in rule order.
    pub fn find(&self, key: &str, source_url: Option<&str>) -> Vec<Identifier> {
        let mut found: Vec<Identifier> = Vec::new();
        for (regex, rule) in &self.rules {
            let text = match rule.target {
                IdentifierTarget::Key => Some(key),
                IdentifierTarget::SourceUrl => source_url,
            };
            let Some(value) = text
                .and_then(|text| regex.captures(text))
                .and_then(|captures| captures.name("id"))
                .map(|value| value.as_str().to_string())
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            if found
                .iter()
                .any(|known| known.name == rule.name && known.value == value)
            {
                continue;
            }
            let encoded = utf8_percent_encode(&value, SEGMENT).to_string();
            found.push(Identifier {
                name: rule.name.clone(),
                url: rule.url_template.replace("{id}", &encoded),
                value,
                target: rule.target,
            });
        }
        found
    }
}

#[cfg(test)]
#[path = "identifier_tests.rs"]
mod tests;
