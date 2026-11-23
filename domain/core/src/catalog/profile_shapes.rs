//! Shapes, reports and errors of a SHACL check of one candidate against a metadata Profile.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use craqle::{CrateViolation, ShaclValidationResult};

/// Every candidate is prepared against one scratch graph that never exists, so
/// the encoded crate root is the same constant for every document and shapes
/// can bind it statically.
pub const VALIDATION_GRAPH_IRI: &str = "https://craqle.invalid/validation/document";
/// Base craqle resolves relative RO-Crate ids against while parsing JSON-LD.
pub const CRAQLE_BASE_IRI: &str = "https://craqle.invalid/";

/// One Profile revision and the Turtle shapes it publishes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileShapes {
    /// Graph the shapes are installed under, unique per Profile revision.
    pub graph_iri: String,
    pub sources: Vec<String>,
}

/// Structural RO-Crate findings and SHACL results for one candidate document.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProfileShaclReport {
    pub results: Vec<ShaclValidationResult>,
    pub structural: Vec<CrateViolation>,
}

/// Why a Profile check could not give a verdict.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ProfileShaclError {
    /// A permanent property of the registered Profile: fail closed.
    #[error("{message}")]
    Unsupported { rule: String, message: String },
    #[error("{message}")]
    Limit { message: String },
    #[error("{message}")]
    Unavailable { message: String },
    #[error("{message}")]
    InvalidInput { message: String },
}
