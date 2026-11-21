//! The login choices and legal documents the portal configuration serves to the portal app.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One legal document: a label and either an external page or Markdown text.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LegalDocument {
    pub label: String,
    #[serde(flatten)]
    pub source: LegalSource,
}

/// Served as `url` or `markdown` beside the label.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum LegalSource {
    Url(String),
    Markdown(String),
}

/// Login choices the portal offers in its login chooser, served unchanged as `login`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PortalLogin {
    /// Replaces the plain realm sign-in row; an option without `params` is that sign-in.
    #[serde(default)]
    pub options: Vec<LoginOption>,
    /// Page of the identity provider that lists and removes linked logins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoginOption {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logo: Option<String>,
    /// A copy of the logo for light backgrounds, for logos drawn in white.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logo_light: Option<String>,
    /// Extra authorization request parameters, such as `kc_idp_hint`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, String>,
    /// Extra parameters that link this login to the signed-in account; empty means not linkable.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub link_params: BTreeMap<String, String>,
    /// The `id` of the realm OIDC provider that issues this option's ID tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Base URL of the login bridge; the portal calls `<bridge>/authorize` and `<bridge>/token`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bridge: Option<String>,
}
