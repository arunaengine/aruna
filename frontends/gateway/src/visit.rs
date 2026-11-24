//! Reads what one request brings to an applet host: its decision and the visitor's sessions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::HeaderMap;

use crate::Gateway;
use crate::policy::{self, Frame};
use crate::session::{PREVIEW, Pick, Session, VISITOR, cookies, now_seconds};
use crate::upstream::{Access, Answer, Serving};

/// One request to an applet host.
pub struct Visit {
    pub host: String,
    pub answer: Arc<Answer>,
    pub visitor: Option<Session>,
    /// A preview session; set only while the node names a valid portal origin.
    pub preview: Option<Session>,
    pub portal: Option<String>,
}

impl Visit {
    pub fn read(gateway: &Gateway, host: String, answer: Arc<Answer>, headers: &HeaderMap) -> Self {
        let now = now_seconds();
        let applet_id = answer.decision.applet_id.as_str();
        let open = |name| {
            cookies(headers, name)
                .find_map(|text| gateway.sealer.open(&host, text, now))
                .filter(|session| session.applet_id == applet_id)
        };
        let preview = open(PREVIEW).filter(|session| session.preview);
        let visitor = open(VISITOR);
        let portal = (answer.decision.portal_origin.as_deref())
            .and_then(policy::origin)
            .map(str::to_string);
        // Without a portal a preview session still signs the visitor in, but frames nothing.
        let (visitor, preview) = match portal {
            Some(_) => (visitor, preview),
            None => (visitor.or(preview), None),
        };
        Self {
            host,
            answer,
            visitor,
            preview,
            portal,
        }
    }

    /// The node token of this visit; a preview session wins.
    pub fn token(&self) -> Option<&str> {
        (self.preview.as_ref().or(self.visitor.as_ref())).map(|session| session.token.as_str())
    }

    /// Only a preview session lets the portal frame the response; it never widens embedding.
    pub fn frame(&self) -> Frame<'_> {
        match (&self.preview, &self.portal) {
            (Some(_), Some(portal)) => Frame::Preview(portal),
            _ => Frame::Embed(&self.answer.decision.frame_ancestors),
        }
    }

    /// Whether every visitor sees the same bytes: the live version of a public applet.
    pub fn shared(&self) -> bool {
        let decision = &self.answer.decision;
        self.preview.is_none()
            && decision.serving == Serving::Live
            && decision.access == Access::Public
    }

    /// The node route of this applet's runtime calls.
    pub fn applet_route(&self) -> Option<String> {
        let id = &self.answer.decision.applet_id;
        plain_id(id).then(|| format!("/edge/v1/applets/{id}"))
    }

    /// The node route of the files this visit sees: the preview pick, else the served version.
    pub fn files_route(&self) -> Option<String> {
        let applet = self.applet_route()?;
        let pick = self
            .preview
            .as_ref()
            .and_then(|session| session.pick.clone());
        match pick.or_else(|| self.answer.decision.version.map(Pick::Version))? {
            Pick::Version(version) => Some(format!("{applet}/versions/{version}")),
            Pick::Build(build) if plain_id(&build) => Some(format!("{applet}/drafts/{build}")),
            Pick::Build(_) => None,
        }
    }
}

/// Whether `text` is a safe single path segment id, such as a ULID or a run id.
pub fn plain_id(text: &str) -> bool {
    (1..=64).contains(&text.len())
        && (text.bytes()).all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// The decoded query parameters; a repeated name keeps its first value.
pub fn query_params(query: Option<&str>) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for (key, value) in url::form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
        params.entry(key.into_owned()).or_insert(value.into_owned());
    }
    params
}
