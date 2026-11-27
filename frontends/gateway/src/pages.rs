//! The gateway's own responses: status pages, bare statuses and redirects, with the policy set.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use axum::http::StatusCode;
use axum::http::header::{CACHE_CONTROL, HeaderValue, LOCATION};
use axum::response::{Html, IntoResponse, Response};

use crate::policy::{self, Frame};

const TEMPLATE: &str = include_str!("status.html");

/// A small page that explains `status`; the texts are fixed, so nothing needs escaping.
pub fn page(
    status: StatusCode,
    title: &'static str,
    message: &'static str,
    frame: Frame,
) -> Response {
    let body = TEMPLATE
        .replace("{title}", title)
        .replace("{message}", message);
    finish((status, Html(body)).into_response(), frame)
}

pub fn bare(status: StatusCode, frame: Frame) -> Response {
    finish(status.into_response(), frame)
}

pub fn redirect(status: StatusCode, location: &str, frame: Frame) -> Response {
    let mut response = bare(status, frame);
    if let Ok(location) = HeaderValue::from_str(location) {
        response.headers_mut().insert(LOCATION, location);
    }
    response
}

pub fn unavailable(frame: Frame) -> Response {
    let message = "The applet could not be loaded. Try again in a moment.";
    page(
        StatusCode::BAD_GATEWAY,
        "Applet not reachable",
        message,
        frame,
    )
}

pub fn not_found(frame: Frame) -> Response {
    let message = "This applet has no page at this address.";
    page(StatusCode::NOT_FOUND, "Page not found", message, frame)
}

fn finish(mut response: Response, frame: Frame) -> Response {
    let headers = response.headers_mut();
    policy::secure(headers, frame);
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
