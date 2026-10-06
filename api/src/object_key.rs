//! Removes object keys before REST tracing and keeps only a redacting guarded value.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::{SecretBytes, SharedSecret};
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use base64::{Engine, engine::general_purpose::STANDARD};
use zeroize::Zeroizing;

#[derive(Clone, Debug)]
pub(crate) struct ObjectKey(pub Result<Option<SharedSecret>, ()>);

pub(crate) async fn middleware(mut request: Request, next: Next) -> Response {
    let count = request
        .headers()
        .get_all("x-aruna-object-key")
        .iter()
        .count();
    let key = match request.headers_mut().remove("x-aruna-object-key") {
        None => Ok(None),
        Some(mut header) => {
            header.set_sensitive(true);
            let mut bytes = Zeroizing::new([0u8; 32]);
            let decoded = if count == 1 && header.as_bytes().len() == 44 {
                STANDARD
                    .decode_slice(header.as_bytes(), &mut bytes[..])
                    .ok()
            } else {
                None
            };
            drop(header);
            if decoded == Some(32) {
                Ok(Some(SharedSecret::new(SecretBytes::new(bytes.to_vec()))))
            } else {
                Err(())
            }
        }
    };
    request.extensions_mut().insert(ObjectKey(key));
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::{Router, routing::get};
    use http::{Request as HttpRequest, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn abe_header() {
        let secret = STANDARD.encode([17; 32]);
        let router = Router::new()
            .route(
                "/probe",
                get(|request: Request| async move {
                    assert!(!request.headers().contains_key("x-aruna-object-key"));
                    let key = request.extensions().get::<ObjectKey>().unwrap();
                    assert!(!format!("{key:?}").contains(&STANDARD.encode([17; 32])));
                    assert_eq!(
                        key.0.as_ref().unwrap().as_ref().unwrap().bytes().expose(),
                        &[17; 32]
                    );
                    StatusCode::NO_CONTENT
                }),
            )
            .layer(axum::middleware::from_fn(middleware));
        let response = router
            .oneshot(
                HttpRequest::builder()
                    .uri("/probe")
                    .header("x-aruna-object-key", secret)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
}
