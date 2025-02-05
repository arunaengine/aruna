use crate::{error::ArunaError, transactions::controller::Controller};
use axum::{extract::DefaultBodyLimit, response::Redirect, routing::get};
use std::{net::SocketAddr, sync::Arc};
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;

use super::openapi::{self, ArunaApi};

pub struct RestServer {}

impl RestServer {
    pub async fn run(handler: Arc<Controller>, rest_port: u16) -> Result<(), ArunaError> {
        let socket_address = SocketAddr::from(([0, 0, 0, 0], rest_port));
        let listener = tokio::net::TcpListener::bind(socket_address).await.unwrap();

        let (router, api) = OpenApiRouter::with_openapi(ArunaApi::openapi())
            .nest("/api/v3", openapi::router(handler))
            .split_for_parts();

        let swagger = SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", api);

        let app = router
            .route("/", get(|| async { Redirect::permanent("/swagger-ui") }))
            .merge(swagger)
            .layer(
                TraceLayer::new_for_http()
                    .on_response(())
                    .on_body_chunk(())
                    .on_eos(()),
            )
            .layer(DefaultBodyLimit::max(1024 * 1024 * 1024));
        axum::serve(listener, app.into_make_service())
            .await
            .map_err(|e| ArunaError::ServerError(e.to_string()))?;

        Ok(())
    }
}
