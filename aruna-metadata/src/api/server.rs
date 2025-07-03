use super::openapi::{self, ArunaApi};
use crate::{
    error::ArunaMetadataError, network::network_trait::Network,
    persistence::search::generic::Search, transactions::controller::Controller,
};
use aruna_storage::storage::store::Store;
use axum::{extract::DefaultBodyLimit, response::Redirect, routing::get};
use std::{net::SocketAddr, sync::Arc};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;

pub struct RestServer {}

impl RestServer {
    #[tracing::instrument(level = "trace", skip(handler, rest_port))]
    pub async fn run<St, Se, N>(
        handler: Arc<Controller<St, Se, N>>,
        rest_port: u16,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a> + 'static,
        Se: Search + 'static,
        N: Network + 'static,
    {
        let socket_address = SocketAddr::from(([0, 0, 0, 0], rest_port));
        let listener = tokio::net::TcpListener::bind(socket_address).await.unwrap();

        let (router, api) = OpenApiRouter::with_openapi(ArunaApi::openapi())
            .nest("/api/v3", openapi::router(handler))
            .split_for_parts();

        let swagger = SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", api);

        let app = router
            .route("/", get(|| async { Redirect::permanent("/swagger-ui") }))
            .merge(swagger)
            .layer(TraceLayer::new_for_http().on_body_chunk(()).on_eos(()))
            .layer(
                CorsLayer::new()
                    .allow_methods(tower_http::cors::AllowMethods::any())
                    .allow_origin(tower_http::cors::Any),
            )
            .layer(DefaultBodyLimit::max(1024 * 1024 * 1024));
        axum::serve(listener, app.into_make_service())
            .await
            .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))?;

        Ok(())
    }
}
