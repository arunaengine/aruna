use super::openapi::{self, ArunaApi};
use crate::error::ArunaDataError;
use crate::controller::controller::Controller;
use aruna_storage::storage::store::Store;
use axum::{extract::DefaultBodyLimit, response::Redirect, routing::get};
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;

pub struct RestServer {}

impl RestServer {
    #[tracing::instrument(level = "trace", skip(handler, rest_address, rest_port))]
    pub async fn run<St>(
        handler: Controller<St>,
        rest_address: Ipv4Addr,
        rest_port: u16,
    ) -> Result<(), ArunaDataError>
    where
        for<'a> St: Store<'a> + 'static,
    {
        let socket_address = SocketAddr::from((rest_address, rest_port));
        let listener = tokio::net::TcpListener::bind(socket_address).await?;

        let (router, api) = OpenApiRouter::with_openapi(ArunaApi::openapi())
            .nest("/api/v3", openapi::router::<St>(handler))
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
        axum::serve(listener, app.into_make_service()).await?;

        Ok(())
    }
}
