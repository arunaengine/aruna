use aruna_storage::storage::store::Store;

use crate::{
    models::requests::{ForwardRequest, ForwardResponse, Request},
    persistence::search::generic::Search,
    transactions::controller::Controller,
};

use super::network_trait::{Body, Network, Response};

pub(super) async fn handle_forwarding_messages<St, Se, N>(
    token: Option<String>,
    controller: &Controller<St, Se, N>,
    request: ForwardRequest,
) -> Body
where
    for<'a> St: Store<'a>,
    Se: Search,
    N: Network,
{
    let body = match request {
        // User requests:
        ForwardRequest::AddUser(req) => match req.authorize(token, controller).await {
            Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::AddUser(req.clone().run_request(auth_ctx, controller).await),
            ))),
            Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::GetResource(Err(e)),
            ))),
        },
        ForwardRequest::GetUser(req) => match req.authorize(token, controller).await {
            Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::GetUser(req.clone().run_request(auth_ctx, controller).await),
            ))),
            Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::GetResource(Err(e)),
            ))),
        },
        ForwardRequest::CreateToken(req) => match req.authorize(token, controller).await {
            Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::CreateToken(req.clone().run_request(auth_ctx, controller).await),
            ))),
            Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::GetResource(Err(e)),
            ))),
        },

        // Group request:
        ForwardRequest::AddGroup(req) => match req.authorize(token, controller).await {
            Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::AddGroup(req.clone().run_request(auth_ctx, controller).await),
            ))),
            Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::GetResource(Err(e)),
            ))),
        },
        ForwardRequest::AddUserToGroup(req) => match req.authorize(token, controller).await {
            Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::AddUserToGroup(
                    req.clone().run_request(auth_ctx, controller).await,
                ),
            ))),
            Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::GetResource(Err(e)),
            ))),
        },
        ForwardRequest::GetGroup(req) => match req.authorize(token, controller).await {
            Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::GetGroup(req.clone().run_request(auth_ctx, controller).await),
            ))),
            Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::GetResource(Err(e)),
            ))),
        },

        // Resource requests:
        ForwardRequest::CreateProject(req) => match req.authorize(token, controller).await {
            Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::CreateProject(req.clone().run_request(auth_ctx, controller).await),
            ))),
            Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::Search(Err(e)),
            ))),
        },
        ForwardRequest::CreateResource(req) => match req.authorize(token, controller).await {
            Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::CreateResource(
                    req.clone().run_request(auth_ctx, controller).await,
                ),
            ))),
            Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::Search(Err(e)),
            ))),
        },
        ForwardRequest::GetResource(req) => match req.authorize(token, controller).await {
            Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::GetResource(req.clone().run_request(auth_ctx, controller).await),
            ))),
            Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::GetResource(Err(e)),
            ))),
        },
        ForwardRequest::UpdateResource(req) => match req.authorize(token, controller).await {
            Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::UpdateResource(
                    req.clone().run_request(auth_ctx, controller).await,
                ),
            ))),
            Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::UpdateResource(Err(e)),
            ))),
        },
        ForwardRequest::Search(req) => match req.authorize(token, controller).await {
            Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::Search(req.clone().run_request(auth_ctx, controller).await),
            ))),
            Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                ForwardResponse::Search(Err(e)),
            ))),
        },
    };
    body
}
