use crate::{
    context::Context,
    error::ArunaError,
    models::requests::{SearchRequest, SearchResponse},
};

use super::{
    controller::Controller,
    request::{Request, Requester},
};

impl Request for SearchRequest {
    type Response = SearchResponse;

    fn get_context<'a>(&'a self) -> Context {
        Context::Public
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, crate::error::ArunaError> {
        // Disallow impersonation
        if requester
            .as_ref()
            .and_then(|r| r.get_impersonator())
            .is_some()
        {
            return Err(ArunaError::Unauthorized);
        }
        let store = controller.get_store();

        let query = self.query.clone();
        let filter = self.filter.clone();
        let offset = self.offset.unwrap_or(0);
        let limit = self.limit.unwrap_or(20);

        tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;

            let universe = match requester {
                Some(requester) => match requester.get_id() {
                    Some(requester_id) => {
                        let user_idx =
                            store
                                .get_idx_from_ulid(&requester_id, &rtxn)
                                .ok_or_else(|| {
                                    ArunaError::NotFound("Requester not found".to_string())
                                })?;
                        let mut permission_targets = store
                            .get_realm_and_groups(user_idx)?
                            .iter()
                            .map(|idx| idx.0)
                            .collect::<Vec<u32>>();
                        permission_targets.push(user_idx.0);
                        let mut universe =
                            store.get_read_permission_universe(&rtxn, &permission_targets)?;
                        universe |= store.get_public_universe(&rtxn)?;
                        universe
                    }
                    None => store.get_public_universe(&rtxn)?,
                },
                None => store.get_public_universe(&rtxn)?,
            };
            let (expected_hits, result) =
                store.search(query, offset, limit, filter.as_deref(), &rtxn, universe)?;

            rtxn.commit()?;

            Ok(SearchResponse {
                expected_hits,
                resources: result,
            })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}
