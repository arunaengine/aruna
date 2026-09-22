//! Queries published repository records through the caller's connector authority.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::invenio::InvenioQuery;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use http::Method;
use serde_json::Value;

use super::{TransferError, connect};
use crate::driver::DriverContext;

pub async fn search_records(
    context: &DriverContext,
    auth: &AuthContext,
    query: &InvenioQuery,
    limit: u64,
) -> Result<Value, TransferError> {
    if query.page == 0 || !(1..=25).contains(&query.size) || query.q.len() > 4096 {
        return Err(TransferError::Permanent(
            "invalid repository search bounds".into(),
        ));
    }
    let client = connect(
        context,
        auth,
        query.group_id,
        query.connector_id,
        Permission::READ,
        limit,
        None,
    )
    .await?;
    let mut url = client.url(&["records"])?;
    url.query_pairs_mut()
        .append_pair("q", &query.q)
        .append_pair("page", &query.page.to_string())
        .append_pair("size", &query.size.to_string())
        .append_pair(
            "allversions",
            if query.all_versions { "true" } else { "false" },
        );
    let page = client.json(Method::GET, url, None).await?;
    if !page["hits"]["hits"].is_array() || page["hits"]["total"].is_null() {
        return Err(TransferError::Permanent(
            "invalid repository search response".into(),
        ));
    }
    Ok(page)
}
