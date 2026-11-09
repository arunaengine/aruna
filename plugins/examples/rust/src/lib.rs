//! Example plugin: posts the title of each new dataset to a catalog and, when granted,
//! records the catalog id on the dataset.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_plugin::{Context, DatasetEvent, Result};
use serde_json::json;

async fn dataset_created(ctx: Context, event: DatasetEvent) -> Result<()> {
    let dataset = ctx.metadata().get(&event.dataset).await?;
    let title = dataset.json.get("name").cloned().unwrap_or_default();
    let record = ctx
        .http("catalog")
        .post("", &json!({ "id": event.dataset, "title": title }))
        .await?;
    if ctx.granted("metadata.write", &event.dataset).await {
        let id = record.field("/id")?;
        let patch = json!({ "identifier": { "catalog": id } });
        ctx.metadata()
            .patch(&event.dataset, &patch, &dataset.revision)
            .await?;
    }
    Ok(())
}

aruna_plugin::export!(dataset_created);
