//! Example plugin: posts each new dataset to the catalog and records the catalog id on it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna::{Dataset, Form, Result};
use serde::Deserialize;
use serde_json::json;

/// The part of the catalog's answer the plugin uses.
#[derive(Deserialize)]
struct Record {
    id: String,
}

fn announce(mut dataset: Dataset) -> Result<()> {
    let body = json!({ "id": dataset.id, "title": dataset.name });
    let record: Record = aruna::service("catalog").post(&body)?.json()?;
    dataset
        .identifiers
        .insert("catalog".into(), record.id.into());
    dataset.save()?;
    Ok(())
}

fn announce_by_hand(form: Form) -> Result<()> {
    announce(form.dataset("dataset")?)
}

aruna::plugin! {
    dataset_created => announce,
    manual => announce_by_hand,
}
