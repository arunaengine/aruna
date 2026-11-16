// Example plugin: posts each new dataset to the catalog and records the catalog id on it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
import aruna from "aruna";

const catalog = aruna.service("catalog");

function announce(dataset) {
  const record = catalog.post({ json: { id: dataset.id, title: dataset.name } });
  const { id } = record.json();
  if (!id) throw new aruna.Fail("the catalog answered without an id");
  dataset.identifiers.catalog = id;
  dataset.save();
}

aruna.on("dataset_created", announce);
aruna.on("manual", (form) => announce(form.dataset));
