"""Example plugin: posts each new dataset to the catalog and records the catalog id on it."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna

catalog = aruna.service("catalog")


@aruna.on("dataset_created")
def announce(dataset):
    record = catalog.post(json={"id": dataset.id, "title": dataset.name})
    dataset.identifiers["catalog"] = record["id"]
    dataset.save()


@aruna.on("manual")
def announce_by_hand(form):
    announce(form.dataset)
