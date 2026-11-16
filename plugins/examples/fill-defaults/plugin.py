"""Example plugin: fills a missing license and publisher on new datasets, or on one by hand."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna


@aruna.on("dataset_created")
def fill(dataset):
    if not dataset.license:
        dataset.license = aruna.settings.license
    if not dataset.get("publisher"):
        dataset["publisher"] = aruna.settings.publisher
    dataset.keywords = sorted({word.strip().lower() for word in dataset.keywords if word.strip()})
    return {"changed": dataset.save()}


@aruna.on("manual")
def fill_by_hand(form):
    return fill(form.dataset)
