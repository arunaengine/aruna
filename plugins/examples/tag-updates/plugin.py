"""Example plugin: adds a keyword to every dataset that changes."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna


@aruna.on("dataset_updated")
def tag(dataset):
    # The plugin's own save is a change too; that run finds the keyword and writes nothing.
    if aruna.settings.tag in dataset.keywords:
        return None
    dataset.keywords.append(aruna.settings.tag)
    dataset.save()
    return {"tagged": dataset.id}
