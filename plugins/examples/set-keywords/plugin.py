"""Example plugin: adds keywords to a dataset, run by hand."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna


@aruna.on("manual")
def add_keywords(form):
    words = [word.strip() for word in form.keywords.split(",") if word.strip()]
    if not words:
        raise aruna.InvalidInput("give at least one keyword")
    dataset = form.dataset
    kept = [] if form.replace else dataset.keywords
    dataset.keywords = kept + [word for word in words if word not in kept]
    dataset.save()
    return {"dataset": dataset.id, "keywords": dataset.keywords}
