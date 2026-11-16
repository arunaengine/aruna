"""Example plugin: sets a license on every dataset that has none, run by hand."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna


@aruna.on("manual")
def fill_all():
    filled, skipped = [], []
    for dataset in aruna.datasets.list():
        if dataset.license:
            continue
        dataset.license = aruna.settings.license
        try:
            dataset.save()
            filled.append(dataset.id)
        except aruna.NotAllowed:
            # The installer may allow writes to some datasets only.
            skipped.append(dataset.id)
    return {"filled": filled, "skipped": skipped}
