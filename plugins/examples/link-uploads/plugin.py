"""Example plugin: links every new upload below a bucket path into one dataset as a file."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna


@aruna.on("data_uploaded")
def link(upload):
    if not upload.path.startswith(aruna.settings.prefix):
        return None
    dataset = aruna.datasets.get(aruna.settings.dataset)
    dataset.add_file(upload)
    return {"dataset": dataset.id, "file": upload.url}
