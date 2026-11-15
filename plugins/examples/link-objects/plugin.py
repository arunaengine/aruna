"""Example plugin: links every object below a bucket path into a dataset as a file."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna


@aruna.on("manual")
def link(form):
    dataset = form.dataset
    linked = {file["@id"] for file in dataset.files()}
    added, total = 0, 0
    for item in aruna.objects.list(form.prefix):
        total += item.size
        file_id = f"objects/{item.path}"
        if file_id in linked:
            continue
        dataset.add_file(item, id=file_id)
        added += 1
    return {"added": added, "bytes": total}
