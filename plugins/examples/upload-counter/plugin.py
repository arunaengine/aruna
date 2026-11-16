"""Example plugin: counts the uploads and bytes of each bucket in the plugin's state."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna


@aruna.on("data_uploaded")
def count(upload):
    bucket = upload.path.split("/", 1)[0]

    def add(totals):
        return {"uploads": totals["uploads"] + 1, "bytes": totals["bytes"] + upload.size}

    # `update` reads, applies `add` and writes again if another run changed the value meanwhile.
    return aruna.state.update(f"bucket/{bucket}", add, {"uploads": 0, "bytes": 0})
