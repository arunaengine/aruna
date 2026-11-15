"""Example plugin: reports how many objects a bucket path holds, their size and the largest one."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna


@aruna.on("manual")
def report(form):
    objects = list(aruna.objects.list(form.prefix))
    largest = max(objects, key=lambda item: item.size, default=None)
    return {
        "objects": len(objects),
        "bytes": sum(item.size for item in objects),
        "largest": largest.path if largest else None,
    }
