"""Example plugin: copies every new upload below one bucket path into an archive bucket path."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna


@aruna.on("data_uploaded")
def archive(upload):
    incoming, archive_path = aruna.settings.incoming, aruna.settings.archive
    # A copy is an upload too; one into an archive inside the watched path is not copied again.
    if not upload.path.startswith(incoming) or upload.path.startswith(archive_path):
        return None
    target = archive_path + upload.path.removeprefix(incoming)
    job = aruna.objects.copy(upload.path, target, version=upload.version)
    return {"job": job.id, "target": target}
