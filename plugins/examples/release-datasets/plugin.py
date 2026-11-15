"""Example plugin: creates a dataset for each published GitHub release."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna


@aruna.on("webhook")
def published(request):
    event = request.json()
    if event.get("action") != "published":
        return None
    release, repository = event["release"], event["repository"]["full_name"]
    # The path names the dataset, so a delivery sent twice answers the same dataset.
    dataset = aruna.datasets.create(
        f"releases/{repository}/{release['tag_name']}",
        name=release["name"] or release["tag_name"],
        description=release.get("body") or "",
        url=release["html_url"],
        identifiers={"github_release": str(release["id"])},
    )
    return {"dataset": dataset.id, "delivery": request.delivery_id}
