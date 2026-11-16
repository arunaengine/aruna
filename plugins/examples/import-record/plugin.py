"""Example plugin: imports a Zenodo record as a new dataset, run by hand."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna

zenodo = aruna.service("zenodo")


@aruna.on("manual")
def import_record(form):
    record_id = form.record.strip()
    if not record_id.isdigit():
        raise aruna.InvalidInput("a Zenodo record id has only digits")
    record = zenodo.get(path=record_id).json()
    meta = record["metadata"]
    dataset = aruna.datasets.create(
        f"zenodo/{record_id}",
        name=meta["title"],
        description=meta.get("description", ""),
        keywords=meta.get("keywords", []),
        license=meta.get("license", {}).get("id"),
        datePublished=meta.get("publication_date"),
        author=[creator["name"] for creator in meta.get("creators", [])],
        identifiers={"doi": record["doi"], "zenodo": record_id},
    )
    return {"dataset": dataset.id, "title": dataset.name}
