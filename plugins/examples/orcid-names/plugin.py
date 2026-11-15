"""Example plugin: adds an author to a dataset from their public ORCID record."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna

orcid = aruna.service("orcid")


@aruna.on("manual")
def add_author(form):
    orcid_id = form.orcid.strip().removeprefix("https://orcid.org/")
    person = orcid.get(f"{orcid_id}/person", headers={"accept": "application/json"}).json()
    name = f"{person['name']['given-names']['value']} {person['name']['family-name']['value']}"
    author = {"@id": f"https://orcid.org/{orcid_id}", "name": name}
    dataset = form.dataset
    authors = dataset.get("author") or []
    if author not in authors:
        dataset["author"] = [*authors, author]
        dataset.save()
    return {"dataset": dataset.id, "author": name}
