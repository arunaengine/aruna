"""Example plugin: uploads one file of a dataset to a new Zenodo draft and records the draft id."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna

zenodo = aruna.service("zenodo")
API = "https://zenodo.org/api/"


@aruna.on("manual")
def deposit(form):
    dataset, source = form.dataset, form.file
    metadata = {"title": dataset.name, "upload_type": "dataset", "description": dataset.description}
    draft = zenodo.post("deposit/depositions", json={"metadata": metadata}).json()
    name = source.path.rsplit("/", 1)[-1]
    job = zenodo.upload(
        source.path,
        path=f"{draft['links']['bucket'].removeprefix(API)}/{name}",
        method="PUT",
        version=source.version,
    )
    aruna.state[f"job/{job.id}"] = {"dataset": dataset.id, "draft": draft["id"]}
    return {"draft": draft["id"], "job": job.id}


@aruna.on("job_finished")
def record(job):
    started = aruna.state.get(f"job/{job.id}")
    if started is None:
        return None
    if not job.ok:
        raise aruna.Fail(f"the upload to Zenodo draft {started['draft']} ended as {job.state}")
    dataset = aruna.datasets.get(started["dataset"])
    dataset.identifiers["zenodo_draft"] = str(started["draft"])
    # Zenodo's answer to the upload is kept with the job.
    dataset["zenodoChecksum"] = job.reply.json().get("checksum")
    dataset.save()
    del aruna.state[f"job/{job.id}"]
