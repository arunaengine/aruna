"""Example plugin: fetches the FASTQ files of an ENA run into a bucket path and links them to a
dataset."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import re

import aruna

ena = aruna.service("ena")
files = aruna.service("ena_files")
FILE_HOST = "ftp.sra.ebi.ac.uk/vol1/"


@aruna.on("manual")
def fetch(form):
    run = form.run.strip()
    if not re.fullmatch(r"[EDS]RR[0-9]+", run):
        raise aruna.InvalidInput(f"{run} is not a run accession")
    fields = "fastq_ftp,fastq_bytes,fastq_md5"
    query = {"accession": run, "result": "read_run", "fields": fields, "format": "json"}
    rows = ena.get("filereport", query=query).json()
    if not rows:
        raise aruna.InvalidInput(f"ENA has no files for {run}")
    links, sizes, sums = (rows[0][name].split(";") for name in fields.split(","))
    for link, size, md5 in zip(links, sizes, sums):
        name = link.rsplit("/", 1)[-1]
        # The bytes go from ENA into the bucket on the node; the plugin only starts the job.
        job = files.download(
            link.removeprefix(FILE_HOST),
            to=f"{aruna.settings.bucket}/{run}/{name}",
            size_limit=int(size),
            checksums={"md5": md5},
        )
        aruna.state[f"job/{job.id}"] = {"dataset": form.dataset.id, "name": name}
    return {"files": len(links)}


@aruna.on("job_finished")
def link(job):
    started = aruna.state.get(f"job/{job.id}")
    if started is None:
        return None
    if job.ok:
        dataset = aruna.datasets.get(started["dataset"])
        dataset.add_file(job.path, id=f"reads/{started['name']}")
    else:
        aruna.log.warn(f"{started['name']} was not fetched: {job.state}, {job.message}")
    # Removed last, so a rerun after a crash still finds the mapping.
    del aruna.state[f"job/{job.id}"]
