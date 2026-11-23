"""Example plugin: maps reads to a reference genome in a container and records the result."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import re

import aruna

mapper = aruna.task("map")


@aruna.on("manual")
def start(form):
    dataset, reference = form.dataset, form.reference.strip()
    if not re.fullmatch(r"GCF_[0-9]{9}\.[0-9]+", reference):
        raise aruna.InvalidInput(f"{reference} is not a RefSeq assembly accession")
    reads = [item for item in aruna.objects.list(form.reads) if item.path.endswith(".fastq.gz")]
    if not reads:
        raise aruna.InvalidInput(f"no .fastq.gz files below {form.reads}")
    samples = "".join(f"{item.path.rsplit('/', 1)[-1]}\t{dataset.name}\n" for item in reads)
    results = f"{aruna.settings.results}/{dataset.id}/{reference}/"
    job = mapper.start(
        parameters={"reference": reference},
        inputs={"/in/reads/": form.reads},
        files={"/in/samples.tsv": samples},
        outputs={"/out/results/": results},
    )
    started = {"dataset": dataset.id, "reference": reference, "results": results}
    aruna.state[f"job/{job.id}"] = started
    return {"job": job.id, "reads": len(reads)}


@aruna.on("job_finished")
def record(job):
    started = aruna.state.get(f"job/{job.id}")
    if started is None:
        return None
    if not job.ok:
        raise aruna.Fail(f"mapping ended as {job.state} (exit code {job.exit_code}): {job.message}")
    dataset = aruna.datasets.get(started["dataset"])
    for output in job.outputs():
        name = output.path.rsplit("/", 1)[-1]
        dataset.add_file(output, id=f"mapping/{started['reference']}/{name}")
    mapping = {"reference": started["reference"]}
    report = job.report_json()
    if report is None:
        aruna.log.warn("the task report was left out, so the numbers are missing")
    else:
        mapping |= {"reads": report["reads"], "mappedPercent": report["mapped_percent"]}
    dataset["mapping"] = mapping
    dataset.save()
    # Removed last, so a rerun after a crash still finds the mapping.
    del aruna.state[f"job/{job.id}"]
