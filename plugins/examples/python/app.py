"""Example plugin: posts each new dataset's title to a catalog, then records the catalog id."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json

from aruna import WitWorld, on  # noqa: F401  WitWorld is the component's entry point
from componentize_py_types import Err
from wit_world.imports import metadata_read, metadata_write, services, types


@on("dataset_created", "manual")
def dataset_created(ctx, event):
    if event["kind"] == "manual":
        given = event["input"]
        dataset = given["dataset"] if isinstance(given, dict) else given
    else:
        dataset = event["resource"]
    doc = metadata_read.get(dataset)
    title = json.loads(doc.json).get("name")
    body = json.dumps({"id": dataset, "title": title}).encode()
    headers = [types.Field("content-type", "application/json")]
    request = services.Request("catalog", services.Method.POST, "", [], headers, body, None)
    reply = services.send(request)
    record = json.loads(bytes(reply.body) or b"{}")
    if "id" not in record:
        raise Err(types.Failure(types.FailureKind.PERMANENT, "the response has no field `id`"))
    patch = json.dumps({"identifier": {"catalog": record["id"]}})
    metadata_write.patch(dataset, patch, doc.revision, None)
