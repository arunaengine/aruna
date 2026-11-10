"""Example plugin: posts each new dataset's title to a catalog, then records the catalog id."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json

from aruna import WitWorld, handler  # noqa: F401  WitWorld is the component's entry point
from componentize_py_types import Err
from wit_world.imports import connections, grants, metadata_read, metadata_write, types


@handler
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
    request = connections.Request("catalog", connections.Method.POST, "", [], headers, body, None)
    reply = connections.send(request)
    if grants.granted("metadata.write", dataset):
        fields = json.loads(reply.json or "{}")
        if "/id" not in fields:
            raise Err(types.Failure(types.FailureKind.PERMANENT, "the response has no field `/id`"))
        patch = json.dumps({"identifier": {"catalog": fields["/id"]}})
        metadata_write.patch(dataset, patch, doc.revision, None)
