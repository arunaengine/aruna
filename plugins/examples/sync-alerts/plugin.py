"""Example plugin: tells a chat channel when a sync relationship starts failing."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna

chat = aruna.service("chat")


@aruna.on("sync_completed", "sync_failed")
def alert(event):
    key = f"failures/{event.relationship}"
    failures = aruna.state.update(key, lambda count: 0 if event.ok else count + 1, 0)
    # Only the first failure after a success is sent, so a failing sync does not flood the channel.
    if failures == 1:
        chat.post(json={"text": f"The sync of {event.path} failed (job {event.job})"})
    return {"failures": failures}
