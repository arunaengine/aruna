"""Example plugin: tells a chat channel when a sync relationship starts failing."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna

chat = aruna.service("chat")


@aruna.on("sync_completed", "sync_failed")
def alert(event):
    key = f"failures/{event.relationship}"
    failures = aruna.state.update(key, lambda count: 0 if event.ok else count + 1, 0)
    # One message per run of failures, so a failing sync does not flood the channel.
    sent = f"sent/{event.relationship}"
    if event.ok:
        del aruna.state[sent]
    elif sent not in aruna.state:
        chat.post(json={"text": f"The sync of {event.path} failed (job {event.job})"})
        # Kept only once the post succeeded, so a refused post is tried again at the next failure.
        aruna.state[sent] = event.job
    return {"failures": failures}
