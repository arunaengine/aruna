"""Example plugin: tells a chat channel each day which datasets still have no description."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import aruna

chat = aruna.service("chat")


@aruna.on("timer")
def report(tick):
    reported = set(aruna.state.get("reported", []))
    missing = [d for d in aruna.datasets.list() if not d.description and d.id not in reported]
    if missing:
        names = ", ".join(d.name or d.id for d in missing)
        message = f"Datasets without a description on {tick:%Y-%m-%d}: {names}"
        chat.post(json={"channel_id": aruna.settings.channel, "message": message})
        aruna.state.update("reported", lambda old: sorted(set(old) | {d.id for d in missing}), [])
    return {"reported": len(missing)}
