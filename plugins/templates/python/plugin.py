"""Plugin handlers: each trigger in plugin.toml runs the function registered for it."""

import aruna


@aruna.on("manual")
def hello():
    return f"hello from {aruna.run.install}"
