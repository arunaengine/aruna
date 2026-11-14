"""Plugin handlers: each trigger in plugin.toml runs the function registered for it with @on."""

from aruna import WitWorld, on  # noqa: F401  WitWorld is the component's entry point


@on("manual")
def run(ctx, event):
    return f"hello from {ctx.install}"
