"""Plugin handlers: each trigger in plugin.toml calls one of these by name."""

from aruna import WitWorld, handler  # noqa: F401  WitWorld is the component's entry point


@handler
def run(ctx, event):
    return f"hello from {ctx.install}"
