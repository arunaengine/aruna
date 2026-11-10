//! Plugin handlers: each trigger in plugin.toml calls one of the functions listed in export!.

use aruna_plugin::{Context, Event, Result};

async fn run(ctx: Context, _event: Event) -> Result<String> {
    Ok(format!("hello from {}", ctx.install()))
}

aruna_plugin::export!(run);
