//! Plugin handlers: each trigger in plugin.toml runs the function export! lists for it.

use aruna_plugin::{Context, Event, Result};

async fn run(ctx: Context, _event: Event) -> Result<String> {
    Ok(format!("hello from {}", ctx.install()))
}

aruna_plugin::export! { manual => run }
