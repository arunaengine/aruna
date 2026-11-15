//! Plugin handlers: `aruna::plugin!` lists the function each trigger in plugin.toml runs.

use aruna::Result;

fn hello() -> Result<String> {
    Ok(format!("hello from {}", aruna::run().install))
}

aruna::plugin! { manual => hello }
