//! Embeds the tracked demonstration environment for the startup guard. A build
//! context without the file, such as the container image, embeds nothing and
//! the guard then has no shipped value to match.
use std::path::Path;

fn main() {
    let shipped = Path::new("..").join(".env");
    println!("cargo:rerun-if-changed=../.env");
    let contents = std::fs::read_to_string(&shipped).unwrap_or_default();
    let out =
        Path::new(&std::env::var("OUT_DIR").expect("cargo sets OUT_DIR")).join("shipped_env.rs");
    std::fs::write(out, format!("const SHIPPED_ENV: &str = {contents:?};\n"))
        .expect("write the shipped environment");
}
