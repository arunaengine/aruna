//! Prints results as aligned text tables for people or as JSON for scripts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::CliError;
use serde::Serialize;
use std::io::Write;

/// Where command results go, and in which form.
pub struct Output<'a> {
    pub out: &'a mut dyn Write,
    pub json: bool,
}

impl Output<'_> {
    pub fn json<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CliError> {
        serde_json::to_writer_pretty(&mut self.out, value)?;
        writeln!(self.out)?;
        Ok(())
    }

    pub fn line(&mut self, text: impl AsRef<str>) -> Result<(), CliError> {
        writeln!(self.out, "{}", text.as_ref())?;
        Ok(())
    }

    /// Prints rows under a header, each column as wide as its widest cell.
    pub fn table(&mut self, header: &[&str], rows: &[Vec<String>]) -> Result<(), CliError> {
        let text = render_table(header, rows);
        self.out.write_all(text.as_bytes())?;
        Ok(())
    }

    /// The JSON form of `value`, or the table built from it.
    pub fn listing<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
        header: &[&str],
        rows: Vec<Vec<String>>,
    ) -> Result<(), CliError> {
        match self.json {
            true => self.json(value),
            false if rows.is_empty() => self.line("(none)"),
            false => self.table(header, &rows),
        }
    }
}

pub fn render_table(header: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = header.iter().map(|cell| cell.chars().count()).collect();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            if let Some(width) = widths.get_mut(index) {
                *width = (*width).max(cell.chars().count());
            }
        }
    }
    let mut text = String::new();
    let header: Vec<String> = header.iter().map(|cell| cell.to_string()).collect();
    for row in std::iter::once(&header).chain(rows) {
        let cells: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect();
        text.push_str(cells.join("  ").trim_end());
        text.push('\n');
    }
    text
}

/// A byte count in binary units, such as `1.5 MiB`.
pub fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    match unit {
        0 => format!("{bytes} B"),
        _ => format!("{value:.1} {}", UNITS[unit]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligns_columns() {
        let rows = vec![
            vec!["a".to_string(), "long value".to_string()],
            vec!["longer".to_string(), "b".to_string()],
        ];
        let text = render_table(&["NAME", "VALUE"], &rows);
        assert_eq!(text, "NAME    VALUE\na       long value\nlonger  b\n");
    }

    #[test]
    fn formats_sizes() {
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(1536), "1.5 KiB");
        assert_eq!(human_size(5 * 1024 * 1024), "5.0 MiB");
    }
}
