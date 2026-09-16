//! Human output helpers: compact tables and key/value blocks.

use std::io::Write;

use tachyon_serverless_api_types::Timestamp;

use crate::error::CliError;

/// Where command output goes. `json` selects raw server output.
pub struct Printer<'a> {
    pub out: &'a mut dyn Write,
    pub err: &'a mut dyn Write,
    pub json: bool,
}

impl Printer<'_> {
    /// Print raw JSON (as received) followed by a newline.
    pub fn raw(&mut self, body: &str) -> Result<(), CliError> {
        let trimmed = body.trim_end();
        writeln!(self.out, "{trimmed}")?;
        Ok(())
    }

    pub fn line(&mut self, s: impl AsRef<str>) -> Result<(), CliError> {
        writeln!(self.out, "{}", s.as_ref())?;
        Ok(())
    }

    pub fn note(&mut self, s: impl AsRef<str>) -> Result<(), CliError> {
        writeln!(self.err, "{}", s.as_ref())?;
        Ok(())
    }

    pub fn table(&mut self, table: &Table) -> Result<(), CliError> {
        write!(self.out, "{}", table.render())?;
        Ok(())
    }

    pub fn kv(&mut self, rows: &[(&str, String)]) -> Result<(), CliError> {
        let width = rows.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
        for (k, v) in rows {
            writeln!(self.out, "{k:<width$}  {v}")?;
        }
        Ok(())
    }

    /// Pretty-print a JSON value to stdout.
    pub fn pretty_json(&mut self, v: &serde_json::Value) -> Result<(), CliError> {
        writeln!(self.out, "{}", serde_json::to_string_pretty(v)?)?;
        Ok(())
    }
}

/// A left-aligned text table.
#[derive(Debug, Default)]
pub struct Table {
    header: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    pub fn new(header: &[&str]) -> Self {
        Self {
            header: header.iter().map(|s| s.to_string()).collect(),
            rows: Vec::new(),
        }
    }

    pub fn row(&mut self, cells: Vec<String>) -> &mut Self {
        self.rows.push(cells);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn render(&self) -> String {
        let cols = self.header.len();
        let mut widths: Vec<usize> = self.header.iter().map(|h| h.chars().count()).collect();
        for r in &self.rows {
            for (i, c) in r.iter().enumerate().take(cols) {
                widths[i] = widths[i].max(c.chars().count());
            }
        }
        let mut s = String::new();
        let fmt_row = |cells: &[String]| -> String {
            let mut line = String::new();
            for (i, width) in widths.iter().enumerate() {
                let cell = cells.get(i).map(String::as_str).unwrap_or("");
                line.push_str(cell);
                if i + 1 < cols {
                    let pad = width - cell.chars().count();
                    line.extend(std::iter::repeat_n(' ', pad + 2));
                }
            }
            line.trim_end().to_string()
        };
        s.push_str(&fmt_row(&self.header));
        s.push('\n');
        for r in &self.rows {
            s.push_str(&fmt_row(r));
            s.push('\n');
        }
        s
    }
}

pub fn ts(t: &Timestamp) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

pub fn opt_ts(t: &Option<Timestamp>) -> String {
    t.as_ref().map(ts).unwrap_or_else(|| "-".into())
}

pub fn opt<T: ToString>(v: &Option<T>) -> String {
    v.as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "-".into())
}

pub fn opt_ms(v: &Option<u64>) -> String {
    v.map(|n| format!("{n} ms")).unwrap_or_else(|| "-".into())
}

/// Shorten a string for table cells.
pub fn ellipsize(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let cut: String = s.chars().take(max.saturating_sub(1)).collect();
        format!("{cut}…")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_aligns_columns() {
        let mut t = Table::new(&["ID", "NAME"]);
        t.row(vec!["fn_1".into(), "hello".into()]);
        t.row(vec!["fn_123456".into(), "x".into()]);
        let s = t.render();
        assert_eq!(s, "ID         NAME\nfn_1       hello\nfn_123456  x\n");
    }

    #[test]
    fn ellipsize_keeps_short_strings() {
        assert_eq!(ellipsize("abc", 5), "abc");
        assert_eq!(ellipsize("abcdefgh", 5), "abcd…");
    }
}
