pub mod parse;
pub mod python;
pub mod stream;

use parse::{PageviewsRow, ParseError, parse_line};
use std::path::Path;
use stream::{StreamError, from_file, from_http};
use url::Url;

pub type RowIterator = Box<dyn Iterator<Item = Result<PageviewsRow, RowError>> + Send>;

#[derive(Debug, thiserror::Error)]
pub enum RowError {
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),

    #[error("IO error while reading a line: {0}")]
    Io(#[from] std::io::Error),
}

fn line_mapper(line: std::io::Result<String>) -> Result<PageviewsRow, RowError> {
    let line = line.map_err(RowError::Io)?;
    parse_line(&line).map_err(RowError::Parse)
}

/// Decompress, stream, and parse lines from a local pageviews file
///
/// The function will return a `StreamError` if it fails to read the file.
/// Otherwise, it returns a `PageviewsRow` iterator, yielding a `RowError`
/// for each line it failed to parse, either due to IO issues or a parsing
/// error.
pub fn parse_lines_from_file(path: &Path) -> Result<RowIterator, StreamError> {
    let lines = from_file(path)?;
    Ok(Box::new(lines.map(line_mapper)))
}

/// Decompress, stream, and parse lines from a remote pageviews file
///
/// The function will return a `StreamError` if it fails to read the file.
/// Otherwise, it returns a `PageviewsRow` iterator, yielding a `RowError`
/// for each line it failed to parse, either due to IO issues or a parsing
/// error.
pub fn parse_lines_from_http(url: Url) -> Result<RowIterator, StreamError> {
    let lines = from_http(url)?;
    Ok(Box::new(lines.map(line_mapper)))
}
