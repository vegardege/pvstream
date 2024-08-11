pub mod parse;
pub mod python;
pub mod stream;

use parse::{PageviewsRow, ParseError, parse_line};
use std::path::Path;
use stream::{StreamError, from_file, from_http};
use thiserror::Error;
use url::Url;

pub type RowIterator = Box<dyn Iterator<Item = Result<PageviewsRow, RowError>> + Send>;

#[derive(Debug, Error)]
pub enum RowError {
    #[error(transparent)]
    Parse(#[from] ParseError),

    #[error(transparent)]
    Io(#[from] StreamError),
}

/// Decompress, stream, and parse lines from a local pageviews file
///
/// The function will return a `StreamError` if it fails to read the file.
/// Otherwise, it returns a `PageviewsRow` iterator, yielding a `RowError`
/// for each line it failed to parse, either due to IO issues or a parsing
/// error.
pub fn stream_from_file(path: &Path) -> Result<RowIterator, RowError> {
    let lines = from_file(path)?;
    Ok(Box::new(lines.map(|line| Ok(parse_line(line?)?))))
}

/// Decompress, stream, and parse lines from a remote pageviews file
///
/// The function will return a `StreamError` if it fails to read the file.
/// Otherwise, it returns a `PageviewsRow` iterator, yielding a `RowError`
/// for each line it failed to parse, either due to IO issues or a parsing
/// error.
pub fn stream_from_http(url: Url) -> Result<RowIterator, RowError> {
    let lines = from_http(url)?;
    Ok(Box::new(lines.map(|line| Ok(parse_line(line?)?))))
}
