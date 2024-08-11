pub mod parse;
pub mod stream;

#[cfg(feature = "python")]
pub mod python;

use parse::{PageviewsRow, ParseError, parse_line};
use std::path::Path;
use stream::{StreamError, from_file, from_http};
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum RowError {
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),

    #[error("IO error while reading a line: {0}")]
    Io(#[from] std::io::Error),
}

/// Decompress, stream, and parse lines from a local pageviews file
///
/// The function will return a `StreamError` if it fails to read the file.
/// Otherwise, it returns a `PageviewsRow` iterator, yielding a `RowError`
/// for each line it failed to parse, either due to IO issues or a parsing
/// error.
pub fn parse_lines_from_file(
    path: &Path,
) -> Result<impl Iterator<Item = Result<PageviewsRow, RowError>>, StreamError> {
    Ok(from_file(path)?.map(|line_result| {
        let line = line_result.map_err(RowError::Io)?;
        parse_line(&line).map_err(RowError::Parse)
    }))
}

/// Decompress, stream, and parse lines from a remote pageviews file
///
/// The function will return a `StreamError` if it fails to read the file.
/// Otherwise, it returns a `PageviewsRow` iterator, yielding a `RowError`
/// for each line it failed to parse, either due to IO issues or a parsing
/// error.
pub fn parse_lines_from_http(
    url: &Url,
) -> Result<impl Iterator<Item = Result<PageviewsRow, RowError>>, StreamError> {
    Ok(from_http(url)?.map(|line_result| {
        let line = line_result.map_err(RowError::Io)?;
        parse_line(&line).map_err(RowError::Parse)
    }))
}
