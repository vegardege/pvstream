pub mod parse;
mod sift;
pub mod stream;

#[cfg(feature = "pyo3")]
pub mod python;

use crate::parse::{Pageviews, ParseError, parse_line};
use regex::Regex;
use sift::is_valid_line;
use std::path::PathBuf;
use stream::{StreamError, lines_from_file, lines_from_http};
use url::Url;

pub type RowIterator = Box<dyn Iterator<Item = Result<Pageviews, ParseError>> + Send>;

/// Decompress, stream, and parse lines from a local pageviews file
///
/// The function will return a `StreamError` if it fails to read the file.
/// Otherwise, it returns a `Pageviews` iterator, yielding a `ParseError`
/// for each line it fails to parse, either due to IO issues or a parsing
/// error.
pub fn stream_from_file(
    path: PathBuf,
    line_regex: Option<Regex>,
) -> Result<RowIterator, StreamError> {
    Ok(Box::new(
        lines_from_file(&path)?
            .filter(is_valid_line(line_regex))
            .map(|line| line.map_err(ParseError::ReadError).and_then(parse_line)),
    ))
}

/// Decompress, stream, and parse lines from a remote pageviews file
///
/// The function will return a `StreamError` if it fails to read the file.
/// Otherwise, it returns a `Pageviews` iterator, yielding a `ParseError`
/// for each line it fails to parse, either due to IO issues or a parsing
/// error.
pub fn stream_from_http(url: Url, line_regex: Option<Regex>) -> Result<RowIterator, StreamError> {
    Ok(Box::new(
        lines_from_http(url)?
            .filter(is_valid_line(line_regex))
            .map(|line| line.map_err(ParseError::ReadError).and_then(parse_line)),
    ))
}
