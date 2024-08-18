mod filter;
pub mod parse;
mod storage;
pub mod stream;

#[cfg(feature = "pyo3")]
pub mod python;

use crate::parse::{Pageviews, ParseError, parse_line};
use filter::{Filter, post_filter, pre_filter};
use std::path::PathBuf;
use stream::{StreamError, lines_from_file, lines_from_http};
use url::Url;

pub type RowIterator = Box<dyn Iterator<Item = Result<Pageviews, ParseError>> + Send + 'static>;

/// Decompress, stream, and parse lines from a local pageviews file
///
/// The function will return a `StreamError` if it fails to read the file.
/// Otherwise, it returns a `Pageviews` iterator, yielding a `ParseError`
/// for each line it fails to parse, either due to IO issues or a parsing
/// error.
pub fn stream_from_file(path: PathBuf, filter: &Filter) -> Result<RowIterator, StreamError> {
    Ok(Box::new(
        lines_from_file(&path)?
            .filter(pre_filter(filter))
            .map(|line| line.map_err(ParseError::ReadError).and_then(parse_line))
            .filter(post_filter(filter)),
    ))
}

/// Decompress, stream, and parse lines from a remote pageviews file
///
/// The function will return a `StreamError` if it fails to read the file.
/// Otherwise, it returns a `Pageviews` iterator, yielding a `ParseError`
/// for each line it fails to parse, either due to IO issues or a parsing
/// error.
pub fn stream_from_http(url: Url, filter: &Filter) -> Result<RowIterator, StreamError> {
    Ok(Box::new(
        lines_from_http(url)?
            .filter(pre_filter(filter))
            .map(|line| line.map_err(ParseError::ReadError).and_then(parse_line))
            .filter(post_filter(filter)),
    ))
}
