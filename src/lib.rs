pub mod filter;
pub mod parse;
mod store;
pub mod stream;

#[cfg(feature = "pyo3")]
pub mod python;

use crate::parse::{Pageviews, ParseError, parse_line};
use filter::{Filter, post_filter, pre_filter};
use std::path::PathBuf;
use store::{arrow_chunks_from_structs, parquet_from_arrow};
use stream::{StreamError, lines_from_file, lines_from_url};
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
pub fn stream_from_url(url: Url, filter: &Filter) -> Result<RowIterator, StreamError> {
    Ok(Box::new(
        lines_from_url(url)?
            .filter(pre_filter(filter))
            .map(|line| line.map_err(ParseError::ReadError).and_then(parse_line))
            .filter(post_filter(filter)),
    ))
}

/// Stores a filtered and parsed pageviews file as a parquet file.
///
/// By default, the batches will equal the default parquet row group size,
/// which causes memory requirements of about 100MB. Lower this to sacrifice
/// performance for lower memory requirements, or vice versa.
pub fn parquet_from_file(
    input_path: PathBuf,
    output_path: PathBuf,
    filter: &Filter,
    batch_size: Option<usize>,
) -> Result<(), StreamError> {
    let iterator = lines_from_file(&input_path)?
        .filter(pre_filter(filter))
        .map(|line| line.map_err(ParseError::ReadError).and_then(parse_line))
        .filter(post_filter(filter));

    parquet_from_arrow(
        &output_path,
        arrow_chunks_from_structs(iterator, batch_size),
    )?;

    Ok(())
}

/// Stores a filtered and parsed pageviews file as a parquet file.
///
/// By default, the batches will equal the default parquet row group size,
/// which causes memory requirements of about 100MB. Lower this to sacrifice
/// performance for lower memory requirements, or vice versa.
pub fn parquet_from_url(
    url: Url,
    output_path: PathBuf,
    filter: &Filter,
    batch_size: Option<usize>,
) -> Result<(), StreamError> {
    let iterator = lines_from_url(url)?
        .filter(pre_filter(filter))
        .map(|line| line.map_err(ParseError::ReadError).and_then(parse_line))
        .filter(post_filter(filter));

    parquet_from_arrow(
        &output_path,
        arrow_chunks_from_structs(iterator, batch_size),
    )?;

    Ok(())
}
