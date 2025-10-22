//! # pvstream
//!
//! Stream download, parse, and filter Wikimedia pageviews files.
//!
//! This library provides efficient streaming access to Wikimedia's hourly pageview
//! dumps. It can download and parse multi-gigabyte compressed files on-the-fly
//! without storing the entire file in memory.
//!
//! ## Features
//!
//! - **Streaming parsing**: Process files as they download, minimizing memory usage
//! - **Flexible filtering**: Filter by language, domain, page title (regex), view counts, and more
//! - **Performance optimization**: Apply regex filters before parsing for maximum efficiency
//! - **Parquet export**: Convert filtered data to Parquet format for analysis
//! - **Rust and Python**: Native Rust library with Python bindings via PyO3
//!
//! ## Quick Start
//!
//! ```no_run
//! use pvstream::{stream_from_file, filter::FilterBuilder};
//! use std::path::PathBuf;
//!
//! let filter = FilterBuilder::new()
//!     .domain_codes(["en.m"])
//!     .page_title("Rust")
//!     .build();
//!
//! let rows = stream_from_file(PathBuf::from("pageviews.gz"), &filter).unwrap();
//! for result in rows {
//!     match result {
//!         Ok(pageview) => println!("{:?}", pageview),
//!         Err(e) => eprintln!("Error: {:?}", e),
//!     }
//! }
//! ```

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

/// Iterator type returned by streaming functions.
///
/// Yields `Result<Pageviews, ParseError>` for each line in the pageviews file.
pub type RowIterator = Box<dyn Iterator<Item = Result<Pageviews, ParseError>> + Send + 'static>;

/// Decompress, stream, and parse lines from a local pageviews file
///
/// The function will return a `StreamError` if it fails to read the file.
/// Otherwise, it returns a `Pageviews` iterator, yielding a `ParseError`
/// for each line it fails to parse, either due to IO issues or a parsing
/// error.
///
/// # Example
///
/// ```no_run
/// use pvstream::{stream_from_file, filter::FilterBuilder};
/// use std::path::PathBuf;
///
/// let filter = FilterBuilder::new().domain_codes(["en"]).build();
/// let rows = stream_from_file(PathBuf::from("pageviews-20240818-080000.gz"), &filter)?;
///
/// for result in rows {
///     println!("{:?}", result?);
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
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
///
/// # Example
///
/// ```no_run
/// use pvstream::{stream_from_url, filter::FilterBuilder};
/// use url::Url;
///
/// let url = Url::parse("https://dumps.wikimedia.org/other/pageviews/2024/2024-08/pageviews-20240818-080000.gz")?;
/// let filter = FilterBuilder::new().languages(["ja"]).build();
/// let rows = stream_from_url(url, &filter)?;
///
/// for result in rows.take(10) {
///     println!("{:?}", result?);
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn stream_from_url(url: Url, filter: &Filter) -> Result<RowIterator, StreamError> {
    Ok(Box::new(
        lines_from_url(url)?
            .filter(pre_filter(filter))
            .map(|line| line.map_err(ParseError::ReadError).and_then(parse_line))
            .filter(post_filter(filter)),
    ))
}

/// Parse a local pageviews file and write filtered results to a Parquet file.
///
/// This function processes the entire input file and writes the filtered
/// results to a Parquet file on disk. Use this when you want to convert
/// pageviews data to Parquet format for later analysis.
///
/// By default, the batches will equal the default parquet row group size,
/// which causes memory requirements of about 100MB. Lower this to sacrifice
/// performance for lower memory requirements, or vice versa.
///
/// # Example
///
/// ```no_run
/// use pvstream::{parquet_from_file, filter::FilterBuilder};
/// use std::path::PathBuf;
///
/// let filter = FilterBuilder::new()
///     .min_views(100)
///     .languages(["en", "de", "fr"])
///     .build();
///
/// parquet_from_file(
///     PathBuf::from("pageviews-20240818-080000.gz"),
///     PathBuf::from("output.parquet"),
///     &filter,
///     None, // Use default batch size
/// )?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
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

/// Download a remote pageviews file and write filtered results to a Parquet file.
///
/// This function streams the file from a remote URL and writes the filtered
/// results to a Parquet file on disk. Use this when you want to download and
/// convert pageviews data to Parquet format in one step.
///
/// By default, the batches will equal the default parquet row group size,
/// which causes memory requirements of about 100MB. Lower this to sacrifice
/// performance for lower memory requirements, or vice versa.
///
/// # Example
///
/// ```no_run
/// use pvstream::{parquet_from_url, filter::FilterBuilder};
/// use std::path::PathBuf;
/// use url::Url;
///
/// let url = Url::parse("https://dumps.wikimedia.org/other/pageviews/2024/2024-08/pageviews-20240818-080000.gz")?;
/// let filter = FilterBuilder::new()
///     .domain_codes(["en.m"])
///     .min_views(50)
///     .build();
///
/// parquet_from_url(
///     url,
///     PathBuf::from("output.parquet"),
///     &filter,
///     None,
/// )?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
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
