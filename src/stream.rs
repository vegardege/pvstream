use flate2::read::GzDecoder;
use reqwest::Error as ReqwestError;
use reqwest::blocking;
use std::fs::File;
use std::io::Error as IoError;
use std::io::copy;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use thiserror::Error;
use url::ParseError as UrlParseError;
use url::Url;

pub type LineStream = Box<dyn Iterator<Item = std::io::Result<String>> + Send>;

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("HTTP error: {0}")]
    Http(#[from] ReqwestError),

    #[error("I/O error: {0}")]
    Io(#[from] IoError),

    #[error("URL error: {0}")]
    Url(#[from] UrlParseError),
}

/// Download a file and store it on the local file system.
///
/// Use this in combination with `from_file` if you plan to parse data from
/// the same file more than once. If you only ever plan to use the file once,
/// skip the disk IO and use `from_http` directly for a ~50% speedup.
///
/// Download is capped at 1GB (1 << 30 bytes), which should never be an issue
/// with pageviews files. This is just a mandatory safety measure.
///
/// This function will create a file if it does not exist, and will truncate
/// it if it does.
pub fn http_to_file(url: &Url, path: &Path) -> Result<(), StreamError> {
    let response = blocking::get(url.as_str())?.error_for_status()?;
    let mut dest = File::create(path)?;
    copy(&mut response.take(1 << 30), &mut dest)?;
    Ok(())
}

/// Creates an iterator to extract lines from a gzipped file on the local fs
pub fn from_file(path: &Path) -> Result<LineStream, StreamError> {
    let file = File::open(path)?;
    Ok(Box::new(decompress_and_stream(file)))
}

/// Creates an iterator to extract lines from a gzipped file server over HTTP
pub fn from_http(url: &Url) -> Result<LineStream, StreamError> {
    let response = blocking::get(url.as_str())?.error_for_status()?;
    Ok(Box::new(decompress_and_stream(response)))
}

/// Creates an iterator to extract lines from a gzipped file
///
/// Works with files from the local file system or a remote server.
fn decompress_and_stream(source: impl Read) -> impl Iterator<Item = std::io::Result<String>> {
    let decoder = GzDecoder::new(source);
    let reader = BufReader::with_capacity(256 * 1024, decoder);

    reader.lines()
}
