use flate2::read::GzDecoder;
use reqwest::Error as ReqwestError;
use reqwest::blocking;
use std::fs::File;
use std::io::Error as IoError;
use std::io::copy;
use std::io::{BufRead, BufReader, Lines, Read};
use std::path::Path;
use thiserror::Error;
use url::ParseError as UrlParseError;
use url::Url;

type LineReader = Box<dyn Iterator<Item = Result<String, IoError>> + Send>;

#[derive(Debug, Error)]
pub enum StreamError {
    #[error(transparent)]
    Http(#[from] ReqwestError),

    #[error(transparent)]
    Io(#[from] IoError),

    #[error(transparent)]
    Url(#[from] UrlParseError),
}

/// Struct that owns both the buffer and its iterator.
///
/// Makes sure we own the entire I/O stack, not borrowing any locals, to
/// avoid lifetime headaches when reading from files.
struct OwnedLines<R: BufRead> {
    lines: Lines<R>,
}

impl<R: BufRead> OwnedLines<R> {
    fn new(reader: R) -> Self {
        Self {
            lines: reader.lines(),
        }
    }
}

impl<R: BufRead + Send + 'static> Iterator for OwnedLines<R> {
    type Item = Result<String, IoError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.lines.next()
    }
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
pub fn lines_from_file(path: &Path) -> Result<LineReader, StreamError> {
    let file = File::open(path)?;
    Ok(Box::new(decompress_and_stream(file)))
}

/// Creates an iterator to extract lines from a gzipped file server over HTTP
pub fn lines_from_http(url: Url) -> Result<LineReader, StreamError> {
    let response = blocking::get(url)?.error_for_status()?;
    Ok(Box::new(decompress_and_stream(response)))
}

/// Creates an iterator to extract lines from a gzipped file
///
/// Works with files from the local file system or a remote server.
fn decompress_and_stream<R>(source: R) -> impl Iterator<Item = Result<String, IoError>> + Send
where
    R: Read + Send + 'static,
{
    let decoder = GzDecoder::new(source);
    let reader = BufReader::with_capacity(256 * 1024, decoder);
    OwnedLines::new(reader)
}
