use flate2::read::GzDecoder;
use reqwest::blocking::Client;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use url::Url;

/// Creates an iterator to extract lines from a gzipped file on the local fs
pub fn from_file(
    path: &Path,
) -> Result<impl Iterator<Item = std::io::Result<String>>, std::io::Error> {
    let file = File::open(path)?;
    Ok(decompress_and_stream(file))
}

/// Creates an iterator to extract lines from a gzipped file server over HTTP
pub fn from_http(
    url: &Url,
) -> Result<impl Iterator<Item = std::io::Result<String>>, Box<dyn std::error::Error>> {
    let response = Client::new().get(url.as_str()).send()?.error_for_status()?;
    Ok(decompress_and_stream(response))
}

/// Creates an iterator to extract lines from a gzipped file
///
/// Works with files from the local file system or a remote server.
fn decompress_and_stream(source: impl Read) -> impl Iterator<Item = std::io::Result<String>> {
    let decoder = GzDecoder::new(source);
    let reader = BufReader::with_capacity(256 * 1024, decoder);

    reader.lines()
}
