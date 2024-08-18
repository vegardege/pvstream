use crate::filter::Filter;
use crate::parse::{Pageviews, ParseError};
use crate::stream::StreamError;
use crate::{RowIterator, stream_from_file, stream_from_http};
use pyo3::exceptions::{PyIOError, PyIndexError, PyValueError};
use pyo3::prelude::*;
use regex::Regex;
use std::path::PathBuf;
use std::sync::Mutex;
use url::Url;

impl From<StreamError> for PyErr {
    fn from(err: StreamError) -> Self {
        match err {
            StreamError::Http(e) => PyIOError::new_err(e.to_string()),
            StreamError::Url(e) => PyIOError::new_err(e.to_string()),
            StreamError::Io(e) => PyIOError::new_err(e.to_string()),
        }
    }
}

impl From<ParseError> for PyErr {
    fn from(err: ParseError) -> Self {
        match err {
            ParseError::MissingField(_, e) => PyIndexError::new_err(e.to_string()),
            ParseError::InvalidField(_, e) => PyValueError::new_err(e.to_string()),
            ParseError::ReadError(e) => PyIOError::new_err(e.to_string()),
        }
    }
}

/// Represents a single row from a pageviews file
///
/// `domain_code`, `page_title`, and `views` are the three columns from the
/// file itself. `language`, `domain`, and `mobile` are parsed from the
/// domain code.
#[pyclass(name = "Pageviews")]
pub struct PyPageviews {
    #[pyo3(get)]
    pub domain_code: String,
    #[pyo3(get)]
    pub page_title: String,
    #[pyo3(get)]
    pub views: u32,
    #[pyo3(get)]
    pub language: String,
    #[pyo3(get)]
    pub domain: Option<String>,
    #[pyo3(get)]
    pub mobile: bool,
}

impl From<Pageviews> for PyPageviews {
    fn from(inner: Pageviews) -> Self {
        Self {
            domain_code: inner.domain_code,
            page_title: inner.page_title,
            views: inner.views,
            language: inner.parsed_domain_code.language,
            domain: inner.parsed_domain_code.domain.map(str::to_owned),
            mobile: inner.parsed_domain_code.mobile,
        }
    }
}

#[pymethods]
impl PyPageviews {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "Pageviews(\
                domain_code={:?}, \
                page_title={:?}, \
                views={}, \
                language={:?}, \
                domain={:?}, \
                mobile={:?})",
            self.domain_code,
            self.page_title,
            self.views,
            self.language,
            self.domain.as_deref().unwrap_or("None"),
            self.mobile,
        ))
    }
}

#[pyclass(name = "RowIterator")]
struct PyRowIterator {
    iterator: Mutex<RowIterator>,
}

#[pymethods]
impl PyRowIterator {
    #[new]
    fn new(
        path: Option<String>,
        url: Option<String>,
        line_regex: Option<String>,
        domain_codes: Option<Vec<String>>,
        page_title: Option<String>,
        min_views: Option<u32>,
        max_views: Option<u32>,
        languages: Option<Vec<String>>,
        domains: Option<Vec<String>>,
        mobile: Option<bool>,
    ) -> PyResult<Self> {
        let line_regex = line_regex
            .map(|pattern| Regex::new(&pattern))
            .transpose()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let page_title = page_title
            .map(|pattern| Regex::new(&pattern))
            .transpose()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let filter = Filter {
            line_regex,
            domain_codes,
            page_title,
            min_views,
            max_views,
            languages,
            domains,
            mobile,
        };

        let iterator = match (path, url) {
            (Some(path), None) => {
                let path = PathBuf::from(path);
                stream_from_file(path, &filter)?
            }
            (None, Some(url)) => {
                let url = Url::parse(&url).map_err(|e| PyValueError::new_err(e.to_string()))?;
                stream_from_http(url, &filter)?
            }
            _ => return Err(PyValueError::new_err("`path` or `url` must be provided")),
        };

        Ok(Self {
            iterator: Mutex::new(iterator),
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRefMut<'_, Self>) -> PyResult<Option<PyPageviews>> {
        match slf.iterator.lock().unwrap().next() {
            Some(Ok(row)) => Ok(Some(row.into())),
            Some(Err(err)) => Err(err.into()),
            None => Ok(None),
        }
    }
}

/// Stream a pageviews file from disk with optional filters.
///
/// Parameters:
///     path (str): Path to the pageviews file.
///     line_regex (str | None): Optional regex to match lines before parsing.
///     domain_codes (list[str] | None): List of domain codes to match exactly.
///     page_title (str | None): Optional regex to match parsed page title.
///     min_views (int | None): Minimum number of views.
///     max_views (int | None): Maximum number of views.
///     languages (list[str] | None): Filter by language codes.
///     domains (list[str] | None): Filter by Wikimedia domain.
///     mobile (bool | None): Filter mobile or desktop traffic.
///
/// Returns:
///     RowIterator: An iterator over parsed Pageviews.
///
/// Raises:
///     IOError: If the file can't be read.
///     ParseError: If parsing fails.
///
/// Example:
///     >>> stream_from_file("pageviews.gz", languages=["de"], mobile=True)
#[pyfunction]
#[pyo3(
    name="stream_from_file",
    signature = (
        path, line_regex=None, domain_codes=None, page_title=None,
        min_views=None, max_views=None, languages=None, domains=None,
        mobile=None)
)]
fn py_stream_from_file(
    path: String,
    line_regex: Option<String>,
    domain_codes: Option<Vec<String>>,
    page_title: Option<String>,
    min_views: Option<u32>,
    max_views: Option<u32>,
    languages: Option<Vec<String>>,
    domains: Option<Vec<String>>,
    mobile: Option<bool>,
) -> PyResult<PyRowIterator> {
    PyRowIterator::new(
        Some(path),
        None,
        line_regex,
        domain_codes,
        page_title,
        min_views,
        max_views,
        languages,
        domains,
        mobile,
    )
}

/// Stream a pageviews file from a remote server with optional filters.
///
/// Parameters:
///     url (str): URL to the pageviews file.
///     line_regex (str | None): Optional regex to match lines before parsing.
///     domain_codes (list[str] | None): List of domain codes to match exactly.
///     page_title (str | None): Optional regex to match parsed page title.
///     min_views (int | None): Minimum number of views.
///     max_views (int | None): Maximum number of views.
///     languages (list[str] | None): Filter by language codes.
///     domains (list[str] | None): Filter by Wikimedia domain.
///     mobile (bool | None): Filter mobile or desktop traffic.
///
/// Returns:
///     RowIterator: An iterator over parsed Pageviews.
///
/// Raises:
///     IOError: If the file can't be read.
///     ParseError: If parsing fails.
///
/// Example:
///     >>> stream_from_url("http://127.0.0.1/pageviews.gz", domains=["wikibooks.org"])
#[pyfunction]
#[pyo3(
    name="stream_from_url",
    signature = (
        url, line_regex=None, domain_codes=None, page_title=None,
        min_views=None, max_views=None, languages=None, domains=None,
        mobile=None)
)]
fn py_stream_from_url(
    url: String,
    line_regex: Option<String>,
    domain_codes: Option<Vec<String>>,
    page_title: Option<String>,
    min_views: Option<u32>,
    max_views: Option<u32>,
    languages: Option<Vec<String>>,
    domains: Option<Vec<String>>,
    mobile: Option<bool>,
) -> PyResult<PyRowIterator> {
    PyRowIterator::new(
        None,
        Some(url),
        line_regex,
        domain_codes,
        page_title,
        min_views,
        max_views,
        languages,
        domains,
        mobile,
    )
}

#[pymodule]
fn pvstream(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyPageviews>()?;
    m.add_function(wrap_pyfunction!(py_stream_from_file, m)?)?;
    m.add_function(wrap_pyfunction!(py_stream_from_url, m)?)?;
    Ok(())
}
