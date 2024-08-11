use crate::parse::{Pageviews, ParseError};
use crate::stream::StreamError;
use crate::{RowIterator, stream_from_file, stream_from_http};
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyValueError;
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
            ParseError::MissingField(_, e) => PyIOError::new_err(e.to_string()),
            ParseError::InvalidField(_, e) => PyValueError::new_err(e.to_string()),
            ParseError::ReadError(e) => PyIOError::new_err(e.to_string()),
        }
    }
}

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
    fn new(path: Option<&str>, url: Option<&str>, line_regex: Option<&str>) -> PyResult<Self> {
        let line_regex = line_regex
            .map(|pattern| Regex::new(pattern))
            .transpose()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let iterator = match (path, url) {
            (Some(path), None) => {
                let path = PathBuf::from(path);
                stream_from_file(path, line_regex)?
            }
            (None, Some(url)) => {
                let url = Url::parse(url).map_err(|e| PyValueError::new_err(e.to_string()))?;
                stream_from_http(url, line_regex)?
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

#[pyfunction]
#[pyo3(name="stream_from_file", signature = (path, line_regex=None))]
fn py_stream_from_file(path: &str, line_regex: Option<&str>) -> PyResult<PyRowIterator> {
    PyRowIterator::new(Some(path), None, line_regex)
}

#[pyfunction]
#[pyo3(name="stream_from_url", signature = (url, line_regex=None))]
fn py_stream_from_url(url: &str, line_regex: Option<&str>) -> PyResult<PyRowIterator> {
    PyRowIterator::new(None, Some(url), line_regex)
}

#[pymodule]
fn pvstream(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyPageviews>()?;
    m.add_function(wrap_pyfunction!(py_stream_from_file, m)?)?;
    m.add_function(wrap_pyfunction!(py_stream_from_url, m)?)?;
    Ok(())
}
